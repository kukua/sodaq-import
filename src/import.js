import _ from 'underscore'
import mysql from 'mysql'
import parallel from 'node-parallel'

// Helper functions
var env = (key, fallback = '') => (typeof process.env[key] !== 'undefined' ? process.env[key] : fallback)
var log = (msg, ...args) => { console.log('[' + new Date() + '] ' + msg, ...args) }

// Configuration
var enabledColumn = env('IMPORT_ENABLED_COLUMN')
var timestampColumn = env('IMPORT_TIMESTAMP_COLUMN')

var endTimestamp = (process.argv[2] || Math.round(Date.now() / 1000))
//var range = endTimestamp // all
var range = (process.argv[3] || 4 * 60 * 60) // seconds
var startTimestamp = endTimestamp - range
//var timeout = 10 * 60 * 1000 // milliseconds
var timeout = 60 * 1000 // milliseconds

// Data processing
var tableNameToDeviceId = (table) => {
	var matches = table.match(/\_([a-f0-9]{8})$/i)
	if ( ! matches[1]) return
	return '4a000000' + matches[1].toLowerCase()
}

var getColumns = (results) => _.map(Object.keys(results[0]), (key) => {
	// MySQL columns are case insensitive
	key = key.replace(/[^a-zA-Z0-9\_]+/g, '').toLowerCase()
	if (key === 'ts') return 'timestamp'
	if (key === 'hum') return 'humid'
	if (key === 'humSHT21') return 'humid'
	if (key === 'gaspres') return 'gasPress'
	if (key === 'rainticks') return 'rain'
	if (key === 'rainticks1') return 'rain'
	if (key === 'windticks') return 'windSpeed'
	if (key === 'windgustticks') return 'gustSpeed'
	if (key === 'windgustdir') return 'gustDir'
	if (key === 'tempbmp085') return 'temp'
	if (key === 'presbmp085') return 'pressure'
	if (key === 'presbmp') return 'pressure'
	return key
})
var getRows = (results) => _.map(results, (result) => {
	if (typeof result.RainTicks !== 'undefined') {
		result.RainTicks = Math.round(10 * result.RainTicks * 0.2) / 10
	}
	if (typeof result.RainTicks2 !== 'undefined') {
		result.RainTicks2 = Math.round(10 * result.RainTicks2 * 0.2) / 10
	}
	if (typeof result.WindGustTicks !== 'undefined') {
		if (result.WindGustTicks < 0.04 * result.WindTicks) {
			result.WindGustTicks = Math.round(10 * result.WindGustTicks * 3.6 / 3) / 10
		} else {
			result.WindGustTicks = 0
		}
	}
	if (typeof result.WindTicks !== 'undefined') {
		result.WindTicks = Math.round(10 * result.WindTicks * 3.6 / 300) / 10
	}
	if (typeof result.WindDir !== 'undefined') {
		if (result.WindDir <= 900) {
			result.WindDir = result.WindDir % 360
		} else {
			result.WindDir = 0
		}
	}
	if (typeof result.WindGustDir !== 'undefined') {
		result.WindGustDir = result.WindGustDir % 360
	}
	if (typeof result.WindLullDir !== 'undefined') {
		if (result.WindLullDir <= 900) {
			result.WindLullDir = result.WindLullDir % 360
		} else {
			result.WindLullDir = 0
		}
	}
	if (typeof result.MaxSolar !== 'undefined') {
		result.MaxSolar = Math.round(result.MaxSolar * 2.5)
	}
	if (typeof result.MaxSolar1 !== 'undefined') {
		result.MaxSolar1 = Math.round(result.MaxSolar1 * 2.5)
	}
	if (typeof result.Temp !== 'undefined') {
		result.Temp /= 10
	}
	if (typeof result.TempBMP085 !== 'undefined') {
		result.TempBMP085 /= 10
	}
	if (typeof result.TempSHT21 !== 'undefined') {
		result.TempSHT21 /= 10
	}
	if (typeof result.Hum !== 'undefined') {
		result.Hum /= 10
	}
	if (typeof result.HumSHT21 !== 'undefined') {
		result.HumSHT21 /= 10
	}
	if (typeof result.PresBMP !== 'undefined') {
		result.PresBMP /= 10
	}
	if (typeof result.PresBMP085 !== 'undefined') {
		result.PresBMP085 /= 10
	}

	// Manually build query (instead of using multidimensional array)
	// to use FROM_UNIXTIME for the timestamps.
	return '(' + _.map(result, (val, key) => {
		if (key === 'ts') return 'FROM_UNIXTIME(' + storage.escape(val) + ')'
		return storage.escape(val)
	}).join(',') + ')'
}).join(',')
var insert = (id, results, cb) => {
	storage.query({
		sql: `REPLACE INTO ?? (??) VALUES ` + getRows(results),
		timeout
	}, [id, getColumns(results)], (err) => {
		cb(err)
	})

	/*
	request.put({
		url: env('CONCAVA_HOST') + '/v1/sensorData/' + id,
		headers: {
			'Content-Type': 'application/octet-stream',
			'Authorization': 'Token ' + env('CONCAVA_AUTH_TOKEN')
		},
		body: buffer
	}, (err, res) => {
		console.log(err, res)
		cb(err)
	})
	*/
}

// Connect to SODAQ database
var source = mysql.createConnection({
	host: env('IMPORT_MYSQL_HOST'),
	user: env('IMPORT_MYSQL_USER'),
	password: env('IMPORT_MYSQL_PASSWORD'),
	database: env('IMPORT_MYSQL_DATABASE')
})

// Connect to storage database
var storage = mysql.createConnection({
	host: env('MYSQL_HOST'),
	user: env('MYSQL_USER'),
	password: env('MYSQL_PASSWORD'),
	database: env('MYSQL_DATABASE')
})

// Fetch data
log('Starting import..')

source.query('SELECT id, tablename, ?? FROM devices WHERE ?? = 1', [timestampColumn, enabledColumn], (err, results) => {
	if (err) {
		log('Error retrieving devices:', err)
		process.exit(1)
	}

	var p = parallel().timeout(timeout)

	_.each(results, (result) => {
		p.add((done) => {
			var table = result.tablename
			var id = tableNameToDeviceId(table)

			log('[%s] Fetching rows (%s, TS %d-%d).', table, id, startTimestamp, endTimestamp)

			source.query({
				sql: 'SELECT * FROM ?? WHERE ts > ? AND ts <= ? ORDER BY ts ASC',
				timeout
			}, [table, startTimestamp, endTimestamp], (err, results) => {
				if (err) {
					log('[%s] Error fetching rows: %s', table, err)
					return done()
				}

				log('[%s] Processing %d rows.', table, results.length)

				if (results.length === 0) return done()

				insert(id, results, (err) => {
					if (err) {
						log('[%s] Insert error: %s', table, err)
						return done()
					}

					done()
				})
			})
		})
	})

	p.done((err) => {
		// Disconnect from MySQL clients
		source.destroy()
		storage.destroy()

		if (err) {
			log('Processing error:', err)
			process.exit(1)
		}

		log('Done.')
	})
})
