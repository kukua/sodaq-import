# SODAQ MySQL sensor data import

> Import sensor data from SODAQ into MySQL database.

## Setup

```bash
git clone https://github.com/kukua/sodaq-import.git
cd sodaq-import
cp .env.example .env
chmod 600 .env
# > Edit .env

sudo cp ./cronjob /etc/cron.d/sodaq-import
sudo service cron reload

# Re-import data for last 10 days
docker-compose run --rm import npm start '' 864000
```

## License

This software is licensed under the [MIT license](https://github.com/kukua/sodaq-import/blob/master/LICENSE).

© 2016 Kukua BV
