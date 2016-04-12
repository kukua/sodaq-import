# SODAQ MySQL sensor data import

> Import sensor data from SODAQ into MySQL database.

## Setup

```bash
git clone https://github.com/kukua/sodaq-import.git
cd sodaq-import
cp .env.sample .env
chmod 600 .env
# > Edit .env

sudo cp ./cronjob /etc/cron.d/sodaq-import
sudo service cron reload
```
