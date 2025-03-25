This repository contains code that processes data from [Trayectorias Afro](https://trayectoriasafro.org/About) project. The purpose is to populate the geolocation data (lat, long) from a matching text, using as reference the TGN (The Geographical Names Database) from the Getty Institute.

## Code

All scripts are intended to run in Linux or Linux-like environments (e.g. WSL).

### dbpopulate.py

Populates a MySQL database with the data from the latest backup file in the `data/raw` directory.

Database credentials must be provided in the `conf/global.conf` file. Rename `conf/global.conf-safe` to `conf/global.conf` and fill in the credentials before running the script.

