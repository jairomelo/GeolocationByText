# 🗺️ Geocoding Historical Locations for Trayectorias Afro

This tool automates the process of geocoding historical place names by matching textual descriptions with standardized geographical coordinates using multiple authoritative sources. It was developed to support the [Trayectorias Afro](https://trayectoriasafro.org/About) project, which traces the historical trajectories of African and Afro-descendant individuals in colonial Latin America.

## Overview

This repository contains the pipeline for identifying the geographical coordinates of place names collected by the Trayectorias Afro project. The system queries four different gazetteers via their SPARQL or REST APIs:

- [Getty TGN](https://www.getty.edu/research/tools/vocabularies/tgn/index.html)
- [HGIS de las Indias](https://hgisdelasindias.org/) via the [World Historical Gazetteer](https://whgazetteer.org/)
- [GeoNames](https://www.geonames.org/export/web-services.html) database
- [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) query service.

Each place name is compared across sources using fuzzy matching, country codes, and place types to determine the most accurate result.

## Workflow

1. **Database Ingestion**: Run `dbpopulate.py` to populate a MySQL database with the latest backup from `data/raw/`. The original backup path must be provided in the `conf/global.conf` file.

2. **Export to CSV**: Use `extractData.py` to export the `dbgestor_lugar` table to CSV format.

3. **Data Cleaning**: Run `preprocessing.py` to clean names, remove duplicates, and normalize values.

4. **Geolocation**: Run `geolocate.py` to match place names to coordinates using the services listed above.

5. **Output**: The final CSV includes columns for `lat`, `lon`, and metadata such as the matched name and data source (optional).

## Requirements
- Linux or Linux-like environment (e.g. WSL, macOS)
- Python >= 3.10
- MySQL database version 8.x
- A registered [Geonames](https://www.geonames.org/export/web-services.html)  account (username required for API access).

## Setup
1. Clone the repository

```bash
git clone https://github.com/yourusername/trayectorias-geocoder.git
cd trayectorias-geocoder
```

2. Configure environment

- Copy the example config:

```bash
cp conf/global.conf-safe conf/global.conf
```

- Edit conf/global.conf and fill in:
    - MySQL credentials
    - GeoNames username (also optionally set .env for environment variables)

3. Install dependencies

Using pip:

```bash
pip install -r requirements.txt
```

### Project Structure

```vbnet
.
├── conf/
│   ├── global.conf
│   └── global.conf-safe
├── data/
│   ├── raw/
│   ├── interim/
│   └── processed/
├── scripts/
│   ├── dbpopulate.py
│   ├── extractData.py
│   ├── preprocessing.py
│   ├── geolocate.py
│   └── getCoordinates.py
├── notebooks/
│   └── (optional analysis/QA notebooks)
├── README.md
└── requirements.txt
```

## Reproducibility & Logging

- Logs are automatically saved in the logs/ directory during geolocation.
- Each script prints status messages and summaries to help track pipeline progress.
- Failed or unresolved place names can be logged for manual review.

## License
CC0 1.0 Universal