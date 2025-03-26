from getCoordinates import PlaceResolver, TGNQuery, HGISQuery, GeonamesQuery, WikidataQuery
import pandas as pd
import os
from datetime import datetime
from utils.logController import setup_logger

logger = setup_logger("geolocate")

def geolocate_lugar(lugar_name: str, country_code: str, place_type: str) -> tuple:
    
    logger.info(f"Geolocating {lugar_name} in {country_code} with type {place_type}")
    
    services = [
        WikidataQuery("https://query.wikidata.org/sparql")
    ]
    
    logger.info("Adding Geonames service")
    try:
        services.append(GeonamesQuery("http://api.geonames.org"))
    except ValueError:
        logger.info("No Geonames username available")
        pass  
    
    logger.info("Adding TGN service")
    try:
        services.append(TGNQuery("http://vocab.getty.edu/sparql"))
    except ValueError:
        logger.info("No TGN service available")
        pass
    
    logger.info("Adding HGIS service")
    try:
        services.append(HGISQuery("https://whgazetteer.org/api"))
    except ValueError:
        logger.info("No HGIS service available")
        pass
    
    logger.info("Creating PlaceResolver")
    resolver = PlaceResolver(services)
    
    logger.info(f"Resolving {lugar_name} in {country_code} with type {place_type}")
    return resolver.resolve(lugar_name, country_code, place_type)

def geolocate_lugares(lugares_df: pd.DataFrame, ccode: str) -> pd.DataFrame:
    
    lugares_df = lugares_df.copy()
    
    
    if not lugares_df.columns.str.contains("coordenadas").any():
        lugares_df["coordenadas"] = None
        logger.info("Created 'coordenadas' column")
    
    
    mask = lugares_df["coordenadas"].isna() | (lugares_df["coordenadas"] == "(None, None)")
    
    if mask.any():
        logger.info(f"Found {mask.sum()} places without coordinates. Starting geolocation...")
    
        lugares_df.loc[mask, "coordenadas"] = lugares_df[mask].apply(
            lambda row: geolocate_lugar(row["nombre_lugar"], row.get("country_code", ccode), row["tipo"]), 
            axis=1
        )
    else:
        logger.info("All places already have coordinates. Skipping geolocation.")
    
    return lugares_df


def set_lat_lon(lugares_df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("Setting lat/lon")
    
    lugares_df["lat"] = lugares_df["coordenadas"].apply(lambda x: x.split(", ")[0].lstrip("("))
    lugares_df["lon"] = lugares_df["coordenadas"].apply(lambda x: x.split(", ")[1].rstrip(")"))
    
    logger.info(f"Successfully extracted lat/lon for {len(lugares_df)} places")
    
    return lugares_df

def update_sql_records(df: pd.DataFrame):
    """
    Custom function to update SQL records with the new lat/lon
    values. This code is tailored to the specific needs
    of Trayectorias Afro database.
    """
    
    df = df[df["coordenadas"] != "(None, None)"]
    df = df[["lugar_id", "lat", "lon"]]
    
    logger.info(f"Updating {len(df)} SQL records")
    
    sql_updates = []
    
    for index, row in df.iterrows():
        sql_updates.append(f"UPDATE `dbgestor_lugar` SET `lat` = {row['lat']}, `lon` = {row['lon']} WHERE `lugar_id` = {int(row['lugar_id'])};")
    
    mode = 'a' if os.path.exists('data/sql/update_foreign_keys.sql') else 'w'
    
    logger.info(f"Writing {len(sql_updates)} SQL updates to data/sql/update_foreign_keys.sql")
    
    with open('data/sql/update_foreign_keys.sql', mode) as f:
        f.write("START TRANSACTION;\n\n")
        
        f.write(f"-- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("-- Add lat/lon to dbgestor_lugar records\n")
        f.write("\n".join(sql_updates))  
        f.write("\n")
        
        f.write("COMMIT;\n")
    

def main(df: pd.DataFrame, destination: str, geolocate: bool = True, lat_lon: bool = True, update_sql: bool = True, dry_run: bool = True):
    logger.info(f"Total places: {len(df)}")

    if geolocate:
        # Geolocate missing coordinates
        df = geolocate_lugares(df, "")
        logger.info(f"Successfully geolocated {len(df)} places")

    # Convert string coordinates to lat/lon columns
    if lat_lon:
        try:
            df = set_lat_lon(df)
            logger.info(f"Successfully extracted lat/lon for {len(df)} places")
        except Exception as e:
            logger.error(f"Error setting lat/lon: {e}")

    if not dry_run:
        if update_sql:
            update_sql_records(df)
            logger.info("Successfully updated SQL records")
        
        df.to_csv(destination, index=False)
        logger.info(f"Successfully processed {len(df)} places")
    else:
        logger.info(f"Dry run completed. Would have processed {len(df)} places")

if __name__ == "__main__":
    df = pd.read_csv("data/processed/lugares_geolocated.csv")
    destination_file = "data/processed/lugares_geolocated_lat_lon.csv"
    main(df, destination_file, geolocate=False, lat_lon=False, update_sql=True, dry_run=False)