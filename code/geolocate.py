from getCoordinates import PlaceResolver, TGNQuery, HGISQuery, GeonamesQuery, WikidataQuery
import pandas as pd
import os
import logging

os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.INFO, filename="logs/geolocate.log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

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

def main():
    lugares_df = pd.read_csv("data/processed/lugares_geolocated.csv")
    logger.info(f"Total places: {len(lugares_df)}")

    # Geolocate missing coordinates
    lugares_df = geolocate_lugares(lugares_df, "")

    # Convert string coordinates to lat/lon columns
    try:
        lugares_df = set_lat_lon(lugares_df)
        logger.info(f"Successfully extracted lat/lon for {len(lugares_df)} places")
    except Exception as e:
        logger.error(f"Error setting lat/lon: {e}")

    lugares_df.to_csv("data/processed/lugares_geolocated.csv", index=False)
    

if __name__ == "__main__":
    main()