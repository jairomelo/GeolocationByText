from SPARQLWrapper import SPARQLWrapper, JSON
import configparser
from rapidfuzz import fuzz
import os
import logging
import requests
import ast

config = configparser.ConfigParser()
config.read("conf/global.conf")

os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.INFO, filename="logs/getCoordinates.log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

class TGNQuery:
    """
    A class to interact with the Getty Thesaurus of Geographic Names (TGN) SPARQL endpoint.
    
    This class provides methods to search and retrieve geographic coordinates for places
    using the Getty TGN linked open data service. It supports fuzzy matching of place names
    and filtering by country and place type.

    Attributes:
        sparql (SPARQLWrapper): SPARQL endpoint wrapper instance for TGN queries
        lang (str): Language code for the place type (default: "es")

    Example:
        >>> tgn = TGNQuery("http://vocab.getty.edu/sparql")
        >>> results = tgn.Places_by_Triple_FTS("Madrid", "Spain", "ciudad")
        >>> coordinates = tgn.get_best_match(results, "Madrid")
    """
    def __init__(self, endpoint: str, lang: str = "es"):
        self.sparql = SPARQLWrapper(endpoint)
        self.sparql.setReturnFormat(JSON)
        self.lang = lang

    def Places_by_Triple_FTS(self, place_name: str, country_code: str, place_type: str = None) -> dict:
        type_filter = f'?p gvp:placeType [rdfs:label "{place_type}"@{self.lang}].' if place_type else ''
        
        query = f"""
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX luc: <http://www.ontotext.com/owlim/lucene#>
            PREFIX gvp: <http://vocab.getty.edu/ontology#>
            PREFIX xl: <http://www.w3.org/2008/05/skos-xl#>
            PREFIX tgn: <http://vocab.getty.edu/tgn/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT * {{
                ?p skos:inScheme tgn:; luc:term "{place_name}"; gvp:prefLabelGVP [xl:literalForm ?pLab].
                ?pp1 skos:inScheme tgn:; luc:term "{country_code}"; gvp:prefLabelGVP [xl:literalForm ?pp1Lab].
                ?p gvp:broaderPartitiveExtended ?pp1.
                {type_filter}
            }}
        """
        
        try:
            self.sparql.setQuery(query)
            results = self.sparql.query().convert()
            return results["results"]["bindings"]
        except Exception as e:
            logger.error(f"Error querying TGN for '{place_name}': {str(e)}")
            return []
        

    def get_coordinates_lod_json(self, tgn_uri: str) -> tuple:
        json_url = tgn_uri + ".json"
        try:
            response = requests.get(json_url)
            if response.status_code == 200:
                data = response.json()

                for item in data.get("identified_by"):
                    if item.get("type") == "crm:E47_Spatial_Coordinates":
                        value = item.get("value")
                        coords = ast.literal_eval(value)
                        if isinstance(coords, list) and len(coords) == 2:
                            lon, lat = coords
                            return (lat, lon)

            return (None, None)
        except Exception as e:
            logger.error(f"Error fetching coordinates via JSON for {tgn_uri}: {e}")
            return (None, None)

    def get_best_match(self, results: dict, place_name: str, fuzzy_threshold: float = 90) -> tuple:
        if not results:
            return (None, None)
        
        if len(results) == 1:
            return self.get_coordinates_lod_json(results[0].get("p", {}).get("value", ""))

        for r in results:
            label = r.get("pLab", {}).get("value", "")
            uri = r.get("p", {}).get("value", "")
            ratio = fuzz.ratio(label.lower(), place_name.lower())
            if ratio >= fuzzy_threshold:
                logger.info(f"Best match for '{place_name}': {label} ({ratio}%)")
                return self.get_coordinates_lod_json(uri)
        
        return (None, None)

class HGISQuery:
    """
    A class to interact with the World Historical Gazetteer (WHG) API.

    This class provides methods to search and retrieve geographic coordinates for historical
    places using the WHG API. It supports filtering by country code and feature class,
    and includes functionality to find the best matching place from multiple results.

    Attributes:
        collection (str): The WHG collection to search in (default: 'lugares13k_rel')
        endpoint (str): The base URL for the WHG API
        search_domain (str): The API endpoint path for searches. Default is "/index"

    Example:
        >>> hgis = HGISQuery("https://whgazetteer.org/api")
        >>> results = hgis.places_by_name("Cuicatlán", ccode="MX", fclass="p")
        >>> coordinates = hgis.get_best_match(results, placetype="pueblo", ccode="MX")
    """
    def __init__(self, endpoint: str, search_domain: str = "/index"):
        if not endpoint or not isinstance(endpoint, str):
            raise ValueError("Endpoint must be a non-empty string")
        self.collection = "lugares13k_rel"
        self.endpoint = endpoint.rstrip("/")
        self.search_domain = search_domain
        
    def places_by_name(self, place_name: str, ccode: str = None, fclass: str = 'p') -> dict:
        """
        Search for place using the World Historical Gazetteer API https://docs.whgazetteer.org/content/400-Technical.html#api
        
        Parameters:
            place_name (str): Any string with the name of the place. This keyword includes place names variants.
            ccode (str): ISO 3166-1 alpha-2 country code.
            fclass (str): Feature class according to Linked Places Format. Default is 'p' for place. Look at https://github.com/LinkedPasts/linked-places-format for more places classes.         
        """
        
        if not place_name or not isinstance(place_name, str):
            raise ValueError("place_name must be a non-empty string")
        if ccode and (not isinstance(ccode, str) or len(ccode) != 2):
            raise ValueError("ccode must be a valid 2-letter country code")
        
        
        url = f"{self.endpoint}{self.search_domain}/?name={place_name}&dataset={self.collection}&ccodes={ccode}&fclass={fclass}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error searching for '{place_name}': {str(e)}")
            return {"features": []}
        except ValueError as e:
            logger.error(f"Invalid JSON response for '{place_name}': {str(e)}")
            return {"features": []}
            
        
    def get_best_match(self, results: dict, placetype: str = None, ccode: str = None) -> tuple:
        
        try:
            if len(results["features"]) == 0:
                return (None, None)
            
            if len(results["features"]) == 1:
                coordinates = results["features"][0].get("geometry").get("coordinates")
                return coordinates[1], coordinates[0]

            for r in results["features"]:
                placetypes = r.get("properties", {}).get("placetypes", [])
                ccodes = r.get("properties", {}).get("ccodes", [])
                if placetype and ccode:
                    if placetype.capitalize() in placetypes and ccode in ccodes:
                        coordinates = r["geometry"]["coordinates"]
                        return coordinates[1], coordinates[0]

            return (None, None)
        
        except Exception as e:
            logger.error(f"Error processing results: {str(e)}")
            return (None, None)
    
if __name__ == "__main__":
    hgis_query = HGISQuery(config["apis"]["hgis_endpoint"])
    results = hgis_query.places_by_name(place_name="cuicatlán", fclass="p", ccode="MX")
    best_match = hgis_query.get_best_match(results, placetype="pueblo", ccode="MX")
    print(best_match)