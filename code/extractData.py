import mysql.connector as mysql
import pandas as pd
import configparser
import os
import logging

os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.INFO, filename="logs/extractData.log", filemode="a", format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read("conf/global.conf")


def check_mysql_data_exists(query: str) -> bool:
    """
    Check if data exists in MySQL database using a query.
    """
    try:
        conn = mysql.connect(host=config["mysql"]["host"],
                             user=config["mysql"]["user"],
                             password=config["mysql"]["password"])
        cursor = conn.cursor()
        cursor.execute(query)
        if not cursor.fetchone():
            logger.info(f"No data found for query: {query}")
            return False
        else:
            logger.info(f"Data found for query: {query}")
            return True
    except Exception as e:
        logger.error(f"Error checking MySQL data: {e}")
        raise e

def get_data_from_mysql(query: str) -> pd.DataFrame:
    """
    Get data from MySQL database using a query.
    """
    if not check_mysql_data_exists(query):
        logger.info(f"No data found for query: {query}")
        return pd.DataFrame()
    try:
        conn = mysql.connect(host=config["mysql"]["host"],
                             user=config["mysql"]["user"],
                             password=config["mysql"]["password"])
        cursor = conn.cursor()
        cursor.execute(query)
        logger.info(f"Query executed: {query}")
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        logger.info(f"Data fetched: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error getting data from MySQL: {e}")
        raise e

def query_lugares():
    query = "SELECT * FROM mdb.dbgestor_lugar"
    try:
        df = get_data_from_mysql(query)
        df.to_csv("data/interim/lugares.csv", index=False)
        logger.info(f"Data saved to data/interim/lugares.csv")
    except Exception as e:
        logger.error(f"Error querying lugares: {e}")
        raise e

if __name__ == "__main__":
    query_lugares()