import subprocess
import os
import configparser
import mysql.connector as mysql
from utils.logController import setup_logger

logger = setup_logger("dbpopulate")

config = configparser.ConfigParser()
config.read("conf/global.conf")

os.makedirs("data/raw", exist_ok=True)

def get_backup_sql(backup_dir: str) -> str:
    """
    Get the SQL script for restoring a database from a backup file.
    """
    if not os.path.exists(backup_dir):
        raise FileNotFoundError(f"Backup directory {backup_dir} does not exist")
    
    last_backup = sorted(os.listdir(backup_dir))[-1]
    
    if not last_backup.endswith(".sql.gz"):
        raise ValueError(f"Last backup {last_backup} is not a .sql.gz file")
    
    logger.info(f"Unpacking {last_backup}")
    sql_file = f"data/raw/{last_backup.replace('.sql.gz', '.sql')}"
    
    if not os.path.exists(sql_file):
        with open(sql_file, "w") as f:
            result = subprocess.run(["gunzip", "-c", f"{backup_dir}/{last_backup}"], 
                                  capture_output=True, text=True, check=True)
            f.write(result.stdout)
    else:
        logger.info(f"Skipping {last_backup} because it already exists")
        
    return sql_file
    
def populate_mysql_db(sql_file: str):
    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"SQL file {sql_file} does not exist")
    
    # Check if already imported
    with open("logs/dbpopulate.log", "r") as f:
        for line in f:
            if f"SQL file: {sql_file}" in line:
                logger.info(f"SQL file {sql_file} already imported into MySQL database")
                return
    
    try:
        conn = mysql.connect(host=config["mysql"]["host"],
                             user=config["mysql"]["user"], 
                             password=config["mysql"]["password"])
        cursor = conn.cursor()
        try:
            database_name = config["mysql"]["database"]
            cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
            cursor.execute(f"CREATE DATABASE `{database_name}`")
            cursor.execute(f"USE `{database_name}`")
            
            logger.info(f"Importing {sql_file} into {database_name}...")
            cursor.execute(open(sql_file).read())
            conn.commit()

            logger.info(f"Successfully populated MySQL database")
            logger.info(f"SQL file: {sql_file}")
        except Exception as e:
            logger.error(f"Error populating MySQL database: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        logger.error(f"Error populating MySQL database: {e}")
        raise e
    
def main():
    sql_file = get_backup_sql(config["default"]["backup_dir"])
    populate_mysql_db(sql_file)


if __name__ == "__main__":
    main()