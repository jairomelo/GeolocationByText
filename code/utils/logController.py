import logging
import configparser 
import os

config = configparser.ConfigParser()
config.read("conf/global.conf")

log_dir = config["default"]["log_dir"]
log_level = config["default"]["log_level"]

os.makedirs(log_dir, exist_ok=True)

def get_logger(name: str):
    logger = logging.getLogger(name)
    return logger

def setup_logger(name: str, level: str = log_level):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    file_handler = logging.FileHandler(os.path.join(log_dir, f"{name}.log"))
    file_handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)

    return logger
