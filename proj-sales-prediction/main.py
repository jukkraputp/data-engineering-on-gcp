import configparser
import pandas as pd
import sqlalchemy as db
from sqlalchemy import text

CONFIG_FILE = "local.conf"
# CONFIG_FILE = "production.conf"

parser = configparser.ConfigParser()
parser.read(CONFIG_FILE)

database = parser.get("mysql_config", "database")
user = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password")
host = parser.get("mysql_config", "host")
port = parser.get("mysql_config", "port")

uri = f"mysql+pymysql://{user}:{password}@{host}/{database}"
engine = db.create_engine(uri)

with engine.connect() as connection:
    df = pd.read_sql("""
    SELECT * FROM products
    """, connection)

    print(df.columns)
    