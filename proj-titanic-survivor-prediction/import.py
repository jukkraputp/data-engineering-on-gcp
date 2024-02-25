import configparser

import pandas as pd
import sqlalchemy
from sqlalchemy import text


CONFIG_FILE = "local.conf"

parser = configparser.ConfigParser()
parser.read(CONFIG_FILE)

database = parser.get("mysql_config", "database")
user = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password")
host = parser.get("mysql_config", "host")
port = parser.get("mysql_config", "port")

uri = f"mysql+pymysql://{user}:{password}@{host}/{database}"
engine = sqlalchemy.create_engine(uri)

# Test SQL command
# df = pd.read_sql("show tables", engine)
# print(df.head(3))

# PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
sql = """
    CREATE TABLE IF NOT EXISTS titanic (
        passenger_id    INT,
        survived        INT,
        pclass          INT,
        name            VARCHAR(300)
    ) 
"""

with engine.connect() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS titanic"))
    conn.execute(text(sql))
    print(f"Created table successfully")

    # Import
    df = pd.read_csv(f"data/titanic-original.csv")
    df.to_sql('titanic', con=uri, if_exists="replace", index=False)
    print(f"Imported titanic data successfully")

    # Export
    # df = pd.read_sql(f"select * from {k}", engine)
    # df.to_csv(f"breakfast_{k}_export.csv", index=False)
