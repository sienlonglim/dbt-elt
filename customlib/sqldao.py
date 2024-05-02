import duckdb
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

class DatabaseAccessObject():
    def __init__(self, db: str):
        self.db = db.upper()
        if db == 'MARIADB':
            self.con = create_engine(f"mysql+mariadbconnector://{os.environ['MARIADB_USER']}:{os.environ['MARIADB_PASSWORD']}@{os.environ['MARIADB_HOST']}:3306/{os.environ['MARIADB_DATABASE']}")
        elif db == 'DUCKDB':
            self.con = duckdb.connect(
                os.environ['DUCKDB_DIR'], 
                config = {'threads': 4})

    def query_db(self, query):
        if isinstance(self.con, sqlalchemy.engine.base.Engine):
            df = pd.read_sql(query, con=self.con)

        elif isinstance(self.con, duckdb):
            df = self.con.sql(query)
        return df