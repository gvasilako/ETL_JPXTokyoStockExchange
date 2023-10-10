import pytest
import json
from sqlalchemy import create_engine
import pandas as pd


# load configuration data
with open('configs.json', 'r') as f:
    db_configs = json.load(f)['DB']

# Connect to the database
user = db_configs['user']
password = db_configs['password']
server = db_configs['server']
dbname = db_configs['database']

# production case
#connection_url = f"mysql+pymysql://{user}:{password}@{server}/{dbname}"

# test purposes
connection_url = f"sqlite:///{dbname}.db"

engine = create_engine(connection_url)

@pytest.fixture
def stock_prices():
    return pd.read_sql_table("StockPrices", con=engine.connect()).drop(columns=['id'])

@pytest.fixture
def stock_metadata():
    return pd.read_sql_table("StockMetadata", con=engine.connect())