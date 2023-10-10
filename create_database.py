from sqlalchemy import create_engine
from models import Base
import json


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

# testing purposes
connection_url = f"sqlite:///{dbname}.db"

engine = create_engine(connection_url, echo=True)

# Create DB schema
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(engine)
