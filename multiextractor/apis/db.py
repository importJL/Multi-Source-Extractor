from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import pymongo
import os

load_dotenv()

NEON_DB_KEY = os.getenv('NEON_CONN_KEY')
NEON_DB_USER = os.getenv('NEON_USER')
NEON_COMPUTE_NAME = os.getenv('NEON_COMPUTE')
NEON_LOCATION = os.getenv('NEON_LOCATION')
NEON_DB_NAME = os.getenv('NEON_DB')
NEON_PORT = os.getenv('NEON_PORT')

MONGO_DB_USER = os.getenv('ATLAS_USER')
MONGO_DB_KEY = os.getenv('ATLAS_KEY')
MONGO_DB_NAME = os.getenv('ATLAS_DB')

NEON_CONN_STR = f'postgresql+psycopg2://{NEON_DB_USER}:{NEON_DB_KEY}@{NEON_COMPUTE_NAME}.{NEON_LOCATION}.aws.neon.tech/{NEON_DB_NAME}'
MONGO_CONN_STR = f'mongodb+srv://{MONGO_DB_USER}:{MONGO_DB_KEY}@{MONGO_DB_NAME}.l0wmrp9.mongodb.net/?retryWrites=true&w=majority'
# MONGO_CONN_STR = f'mongodb://{MONGO_DB_USER}:{MONGO_DB_KEY}@{MONGO_DB_NAME}.l0wmrp9.mongodb.net/?retryWrites=true&w=majority'


def neondb_connection() -> psycopg2.extensions.connection:
    '''Initiates backend PostgreSQL database connection via NEON DB host'''
    engine = create_engine(NEON_CONN_STR)
    conn = psycopg2.connect(host=f'{NEON_COMPUTE_NAME}.{NEON_LOCATION}.aws.neon.tech', 
                            dbname=NEON_DB_NAME, 
                            user=NEON_DB_USER, 
                            password=NEON_DB_KEY, 
                            port=NEON_PORT)
    return conn

def mongodb_connection() -> pymongo.mongo_client.MongoClient:
   return pymongo.MongoClient(MONGO_CONN_STR)


def mongodb_get_db(client: pymongo.mongo_client.MongoClient) -> pymongo.database.Database:
    return client[MONGO_DB_NAME]