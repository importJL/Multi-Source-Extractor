from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import pymongo
import redis
import os
from multiextractor.constants import DBConstLoader

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

COCKROACH_USER = os.getenv('COCKROACH_USER')
COCKROACH_PW = os.getenv('COCKROACH_PW')
COCKROACH_HOST = os.getenv('COCKROACH_HOST')
COCKROACH_PORT = os.getenv('COCKROACH_PORT')
COCKROACH_DB = os.getenv('COCKROACH_DB')

REDIS_USER = os.getenv('REDIS_USER')
REDIS_KEY = os.getenv('REDIS_KEY')
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT')
REDIS_DB = os.getenv('REDIS_DB')

NEON_CONN_STR = f'postgresql+psycopg2://{NEON_DB_USER}:{NEON_DB_KEY}@{NEON_COMPUTE_NAME}.{NEON_LOCATION}.aws.neon.tech/{NEON_DB_NAME}'
MONGO_CONN_STR = f'mongodb+srv://{MONGO_DB_USER}:{MONGO_DB_KEY}@{MONGO_DB_NAME}.l0wmrp9.mongodb.net/?retryWrites=true&w=majority'
COCKROACH_CONN_STR = f'postgresql://{COCKROACH_USER}:{COCKROACH_PW}@{COCKROACH_HOST}:{COCKROACH_PORT}/{COCKROACH_DB}'

def pg_connection(conn_params: DBConstLoader) -> psycopg2.extensions.connection:
    '''Initiates backend PostgreSQL database connection'''
    # engine = create_engine(conn_params.conn_str)
    conn = psycopg2.connect(host=conn_params.DB_HOST, 
                            dbname=conn_params.DB_NAME, 
                            user=conn_params.DB_USER, 
                            password=conn_params.DB_KEY, 
                            port=conn_params.DB_PORT)
    return conn

# def mongodb_connection(conn_params: DBConstLoader) -> pymongo.mongo_client.MongoClient:
#     '''Initiates backend MongoDB Atlas database connection'''
#     return pymongo.MongoClient(conn_params.conn_str)


# def mongodb_get_db(client: pymongo.mongo_client.MongoClient, conn_params: DBConstLoader) -> pymongo.database.Database:
#     '''Enables MongoDB client to respective database'''
#     return client[conn_params.DB_NAME]

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
    '''Initiates backend MongoDB Atlas database connection'''
    return pymongo.MongoClient(MONGO_CONN_STR)


def mongodb_get_db(client: pymongo.mongo_client.MongoClient) -> pymongo.database.Database:
    '''Enables MongoDB client to respective database'''
    return client[MONGO_DB_NAME]

def redis_connection(conn_params: DBConstLoader) -> redis.Redis:
    '''Initiates backend Redis connection'''
    rd = redis.Redis(host=conn_params.DB_HOST, port=conn_params.DB_PORT, password=conn_params.DB_KEY)
    return rd