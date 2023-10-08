import pymongo
import spacy
from dotenv import load_dotenv
import os

load_dotenv()

CATEGORY_MAP = {
    'general': 'general',
    'world': 'world',
    'nation': 'nation',
    'business': 'business',
    'technology': 'technology',
    'entertainment': 'entertainment',
    'sports': 'sports',
    'science': 'science',
    'health': 'health'
}

LANGUAGE_MAP = {
    'Chinese': 'zh',
    'English': 'en'
}

COUNTRY_MAP = {
    'Australia': 'au',
    'Brazil': 'br',
    'Canada': 'ca',
    'Hong Kong': 'hk',
    'United Kingdom': 'gb',
    'United States': 'us'
}

TEXT = pymongo.TEXT

SPACY_DEFAULT_MODEL = 'en_core_web_md'
SPACY_NLP = spacy.load(SPACY_DEFAULT_MODEL)

TRANSLATOR = str.maketrans({chr(10): '', chr(9): ''})

class SciDailyConstants:
    SCI_SITE_NAME = 'Science Daily'
    SCI_DOMAIN_NAME = 'https://www.sciencedaily.com'
    SCI_URL_FULL = f'{SCI_DOMAIN_NAME}/news/computers_math/artificial_intelligence/'
    SCI_HEADERS = {'User-Agent': 'Chrome/116.0.5845.110  Safari/18615.2.9.11.10'}
    

class DBConstLoader:
    _NEON_CONN_STR = 'postgresql+psycopg2://{DB_USER}:{DB_KEY}@{COMPUTE_NAME}.{COMPUTE_LOCATION}.aws.neon.tech/{DB_NAME}'
    _MONGO_CONN_STR = 'mongodb+srv://{DB_USER}:{DB_KEY}@{DB_NAME}.l0wmrp9.mongodb.net/?retryWrites=true&w=majority'
    _COCKROACH_CONN_STR = 'postgresql://{DB_USER}:{DB_KEY}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

    def __init__(self, data_name: str):
        self.DB_KEY = None
        self.DB_USER = None
        self.COMPUTE_NAME = None
        self.COMPUTE_LOCATION  = None
        self.DB_NAME = None
        self.DB_PORT = None
        self.DB_HOST = None
        self.data_name = data_name
        self._get_params()
        self.conn_str = self._build_conn_str()
        
    def _get_params(self):
        match self.data_name.lower():
            case ('gnews' | 'scidaily'):
                self.DB_KEY = os.getenv('NEON_CONN_KEY')
                self.DB_USER = os.getenv('NEON_USER')
                self.COMPUTE_NAME = os.getenv('NEON_COMPUTE')
                self.COMPUTE_LOCATION = os.getenv('NEON_LOCATION')
                self.DB_NAME = os.getenv('NEON_DB')
                self.DB_PORT = os.getenv('NEON_PORT')
                self.DB_HOST = f'{self.COMPUTE_NAME}.{self.COMPUTE_LOCATION}.aws.neon.tech'
            case 'alphavan':
                self.DB_USER = os.getenv('ATLAS_USER')
                self.DB_KEY = os.getenv('ATLAS_KEY')
                self.DB_NAME = os.getenv('ATLAS_DB')
            case ('trade_econ' | 'cnbc'):
                self.DB_USER = os.getenv('COCKROACH_USER')
                self.DB_KEY = os.getenv('COCKROACH_PW')
                self.DB_HOST = os.getenv('COCKROACH_HOST')
                self.DB_PORT = os.getenv('COCKROACH_PORT')
                self.DB_NAME = os.getenv('COCKROACH_DB')
            case _:
                return Exception('No paramaters found for connection.')
    
    def _build_conn_str(self):
        match self.data_name.lower():
            case 'gnews' | 'scidaily':
                _str = self._NEON_CONN_STR.format(
                    DB_USER=self.DB_USER,
                    DB_KEY=self.DB_KEY,
                    COMPUTE_NAME=self.COMPUTE_NAME,
                    COMPUTE_LOCATION=self.COMPUTE_LOCATION,
                    DB_NAME=self.DB_NAME,
                )
            case 'alphavan':
                _str = self._MONGO_CONN_STR.format(
                    DB_USER=self.DB_USER,
                    DB_KEY=self.DB_KEY,
                    DB_NAME=self.DB_NAME
                )
            case 'trade_econ' | 'cnbc':
                _str = self._COCKROACH_CONN_STR.format(
                    DB_USER=self.DB_USER,
                    DB_KEY=self.DB_KEY,
                    DB_HOST=self.DB_HOST,
                    DB_PORT=self.DB_PORT,
                    DB_NAME=self.DB_NAME
                )
            case _:
                raise Exception('Connection string cannot be built.')
        return _str