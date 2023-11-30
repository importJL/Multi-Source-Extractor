import pymongo
import spacy
from dotenv import load_dotenv
import os
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

load_dotenv()

CATEGORY = {
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

OPTIONS = {
    'daily_price': 'TIME_SERIES_DAILY',
    'news_sentiment': 'NEWS_SENTIMENT',
    'treasury_yield': 'TREASURY_YIELD',
    'inflation': 'INFLATION'
}

LANGUAGE = {
    'Chinese': 'zh',
    'English': 'en'
}

COUNTRY = {
    'Australia': 'au',
    'Brazil': 'br',
    'Canada': 'ca',
    'Hong Kong': 'hk',
    'United Kingdom': 'gb',
    'United States': 'us'
}

METRICS = {
    'aqius': 'AQI_US_STANDARD',
    'aqicn': 'AQI_CHINA_STANDARD',
    'mainus': 'MAIN_POLLUTANT_US',
    'maincn': 'MAIN_POLLUTANT_CHINA',
    'tp': 'TEMPERATURE_CELSIUS',
    'tp_min': 'MIN_TEMPERATURE_CELSIUS',
    'pr': 'ATMOSPHERIC_PRESSURE_HPA',
    'hu': 'HUMIDITY_PERC',
    'ws': 'WIND_SPEED_METRES_PER_SEC',
    'wd': 'WIND_DIRECTION_ANGLE',
    'ic': 'WEATHER_ICON_CODE',
    'p2': 'PM2.5_UGM3',
    'p1': 'PM10_UGM3',
    'o3': 'OZONE_PPB',
    'n2': 'NITROGEN_DIOXIDE_PPB',
    's2': 'SULFUR_DIOXIDE_PPB',
    'co': 'CARBON_MONOXIDE_PPM'
}

MAIN_POLLUTANT = {
    'conc': 'CONCENTRATION',
    'aqius': 'AQI_US_STANDARD',
    'aqicn': 'AQI_CHINA_STANDARD'
}

ICON_CODE = {
    '01d': 'CLEAR_SKY_DAY',
    '01n': 'CLEAR_SKY_NIGHT',
    '02d': 'FEW_CLOUDS_DAY',
    '02n': 'FEW_CLOUDS_NIGHT',
    '03d': 'SCATTERED_CLOUDS',
    '04d': 'BROKEN_CLOUDS',
    '09d': 'SHOWER_RAIN',
    '10d': 'RAIN_DAY',
    '10n': 'RAIN_NIGHT',
    '11d': 'THUNDERSTORM',
    '13d': 'SNOW',
    '50d': 'MIST'
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
        self.conn_str = self._build_conn_path()
        
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
            case 'cnbc':
                self.DB_USER = os.getenv('COCKROACH_USER')
                self.DB_KEY = os.getenv('COCKROACH_PW')
                self.DB_HOST = os.getenv('COCKROACH_HOST')
                self.DB_PORT = os.getenv('COCKROACH_PORT')
                self.DB_NAME = os.getenv('COCKROACH_DB')
            case 'trade_econ':
                self.DB_USER = os.getenv('REDIS_USER')
                self.DB_KEY = os.getenv('REDIS_KEY')
                self.DB_HOST = os.getenv('REDIS_HOST')
                self.DB_PORT = os.getenv('REDIS_PORT')
                self.DB_NAME = os.getenv('REDIS_DB')
            case _:
                return Exception('No paramaters found for connection.')
    
    def _build_conn_path(self):
        match self.data_name.lower():
            case 'gnews' | 'scidaily':
                _path = self._NEON_CONN_STR.format(
                    DB_USER=self.DB_USER,
                    DB_KEY=self.DB_KEY,
                    COMPUTE_NAME=self.COMPUTE_NAME,
                    COMPUTE_LOCATION=self.COMPUTE_LOCATION,
                    DB_NAME=self.DB_NAME,
                )
            case 'alphavan':
                _path = self._MONGO_CONN_STR.format(
                    DB_USER=self.DB_USER,
                    DB_KEY=self.DB_KEY,
                    DB_NAME=self.DB_NAME
                )
            case 'trade_econ' | 'cnbc':
                _path = self._COCKROACH_CONN_STR.format(
                    DB_USER=self.DB_USER,
                    DB_KEY=self.DB_KEY,
                    DB_HOST=self.DB_HOST,
                    DB_PORT=self.DB_PORT,
                    DB_NAME=self.DB_NAME
                )
            case _:
                raise Exception('Connection string cannot be built.')
        return _path
    
class CloudDBConnect:
    '''
    Client initializer for GCP BigQuery.
    '''
    def __init__(self, key_path: str | None = None):
        self.FROM_ENV = False
        if key_path is None: self.FROM_ENV = True
        
        self.DB_KEY = None
        self.KEY_PATH = key_path
        self._get_params()
        self.client = self._build_conn()
        
    def _get_params(self):
        match self.data_name.lower():
            case ('iqair' | 'openweather'):
                if self.FROM_ENV:
                    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ['GCLOUD_SERVICE_KEY_PATH']
            case _:
                return Exception('No paramaters found for connection.')
            
    def _build_conn(self):
        if self.KEY_PATH is not None:
            _credentials = Credentials.from_service_account_file(self.KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"])
            _client = bigquery.Client(project=_credentials.project_id, credentials=_credentials)
        else:
            _client = bigquery.Client()
        return _client