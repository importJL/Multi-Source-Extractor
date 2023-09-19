import pymongo
import spacy

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