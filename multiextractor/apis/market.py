from dotenv import load_dotenv
import requests
import json
import os
from datetime import datetime, timedelta

load_dotenv()

ALPHAVANTAGE_KEY = os.getenv('ALPHAVANTAGE_API')
OPTIONS = {
    'daily_price': 'TIME_SERIES_DAILY',
    'news_sentiment': 'NEWS_SENTIMENT',
    'treasury_yield': 'TREASURY_YIELD',
    'inflation': 'INFLATION'
}

URL_BASE = 'https://www.alphavantage.co/query?function={FUNCTION}{QUERY_PARAMS}&apikey={KEY}'

def get_alphavan_data(category: str, tickers: str | None = None, **params) -> dict | list[dict]:
    '''Get data from Alpha Vantage API'''
    
    function = OPTIONS.get(category, 'TIME_SERIES_DAILY')
    outputsize = '&outputsize=' + params.get('outputsize', 'compact')
    datatype = '&datatype=' + params.get('datatype', 'json')
    topics = '&topics=' + params.get('topics', 'technology')
    time_from = '&time_from=' + params.get('time_from', (datetime.now() - timedelta(days=1)).strftime('%Y%m%dT%H%M'))
    time_to = '&time_to=' + params.get('time_to', datetime.now().strftime('%Y%m%dT%H%M'))
    sort_by = '&sort=' + params.get('sort_by', 'LATEST')
    limit = '&limit=' + str(params.get('limit', 50))
    interval = '&interval=' + params.get('interval', 'daily')
    maturity = '&maturity=' + params.get('maturity', '3month')
    
    match category:
        case 'daily_price':
            symbol = '&symbol=' + tickers
            query_params = symbol + outputsize + datatype
        case 'news_sentiment':
            tickers = '&tickers=' + tickers
            query_params = tickers + topics + time_from + time_to + sort_by + limit
        case 'treasury_yield':
            query_params = interval + maturity
        case 'inflation':
            query_params = datatype
        case _:
            raise Exception('Category not input')
    
    url = URL_BASE.format(
        FUNCTION=function,
        QUERY_PARAMS=query_params,
        KEY=ALPHAVANTAGE_KEY
    )
    
    r = requests.get(url)
    data = r.json()
    return data
