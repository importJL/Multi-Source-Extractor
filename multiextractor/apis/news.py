from dotenv import load_dotenv
import urllib.request
import feedparser
import pandas as pd
import tldextract
from urllib.parse import urlparse
from datetime import date, time
import os
import json
import requests
import asyncio
from bs4 import BeautifulSoup
from concurrent.futures import ProcessPoolExecutor
from multiextractor.transforms.soup_funcs import locate_elements, process_text

load_dotenv()

GNEWS_KEY = os.getenv('GNEWS_API')

def extract_news(**params) -> dict:
    '''
    Extract news articles with GNews API
    
    :params:
    **params: dict - custom parameters entered to build query string of URL for news extraction
    '''
    category = params.get('category', 'technology')
    language = params.get('language', 'en')
    country = params.get('country', 'us')
    max_articles = params.get('max_articles', 10)
    
    url = "https://gnews.io/api/v4/top-headlines?category={category}&lang={language}&country={country}&max={max_articles}&apikey={key}".format(
        category=category, language=language, country=country, max_articles=max_articles, key=GNEWS_KEY
    )
    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode("utf-8"))
        articles = data["articles"]
    return data, articles
    
def extract_content(url: str, element_search: str, method: str, headers: dict[str, str] | None = None):
    '''
    Extract news articles using conventional requests & BeautifulSoup
    
    :params:
    url: str - webiste URL to scrape articles from
    element_search: str - search string to be used to identify specific elements for extraction as per method used
    method: str - method of searching.  Values given include `select` and `find`.
    headers: dict[str, str] -  optional, URL headers to be input into request API for additional protocol specifications on scraping
    '''
    r = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(r.content, 'html5lib')
    return locate_elements(soup, element_search, method)

def extract_rss_body(url, body_attrs=None, list_attrs=None) -> str:
    ''''''
    _body_kwargs, _list_kwargs = {}, {}
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')
    
    if body_attrs is not None: _body_kwargs.update({'attrs': body_attrs})
    _art_body = locate_elements(soup, 'div', 'find', **_body_kwargs)
    
    if list_attrs is not None: _list_kwargs.update({'attrs': list_attrs})
    _body_list = process_text(_art_body, 'summary', 'find', 'div', **_list_kwargs)
    return ' '.join(_body_list)

async def parallel_rss_extract(executor: ProcessPoolExecutor, entry: object, body_attrs: dict[str, str] | None, list_attrs: dict[str, str] | None):
    ''''''
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        executor,
        extract_rss_body,
        entry.link, 
        body_attrs, 
        list_attrs
    ) if (body_attrs is not None) & (list_attrs is not None) else entry.summary
    
async def populate_data_struct(executor: ProcessPoolExecutor, entry: object, body_attrs: dict[str, str] | None, list_attrs: dict[str, str] | None):
    _summary = await parallel_rss_extract(executor, entry, body_attrs, list_attrs)
    return {
        'title': entry.title,
        'description': entry.summary,
        'content': _summary,
        'url': entry.link,
        'image': '',
        'publishedAt': entry.published,
        'name': tldextract.extract(entry.link).domain.title(),
        'domainName': urlparse(entry.link).netloc,
        'publishedDate': date(entry.published_parsed.tm_year, entry.published_parsed.tm_mon, entry.published_parsed.tm_mday),
        'publishedTime': time(entry.published_parsed.tm_hour, entry.published_parsed.tm_min, entry.published_parsed.tm_sec)
    }   

async def create_entry_from_rss(url: str, executor: ProcessPoolExecutor, body_attrs: dict[str, str] | None = None, list_attrs: dict[str, str] | None = None) -> pd.DataFrame:
    _feeds = feedparser.parse(url)
    _entry_list = await asyncio.gather(*(populate_data_struct(executor, _entry, body_attrs, list_attrs) for _entry in _feeds.entries))
    _df = pd.DataFrame(_entry_list)
    _df['publishedAt'] = pd.to_datetime(_df['publishedAt']).dt.strftime('%Y%m%d %H:%M:%S%z+00:00')
    return _df