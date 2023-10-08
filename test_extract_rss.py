import feedparser
from bs4 import BeautifulSoup
import requests
import asyncio
import pandas as pd
import multiextractor
import tldextract
from urllib.parse import urlparse
from datetime import date, time

def extract_rss_body(url, body_attrs=None, list_attrs=None) -> str:
    _body_kwargs, _list_kwargs = {}, {}
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')
    
    if body_attrs is not None: _body_kwargs.update({'attrs': body_attrs})
    _art_body = multiextractor.locate_elements(soup, 'div', 'find', **_body_kwargs)
    
    if list_attrs is not None: _list_kwargs.update({'attrs': list_attrs})
    _body_list = multiextractor.process_text(_art_body, 'summary', 'find', 'div', **_list_kwargs)
    return ' '.join(_body_list)

async def parallel_rss_extract(executor, entry, body_attrs, list_attrs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        executor,
        extract_rss_body,
        entry.link, 
        body_attrs, 
        list_attrs
    ) if (body_attrs is not None) & (list_attrs is not None) else entry.summary
    
async def populate_data_struct(executor, entry, body_attrs, list_attrs):
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

async def create_entry_from_rss(url, executor, body_attrs=None, list_attrs=None) -> pd.DataFrame:
    _feeds = feedparser.parse(url)
    _entry_list = await asyncio.gather(*(populate_data_struct(executor, _entry, body_attrs, list_attrs) for _entry in _feeds.entries))
    # loop = asyncio.get_event_loop()
    # for _entry in _feeds.entries:
    #     _entry.body = await loop.run_in_executor(
    #         executor,
    #         extract_rss_body,
    #         _entry.link, 
    #         body_attrs, 
    #         list_attrs
    #     ) if (body_attrs is not None) & (list_attrs is not None) else _entry.summary
    #     _tmp = {
    #         'title': _entry.title,
    #         'description': _entry.summary,
    #         'content': _entry.body,
    #         'url': _entry.link,
    #         'image': '',
    #         'publishedAt': _entry.published,
    #         'name': tldextract.extract(_entry.link).domain.title(),
    #         'domainName': urlparse(_entry.link).netloc,
    #         'publishedDate': date(_entry.published_parsed.tm_year, _entry.published_parsed.tm_mon, _entry.published_parsed.tm_mday),
    #         'publishedTime': time(_entry.published_parsed.tm_hour, _entry.published_parsed.tm_min, _entry.published_parsed.tm_sec)
    #     }
    #     _entry_list.append(_tmp)
    _df = pd.DataFrame(_entry_list)
    _df['publishedAt'] = pd.to_datetime(_df['publishedAt']).dt.strftime('%Y%m%d %H:%M:%S%z+00:00')
    return _df