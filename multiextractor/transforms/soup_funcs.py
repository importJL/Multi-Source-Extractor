from datetime import datetime
import bs4

def locate_elements(soup: bs4.BeautifulSoup, element_search: str, method: str, **kwargs):
    '''
    Identifies web elements from given BeautifulSoup object for further data extraction.
    
    :params:
    soup: BeautifulSoup instance - contains HTML scraped elements and methods for element search
    element_search: str - search string to be used to identify specific elements for extraction as per method used
    method: str - method of searching.  Values given include `select` and `find`
    '''
    match method:
        case 'select':
            return soup.select(element_search, **kwargs)
        case 'find':
            return soup.find(element_search, **kwargs)
        
def process_text(soup: bs4.BeautifulSoup, nature: str, method: str | None = None, element_search: str | None = None, translator: dict | None = None, **kwargs):
    '''
    Executes text processing based on element identification and text extraction of scraped website.
    
    :params:
    soup: BeautifulSoup object - contains HTML scraped elements and methods for element search
    nature: str - category of text to which would be scraped from
    method: str - method of searching.  Values given include `select` and `find`
    element_search: str - search string to be used to identify specific elements for extraction as per method used
    translator: dict - mapper to convert all instances of identifiable instances to default value
    '''
    match nature:
        case 'title':
            soup_title = locate_elements(soup, element_search, method, **kwargs)
            return [e.text for e in soup_title]
        case 'summary':
            soup_summary = locate_elements(soup, element_search, method, **kwargs)
            if translator is not None:
                return [e.text.translate(translator).split('—')[-1].strip() for e in soup_summary]
            else:
                return [e.text for e in soup_summary]
        case 'story':
            return soup.text.translate(translator)
        case _:
            raise Exception('Please select correct text processing')
        
def extract_date(href):
    '''
    Extract and convert text date extracted from web element into readable full date-time format.
    
    :params:
    href: str - HREF element component of scraped object
    '''
    url_date_str = href.split('.')[0].split('/')[-1]
    url_date = datetime.strptime(url_date_str, '%y%m%d%H%M%S')
    pub_date_full = url_date.strftime('%Y%m%d %H:%M:%S%z+00:00')
    return pub_date_full

def process_date(href):
    '''
    Process date extracted from web element into partitioned date and time objects.
    
    :params:
    href: str - HREF element component of scraped object
    '''
    url_date_str = href.split('.')[0].split('/')[-1]
    url_date = datetime.strptime(url_date_str, '%y%m%d%H%M%S')
    pub_date_full = url_date.strftime('%Y%m%d %H:%M:%S%z+00:00')
    pub_date = url_date.strftime('%Y-%m-%d')
    pub_time = url_date.strftime('%H:%M:%S')
    return url_date, pub_date_full, pub_date, pub_time