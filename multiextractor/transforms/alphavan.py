import pandas as pd
import re
import copy
import numpy as np

def extract_price_data(data: dict) -> pd.DataFrame:
    '''
    Process price data from Alpha Vantage API resquest for subsequent loading to database.  Relabels column names, and converts all dates to acceptable format.
    
    :params:
    data: dict - JSON request from API call
    '''
    tmp = pd.DataFrame(data['Time Series (Daily)']).T
    tmp['ticker'] = data['Meta Data']['2. Symbol']
    tmp['last_refreshed'] = data['Meta Data']['3. Last Refreshed']
    tmp['time_zone'] = data['Meta Data']['5. Time Zone']
    tmp.reset_index(names='date', inplace=True)
    tmp.columns = tmp.columns.str.replace('\d+\.', '', regex=True).str.strip()
    tmp['date'] = pd.to_datetime(tmp['date']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    tmp['last_refreshed'] = pd.to_datetime(tmp['last_refreshed']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    return tmp

def extract_perc_data(data: dict) -> pd.DataFrame:
    '''
    Process percentage-related data from Alpha Vantage API resquest for subsequent loading to database.  Replaces all absent values with NaN and converts all dates to acceptable format.
    
    :params:
    data: dict - JSON request from API call    
    '''
    df = pd.DataFrame.from_dict(data)
    df['value'] = df['value'].apply(lambda x: float(x) if x != '.' else np.nan) / 100
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    return df

def extract_main_article(article: dict) -> tuple[pd.DataFrame, str, str]:
    '''
    Processes article to proper input format into MongoDB collection, including datetime reformatting.  Further extracts article title and time published for use as index in other related article collections.
    
    :params:
    article: dict - Extracted article from Alpha Vantage API

    '''
    tmp_base = pd.DataFrame.from_dict([article])
    tmp_base['time_published'] = pd.to_datetime(tmp_base['time_published']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    _index_name = tmp_base['title'].values[0]
    _index_time = tmp_base['time_published'].values[0]
    del tmp_base['topics'], tmp_base['ticker_sentiment']
    return tmp_base, _index_name, _index_time

def generate_sentiment_data_dict(defn: str) -> list[dict]:
    '''
    Reformats extracted sentiment data definition form Alpha Vantage API into separate entries for acceptable formatting.
    
    :params:
    defn: str - contains definition of sentiment labels and score bounds
    '''
    defn_parts = [parts.strip() for parts in defn.split(';')]
    score_bounds = [re.findall('(\-{0,1}\d+\.\d+)', part) for part in defn_parts]
    sentiments = [part.split(':')[-1].strip() for part in defn_parts]
    for i in range(len(score_bounds)):
        if i == 0:
            score_bounds[i].insert(0, 'NaN')
        elif i == len(score_bounds) - 1:
            score_bounds[i].append('NaN')
            
    sent_data_list = []
    for bounds, sent_label in zip(score_bounds, sentiments):
        sent_data_list.append({
            'sentiment': sent_label,
            'lower_bound': bounds[0],
            'upper_bound': bounds[1]
        })
    return sent_data_list

def extract_ticker_sentiment_topic(article: list[dict], index_name: str | None = None, index_time: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Processes topics and sentiments of related companies associated with article as extracted from Alpha Vantage API.  Converts into acceptable format.
    
    :params:
    article: list of dict - extracted article(s)
    index_name: str or None - article title used as index for database storage
    index_time: str or None - article published date as index for database storage
    '''
    tmp_article = copy.deepcopy(article)
    tmp_article['authors'] = ','.join(tmp_article['authors'])
    _sent = pd.DataFrame(tmp_article['ticker_sentiment'])
    _topics = pd.DataFrame.from_dict(tmp_article['topics'])
    
    if (index_name is not None) | (index_time is not None):
        assert isinstance(index_name, str) & isinstance(index_time, str)
        _sent = _sent.assign(title=index_name, time_published=index_time)
        _topics = _topics.assign(title=index_name, time_published=index_time)
    return _sent, _topics

def insert_to_collection(db: object, collection_name: str, item: list, set_index: list[tuple] | None = None, **kwargs):
    '''
    Executes insert of items into MongoDB collection of pointed database reference.
    
    :params:
    db: MongoDB database instance - object referencing to database
    collection_name: MongoDB collection instance - object referencing to collection
    item: list of dicts - contain article item to be inserted to collection
    set_index: list of tuples containing str and integer/enum value - if required, index parameters for index creation on collection to allow easier sorting and search
    **kwargs: dict - contain other parameters for insertion method
    '''
    _collection = db[collection_name]
    if set_index is not None:
        unique = kwargs.get('unique', False)
        _collection.create_index(set_index, unique=unique)
    
    if isinstance(item, list):
        if len(item) > 1:
            ordered = kwargs.get('ordered', True)
            _collection.insert_many(item, ordered=ordered)
        else:
            _collection.insert_one(item[0])
    else:
        _collection.insert_one(item[0])
        
def check_doc_presence(db_name: object, collection_name: object, column: str, condition: dict) -> list[str]:
    '''
    Conducts document query on MongoDB collection to indicate and return (if any) presence of documents based on entered query.
    
    :params:
    db_name: MongoDB database instance - object referencing to database
    collection_name: MongoDB collection instance - object referencing to collection
    column: str - name of key column of document to be searched under
    condition: dict - MongoDB-structured query for search
    '''
    query = {column: condition}
    return db_name[collection_name].find(query)

def extract_top_n(db_name: object, collection_name: object, column: str | list[str], n: int = 1) -> dict:
    '''
    Extracts top `n` document results after sorting of document done provided specified columns and sort order.
    
    :params:
    db_name: MongoDB database instance - object referencing to database
    collection_name: MongoDB collection instance - object referencing to collection
    column: str or list of str - name of key column(s) to be searched under given collection and respective order provided (current setting is 'reverse' order) 
    '''
    if isinstance(column, list):
        sort_cols = [(c, -1) for c in column]
    else:
        sort_cols = [(column, -1)]
    result = db_name[collection_name].find().sort(sort_cols).limit(n)
    return list(result)[0]

def subset_data(df: pd.DataFrame, db_name: object, collection_name: object, column: str):
    '''
    Creates a subset of original data by providing custom indices based on the exclusion of existing records in MongoDB collection.
    Carried out by extracting latest documents stored in collection and excluding records already in the to-be inputted data.
    
    :params:
    df: dataframe object - main data (i.e. latest extracted data pulled from API)
    db_name: MongoDB database instance - object referencing to database
    collection_name: MongoDB collection instance - object referencing to collection
    column: str or list of str - name of key column(s) to be searched under given collection and respective order provided (current setting is 'reverse' order) 
    '''
    top_values = extract_top_n(db_name, collection_name, column)
    idx = df[df['date'] == top_values['date']].index
    filtered_df = df.loc[:idx.values[0] - 1]
    if filtered_df.shape[0] == 0:
        return None
    return filtered_df