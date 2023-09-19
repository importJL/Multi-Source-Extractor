import pandas as pd
import spacy
import multiextractor


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    '''Renames columns of articles table'''
    
    _tmp = df.copy()
    _tmp = _tmp.rename({'descriptionNumTokens': 'descNumTokens', 'descriptionNumSents': 'descNumSents'}, axis=1)
    return _tmp

def split_source(df: pd.DataFrame) -> pd.DataFrame:
    '''Split source column struct into separate columns'''
    
    _base = df.copy()
    _source = _base['source'].apply(pd.Series).rename({'url': 'domainName'}, axis=1)
    return pd.concat([_base, _source], axis=1).drop('source', axis=1)
    
def process_datetime(df: pd.DataFrame, date_col='publishedAt') -> pd.DataFrame:
    '''Convert to datetime format and extract dates'''
    
    _tmp = df.copy()
    _tmp[date_col] = pd.to_datetime(_tmp[date_col])
    _tmp['publishedDate'] = _tmp[date_col].dt.date
    _tmp['publishedTime'] = _tmp[date_col].dt.time
    return _tmp

def process_sentence_count(df: pd.DataFrame, nlp: spacy.lang, *cols) -> pd.DataFrame:
    '''Get number of sentences from string columns'''
    
    _tmp = df.copy()
    for col in cols:
        _tmp[f'{col}NumSents'] = _tmp[col].apply(lambda x: len(multiextractor.extract_sentences(x, nlp)))
    return _tmp

def process_token_count(df: pd.DataFrame, nlp: spacy.lang, *cols) -> pd.DataFrame:
    '''Get number of word tokens from string columns'''
    
    _tmp = df.copy()
    for col in cols:
        _tmp[f'{col}NumTokens'] = _tmp[col].apply(lambda x: len(multiextractor.extract_tokens(x, nlp)))
    return _tmp