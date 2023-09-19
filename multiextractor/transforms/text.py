from typing import Union, Optional, List
import re
from nltk.tokenize import word_tokenize
import spacy

def extract_tokens(text: List[str], remove_puncs: Optional[bool] = True) -> List[str]:
    '''
    Extracts tokens from string data using NLP tokenizer with option to remove punctuations.
    
    :params:
    text: List[Str] - given text in array
    remove_puncs: Bool - if True, carries out additional remove of non-alphanumeric characters
    '''
    _stripped_text = text.strip()
    _tokens = list(filter(lambda s: s != '', word_tokenize(_stripped_text)))
    if remove_puncs:
        alphanum_regex = re.compile(r'\w+')
        _tokens = alphanum_regex.findall(' '.join(_tokens))
    return _tokens

def extract_sentences(sent: List[str], nlp: spacy.lang):
    '''
    Extracts sentences from string data using NLP sentencizer.
    
    :params:
    sent: List[Str] - given text in array
    nlp: object - imported spacy english model
    '''
    _doc = nlp(sent)
    return list(_doc.sents)