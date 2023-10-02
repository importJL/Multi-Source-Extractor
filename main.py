import pandas as pd
import multiextractor
from multiextractor import SciDailyConstants as sci

def gnews_extract() -> pd.DataFrame:
    _, articles = multiextractor.extract_news()
    df_articles = pd.DataFrame.from_dict(articles)
    return df_articles

def gnews_transform(df_articles: pd.DataFrame) -> pd.DataFrame:
    df_articles = multiextractor.split_source(df_articles)
    df_articles = multiextractor.process_datetime(df_articles)
    df_articles = multiextractor.process_sentence_count(df_articles, multiextractor.SPACY_NLP, 'title', 'description', 'content')
    df_articles = multiextractor.process_token_count(df_articles, multiextractor.SPACY_NLP, 'title', 'description', 'content')
    return df_articles

def gnews_load(df_articles: pd.DataFrame) -> None:
    multiextractor.sql_create_table(df_articles, unique_col='title')
    multiextractor.sql_insert_articles(df_articles, constraint_col='title')

def scidaily_extract() -> tuple[list]:
    soup_heroes = multiextractor.extract_content(sci.SCI_URL_FULL, 'div[id*="heroes"]', 'select')[0]
    
    soup_latests = multiextractor.locate_elements(soup_heroes, 'div[class*="latest-head"]', 'select')
    article_titles = multiextractor.process_text(soup_heroes, 'title', method='select', element_search='div[class*="latest-head"]')
    article_summary = multiextractor.process_text(soup_heroes, 'story', method='select', element_search='div[class*="latest-summary"]', translator=multiextractor.TRANSLATOR)

    story_list, url_list, pub_date_full_list = [], [], []
    for soup_latest in soup_latests:
        soup_latest_a = multiextractor.locate_elements(soup_latest, 'a', 'find')
        href_latest = soup_latest_a['href']
        pub_date_full = multiextractor.extract_date(href_latest)

        STORY_URL = sci.SCI_DOMAIN_NAME + href_latest
        soup_story = multiextractor.extract_content(STORY_URL, 'div[id="story_text"]', 'select')[0]
        article_story = multiextractor.process_text(soup_story, 'story', translator=multiextractor.TRANSLATOR)
        
        story_list.append(article_story)
        url_list.append(STORY_URL)
        pub_date_full_list.append(pub_date_full)
        return article_titles, article_summary, story_list, url_list, pub_date_full_list

def scidaily_transform(
        article_titles: list[str], 
        article_summary: list[str], 
        story_list: list[str], 
        url_list: list[str], 
        pub_date_full_list: list[str]
    ) -> pd.DataFrame:
    
    df_articles_2 = pd.DataFrame({
        'title': article_titles,
        'description': article_summary,
        'content': story_list,
        'url': url_list,
        'image': '',
        'publishedAt': pub_date_full_list,
        'name': sci.SCI_SITE_NAME,
        'domainName': sci.SCI_DOMAIN_NAME
    })

    df_articles_2 = multiextractor.process_datetime(df_articles_2)
    df_articles_2 = multiextractor.process_sentence_count(df_articles_2, multiextractor.SPACY_NLP, 'title', 'description', 'content')
    df_articles_2 = multiextractor.process_token_count(df_articles_2, multiextractor.SPACY_NLP, 'title', 'description', 'content')
    return df_articles_2

def scidaily_load(df_articles_2: pd.DataFrame) -> None:
    multiextractor.sql_insert_articles(df_articles_2, constraint_col='title')

def alphavan_extract(ticker: str) -> tuple[dict | list[dict]]:
    ticker_prices = multiextractor.get_alphavan_data('daily_price', tickers=ticker)
    news = multiextractor.get_alphavan_data('news_sentiment', ticker)
    yield_data = multiextractor.get_alphavan_data('treasury_yield')
    inflation_data = multiextractor.get_alphavan_data('inflation')
    return ticker_prices, news, yield_data, inflation_data

def alphavan_transform(
        ticker_prices: dict | list[dict], 
        news: dict | list[dict], 
        yield_data: dict | list[dict], 
        inflation_data: dict | list[dict]
    ) -> tuple[list[dict], list[pd.DataFrame], list[pd.DataFrame], list[pd.DataFrame], tuple[pd.DataFrame | None], tuple[pd.DataFrame | None], tuple[pd.DataFrame | None]]:
    
    article_base_list, sentiments_list, topics_list = [], [], []
    _score_defn = news['sentiment_score_definition']
    sent_data_list = multiextractor.generate_sentiment_data_dict(_score_defn)
    
    for article in news['feed']:
        _df_ticker_article_base, index_name, index_time = multiextractor.extract_main_article(article)
        _df_ticker_sent, _df_ticker_topics = multiextractor.extract_ticker_sentiment_topic(article, index_name=index_name, index_time=index_time)
        article_base_list.append(_df_ticker_article_base)
        sentiments_list.append(_df_ticker_sent)
        topics_list.append(_df_ticker_topics)
    
    df_daily_treasury = multiextractor.extract_perc_data(yield_data['data'])
    df_annual_inflation = multiextractor.extract_perc_data(inflation_data['data'])
    df_stock_prices = multiextractor.extract_price_data(ticker_prices)
    return sent_data_list, article_base_list, sentiments_list, topics_list, df_daily_treasury, df_annual_inflation, df_stock_prices

def alphavan_load(
        sent_data_list: list[dict], 
        article_base_list: list[pd.DataFrame], 
        sentiments_list: list[pd.DataFrame], 
        topics_list: list[pd.DataFrame], 
        df_daily_treasury: tuple[pd.DataFrame | None], 
        df_annual_inflation: tuple[pd.DataFrame | None], 
        df_stock_prices: tuple[pd.DataFrame | None]
    ) -> None:
    
    client = multiextractor.mongodb_connection()
    db_name = multiextractor.mongodb_get_db(client)

    query_result = multiextractor.check_doc_presence(db_name, 'alphav_sentiment_reference', 'sentiment', {'$in': ['Bearish', 'Somewhat-Bearish', 'Neutral', 'Somewhat_Bullish', 'Bullish']})
    if len(list(query_result)) == 0:
        multiextractor.insert_to_collection(db_name, 'alphav_sentiment_reference', sent_data_list, set_index=[('sentiment', 1)], unique=True, ordered=False)
        
    for df_ticker_article_base, df_ticker_sent, df_ticker_topics in zip(article_base_list, sentiments_list, topics_list):
        query_result = multiextractor.check_doc_presence(db_name, 'alphav_news_main', 'title', {'$eq': df_ticker_article_base['title'].values[0]})
        if len(list(query_result)) == 0:
            multiextractor.insert_to_collection(db_name, 'alphav_news_main', df_ticker_article_base.to_dict(orient='records'), set_index=[('title', 1), ('time_published', 1)], unique=True, ordered=False)  
            multiextractor.insert_to_collection(db_name, 'alphav_news_topic', df_ticker_topics.to_dict(orient='records'), set_index=[('title', 1), ('time_published', 1), ('topic', 1)], unique=True, ordered=False)
            multiextractor.insert_to_collection(db_name, 'alphav_news_comp_sent', df_ticker_sent.to_dict(orient='records'), set_index=[('title', 1), ('time_published', 1), ('ticker', 1)], unique=True, ordered=False)

    df_daily_treasury = multiextractor.subset_data(df_daily_treasury, db_name, 'alphav_treasury', 'date')
    if df_daily_treasury is not None:
        multiextractor.insert_to_collection(db_name, 'alphav_treasury', df_daily_treasury.to_dict(orient='records'), set_index=[('date', 1)], unique=True, ordered=False)
    
    df_annual_inflation = multiextractor.subset_data(df_annual_inflation, db_name, 'alphav_inflation', 'date')
    if df_annual_inflation is not None:
        multiextractor.insert_to_collection(db_name, 'alphav_inflation', df_annual_inflation.to_dict(orient='records'), set_index=[('date', 1)], unique=True, ordered=False)
    
    df_stock_prices = multiextractor.subset_data(df_stock_prices, db_name, 'alphav_daily_price', ['date', 'ticker'])
    if df_stock_prices is not None:
        multiextractor.insert_to_collection(db_name, 'alphav_daily_price', df_stock_prices.to_dict(orient='records'), set_index=[('date', 1), ('last_refreshed', 1), ('ticker', 1)], unique=True, ordered=False)

if __name__ == '__main__':
    df_articles = gnews_extract()
    df_articles = gnews_transform(df_articles)
    gnews_load(df_articles)
    
    article_titles, article_summary, story_list, url_list, pub_date_full_list = scidaily_extract()
    df_articles_2 = scidaily_transform(article_titles, article_summary, story_list, url_list, pub_date_full_list)
    scidaily_load(df_articles_2)
    
    ticker_prices, news, yield_data, inflation_data = alphavan_extract('AAPL')
    sent_data_list, article_base_list, sentiments_list, topics_list, df_daily_treasury, df_annual_inflation, df_stock_prices = alphavan_transform(ticker_prices, news, yield_data, inflation_data)
    alphavan_load(sent_data_list, article_base_list, sentiments_list, topics_list, df_daily_treasury, df_annual_inflation, df_stock_prices)

# client = utils.mongodb_connection()
# db_name = utils.mongodb_get_db(client)

# score_defn = news['sentiment_score_definition']
# sent_data_list = utils.generate_sentiment_data_dict(score_defn)

# query_result = utils.check_doc_presence(db_name, 'alphav_sentiment_reference', 'sentiment', {'$in': ['Bearish', 'Somewhat-Bearish', 'Neutral', 'Somewhat_Bullish', 'Bullish']})
# if len(list(query_result)) == 0:
#     utils.insert_to_collection(db_name, 'alphav_sentiment_reference', sent_data_list, set_index=[('sentiment', 1)], unique=True, ordered=False)

# for article in news['feed']:
#     df_ticker_article_base, index_name, index_time = utils.extract_main_article(article)
#     df_ticker_sent, df_ticker_topics = utils.extract_ticker_sentiment_topic(article, index_name=index_name, index_time=index_time)
    
#     query_result = utils.check_doc_presence(db_name, 'alphav_news_main', 'title', {'$eq': df_ticker_article_base['title'].values[0]})
#     if len(list(query_result)) == 0:
#         utils.insert_to_collection(db_name, 'alphav_news_main', df_ticker_article_base.to_dict(orient='records'), set_index=[('title', 1), ('time_published', 1)], unique=True, ordered=False)  
#         utils.insert_to_collection(db_name, 'alphav_news_topic', df_ticker_topics.to_dict(orient='records'), set_index=[('title', 1), ('time_published', 1), ('topic', 1)], unique=True, ordered=False)
#         utils.insert_to_collection(db_name, 'alphav_news_comp_sent', df_ticker_sent.to_dict(orient='records'), set_index=[('title', 1), ('time_published', 1), ('ticker', 1)], unique=True, ordered=False)

# df_daily_treasury = utils.extract_perc_data(yield_data['data'])
# df_daily_treasury_2 = utils.subset_data(df_daily_treasury, db_name, 'alphav_treasury', 'date')
# if df_daily_treasury_2 is not None:
#     utils.insert_to_collection(db_name, 'alphav_treasury', df_daily_treasury_2.to_dict(orient='records'), set_index=[('date', 1)], unique=True, ordered=False)

# df_annual_inflation = utils.extract_perc_data(inflation_data['data'])
# df_annual_inflation_2 = utils.subset_data(df_annual_inflation, db_name, 'alphav_inflation', 'date')
# if df_annual_inflation_2 is not None:
#     utils.insert_to_collection(db_name, 'alphav_inflation', df_annual_inflation_2.to_dict(orient='records'), set_index=[('date', 1)], unique=True, ordered=False)

# df_stock_prices = utils.extract_price_data(ticker_prices)
# df_stock_prices_2 = utils.subset_data(df_stock_prices, db_name, 'alphav_daily_price', ['date', 'ticker'])
# if df_stock_prices_2 is not None:
#     utils.insert_to_collection(db_name, 'alphav_daily_price', df_stock_prices_2.to_dict(orient='records'), set_index=[('date', 1), ('last_refreshed', 1), ('ticker', 1)], unique=True, ordered=False)