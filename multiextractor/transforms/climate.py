import polars as pl
import pandas as pd

def reformat_iqair(data: dict):
    return (
        pl
        .from_records([data])
        .unnest('weather')
        .unnest('pollution')
        .unnest('sys')
        .with_columns([
            pl.col('pollution_ts').dt.date().alias('pollution_date'), 
            pl.col('pollution_ts').dt.time().alias('pollution_time'), 
            pl.col('weather_ts').dt.date().alias('weather_date'),
            pl.col('weather_ts').dt.time().alias('weather_time')
        ])
        .drop(['pollution_ts', 'weather_ts'])
    )

def reformat_forecasted_ow(data: dict):
    df = pd.DataFrame.from_records(data)
    pdf_main = (
        pl
        .DataFrame(df)
        .unnest('sys')
        .unnest('main')
        .drop('weather')
        .with_columns(
            pl.col('part_of_day').cast(pl.Utf8, strict=False).alias('part_of_day'),
            pl.col('sunrise').cast(pl.Datetime, strict=False).alias('sunrise'),
            pl.col('sunrise').dt.date().alias('sunrise_date'),
            pl.col('sunrise').dt.time().alias('sunrise_time'),
            pl.col('sunset').dt.date().alias('sunset_date'),
            pl.col('sunset').dt.time().alias('sunset_time'),
            pl.col('date').dt.date().alias('measure_date'),
            pl.col('date').dt.time().alias('measure_time'),
            pl.col('forecasted_date').dt.date().alias('forecasted_date'),
            pl.col('forecasted_date').dt.time().alias('forecasted_time')                      
        )
        .drop(['sunrise', 'sunset', 'date'])    
    )

    pdf_weather_map = (
        pl
        .DataFrame(df[['forecasted_date', 'weather']])
        .explode('weather')
        .unnest('weather')
    )
    
    return pdf_main, pdf_weather_map

def reformat_current_ow(data: dict):
    pdf_main = (
        pl
        .DataFrame([data])
        .unnest('main')
        .unnest('sys')
        .with_columns(
            pl.col('sunrise').dt.date().alias('sunrise_date'),
            pl.col('sunrise').dt.time().alias('sunrise_time'),
            pl.col('sunset').dt.date().alias('sunset_date'),
            pl.col('sunset').dt.time().alias('sunset_time'),
            pl.col('date').dt.date().alias('measure_date'),
            pl.col('date').dt.time().alias('measure_time')                     
        )
        .drop(['sunrise', 'sunset', 'date'])    
    )

    pdf_weather_map = (
        pdf_main
        .select([pl.col('forecasted_date'), pl.col('weather')])
        .explode('weather')
        .unnest('weather')
    )
    
    return pdf_main, pdf_weather_map