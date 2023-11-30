import pandas as pd
import polars as pl
from datetime import datetime
import io
import json
import polars as pl
from google.cloud import bigquery
from multiextractor.apis.db import neondb_connection, pg_connection, redis_connection
from multiextractor.constants import DBConstLoader, CloudDBConnect


def sql_insert_articles(df: pd.DataFrame, constraint_col: str | None, conn_params: DBConstLoader | None = None):
    '''
    Creates and executes SQL script for article insertion
    
    :params:
    df: DataFrame object - table containing data to be inserted
    constraint_col: str - column name indicating key column upon detecting duplicattes
    '''
    # conn = pg_connection(conn_params)
    conn = neondb_connection()
    cur = conn.cursor()
    
    sql_col_str = ''.join(['("', '","'.join(df.columns), '")'])
    sql_val_str = ''.join(['(', ','.join(['%s'] * df.shape[1]), ')'])
    args_str = ','.join(cur.mogrify(sql_val_str, x).decode('utf-8') for x in df.values)
    
    if constraint_col is None:
        insert_script = f'INSERT INTO articles {sql_col_str} VALUES {args_str}'
    else:
        sql_col_ups_str = ''.join(['(EXCLUDED."', '", EXCLUDED."'.join(df.columns), '")'])
        update_query_template = '{} = {}'.format(sql_col_str, sql_col_ups_str)
        insert_script = f'''
            INSERT INTO articles {sql_col_str}
            VALUES {args_str}
            ON CONFLICT ("{constraint_col}") DO
            UPDATE SET {update_query_template}
        '''
    # print(insert_script)
    try:
        cur.execute(insert_script)
        conn.commit()
        conn.close()
        print('All articles inserted')
    except Exception as e:
        print(e)
        print('Error encountered in article insertion process')

def sql_create_table(df: pd.DataFrame, table_name: str = 'articles', unique_col: str | None = None, conn_params: DBConstLoader | None = None):
    '''
    Creates and executes SQL script for article table creation
    
    :params:
    df: DataFrame object - data table to extract table schema for SQL table creation query
    table_name: str - name of table to be created in Postgres database
    unique_col: str - name of column to set as unique index in created table
    conn_params: DBConstLoader object - preset connection parameters based on data source loading
    '''
    table_script = pd.io.sql.get_schema(df, table_name)
    idx = table_script.index('TABLE')
    table_script = table_script[:idx + len('TABLE')] + ' IF NOT EXISTS' + table_script[idx + len('TABLE'):]

    if unique_col is not None:
        # to_replace_str = '"' + unique_col + '"'
        to_replace_str = ','
        idx = table_script.index(to_replace_str)
        # len_replace_str = len(to_replace_str)
        # table_script = table_script[: (idx + len_replace_str)] + ' UNIQUE' + table_script[(idx + len_replace_str):]
        table_script = table_script[:idx] + ' UNIQUE' + table_script[idx:]

    # print(table_script)
    # with pg_connection(conn_params) as conn:
    with neondb_connection() as conn:
        try:
            cur = conn.cursor()
            cur.execute(table_script)
        except Exception as e:
            print(e)
            print('Error encountered in table creation process')
            
def key_val_insert(records: dict[dict[str]], conn_params: DBConstLoader | None = None) -> None:
    '''
    Creates and executes reocrds insert operation on Redis database
    
    :params:
    records: dict of dicts - data records to be inserted to database
    conn_params: DBConstLoader object - preset connection parameters based on data source loading
    '''
    rd = redis_connection(conn_params)
    with rd.pipeline() as pipe:
        for rec_id, rec in records.items():
            pipe.set(rec_id, json.dumps(rec))
    pipe.execute()
    
def dt_to_isoformat(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Converts datetime to readable datetime isoformat for insert to Redis database. Identifies all datetime columns and converts it into string-time format.
    
    :params:
    df: pd.DataFrame - Dataframe table containing extracted, pre-processed data
    '''
    cols = []
    tmp = df.copy()
    cols += tmp.select_dtypes(['datetimetz']).columns.tolist()
    cols += list(filter(lambda x: type(tmp[x].dropna()[0]).__name__ in [datetime.date.__name__, datetime.time.__name__], tmp.dtypes[tmp.dtypes == object].index.tolist()))
    if len(cols) > 0:
        for col in cols:
            tmp[col] = tmp[col].apply(lambda t: t.isoformat())
    return tmp

def create_redis_records(df: pd.DataFrame, redis_hash_idx_name: str) -> dict[dict[str]]:
    '''
    Constructs Redis hash and generates records from DataFrame. Hash is generated via combination of Dataframe index, table shortform, and current execution time.
    
    :params:
    df: pd.DataFrame - Dataframe table containing extracted, pre-processed data
    redis_hash_idx_name: str - Truncated table shortname of extracted API info
    '''
    tmp = df.copy()
    tmp['hash'] = tmp.reset_index()['index'].astype(str).apply(lambda x: '_'.join([redis_hash_idx_name, datetime.now().strftime('%Y%m%d%H%M%S'), x.zfill(2)]))
    tmp_col_list = tmp.columns.tolist()
    tmp_col_list.insert(0, tmp.columns.tolist().pop())
    tmp = tmp[tmp_col_list[:-1]]
    tmp_recs = tmp.set_index('hash').to_dict('index')
    return tmp_recs

# def get_bq_type(col_name, data_type, main_col_name, override_table_schema, override_col_list):
#     bq_mode = 'NULLABLE'
#     bq_type = None
#     match data_type:
#         case pl.Float64:
#             bq_type = 'FLOAT64'
#         case pl.Int32 | pl.Int64:
#             bq_type = 'INT64'
#         case pl.Utf8:
#             bq_type = 'STRING'
#         case pl.Boolean:
#             bq_type = 'BOOL'
#         case pl.Datetime:
#             bq_type = 'DATETIME'
#             # bq_type = 'TIMESTAMP'
#             bq_mode = 'REQUIRED'
#         case pl.Date:
#             bq_type = 'DATE'
#             bq_mode = 'REQUIRED'
#         case pl.Time:
#             bq_type = 'TIME'
#             bq_mode = 'REQUIRED'            
#         case _:
#             pass
#             # match col_name:
#             #     case 'lat' | 'lon':
#             #         bq_type = 'FLOAT64'
#             #     case _:
#             #         raise Exception('Error processing BQ data type')
                
#     if override_col_list is not None:
#         if main_col_name in override_col_list:
#             match col_name:
#                 case 'feels_like_c' :
#                     bq_type = 'FLOAT64'
#                 case '1h_mm' | '3h_mm' | 'lat' | 'lon':
#                     bq_type = 'FLOAT64'
#                     if override_table_schema == 'current':
#                         bq_type = 'INT64'
#                 case 'part_of_day':
#                     bq_type = 'STRING'
#                 case _:
#                     raise Exception('Error processing BQ data type')
#     return bq_type, bq_mode
            
# def create_bq_schema(schema: dict, override_table_schema: str = None, override_col_list: list = None):
#     _bq_schema_list = []
#     for col_name, data_type in schema.items():
#         if isinstance(data_type, (pl.Struct, pl.List)):
#             if isinstance(data_type, pl.List):
#                 data_type_struct = data_type.inner
#                 bq_mode = 'REPEATED'
#             else:
#                 data_type_struct = data_type
#                 bq_mode = 'NULLABLE'
                
#             _tmp_struct_list = []
#             for col_name2, data_type2 in data_type_struct.to_schema().items():
#                 bq_type2, _ = get_bq_type(col_name2, 
#                                           data_type2,
#                                           col_name,
#                                           override_table_schema,
#                                           override_col_list)
#                 if col_name2 in ['1h_mm', '3h_mm']: col_name2 = '_'.join([col_name2.split('_')[-1], col_name2.split('_')[0]])
#                 _tmp_struct_list.append(bigquery.SchemaField(col_name2, bq_type2))
                
#             bq_type = 'RECORD'
#         else:
#             bq_type, bq_mode = get_bq_type(col_name, 
#                                            data_type, 
#                                            col_name,
#                                            override_table_schema,
#                                            override_col_list)
            
#         if isinstance(data_type, pl.Struct):
#             _field = bigquery.SchemaField(col_name, bq_type, mode=bq_mode, fields=tuple(_tmp_struct_list))
#         elif isinstance(data_type, pl.List):
#             if isinstance(data_type.inner, pl.Struct):
#                 _field = bigquery.SchemaField(col_name, bq_type, mode=bq_mode, fields=tuple(_tmp_struct_list))
#             else:
#                 _field = bigquery.SchemaField(col_name, bq_type, mode=bq_mode)
#         else:
#             _field = bigquery.SchemaField(col_name, bq_type, mode=bq_mode)
#         _bq_schema_list.append(_field)
#     return _bq_schema_list

# def create_load_bq_dataset(client, dataset_name):
#     dataset_id = f'{client.project}.{dataset_name}'
#     dataset = bigquery.Dataset(dataset_id)
#     dataset = client.create_dataset(dataset, timeout=30, exists_ok=True)
#     print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
#     return dataset
    
# def create_load_bq_table(client, dataset, table_name, table_schema):
#     table_id = f'{client.project}.{dataset.dataset_id}.{table_name}'
#     table = bigquery.Table(table_id, schema=table_schema)
#     table = client.create_table(table, timeout=30, exists_ok=True)
#     print('Created table {}.{}.{}'.format(table.project, table.dataset_id, table.table_id))
#     return table

# def load_table_to_gcp(client, data, dataset_name, table_name, table_schema, src_format='polars'):
#     dataset = create_load_bq_dataset(client, dataset_name)
#     table = create_load_bq_table(client, dataset, table_name, table_schema)
    
#     match src_format:
#         case 'polars':
#             with io.BytesIO() as stream:
#                 data.write_parquet(stream)
#                 stream.seek(0)
#                 job = client.load_table_from_file(
#                     stream,
#                     destination=table,
#                     job_config=bigquery.LoadJobConfig(
#                         source_format=bigquery.SourceFormat.PARQUET,
#                         ignore_unknown_values=True,
#                         schema=table_schema
#                     ),
#                 )
#             job.result()
#         case 'pandas':
#             job_config = bigquery.LoadJobConfig(schema=table_schema, write_disposition="WRITE_APPEND")
#             job = client.load_table_from_dataframe(data.to_pandas(), table, job_config=job_config)
#             job.result()            
#         case _:
#             raise Exception('Improper option')
        
#     print(f'Data loaded to {table.project}.{table.dataset_id}.{table.table_id}!')

class BQClimate:
    def __init__(self, client: None, **kwargs):
        self.table_schema = None
        self._is_registered = False
        if client is not None:
            self.client = client
        else:
            key_path = kwargs.get('key_path', None)
            self.client = CloudDBConnect(key_path).client
    
    def register_sources(self, dataset_name: str, table_name: str, table_schema: list[bigquery.SchemaField] | None = None):
        if self.table_schema is None:
            self.table_schema = table_schema
        assert self.table_schema is not None, 'No BigQuery schema was created or loaded externally.'
        assert not self.dataset_name in ['', None], 'No BigQuery dataset name provided.'
        assert not self.table_name in ['', None], 'No BigQuery table name provided.'
        self._create_load_dataset(dataset_name)
        self._create_load_table(table_name)
        self._is_registered = True
        
    def _set_dataset(self, dataset_name: str):
        dataset_id = f'{self.client.project}.{dataset_name}'
        _dataset = bigquery.Dataset(dataset_id)
        self.dataset = self.client.create_dataset(_dataset, timeout=30, exists_ok=True)
        
    def _set_table(self, table_name: str):
        table_id = f'{self.client.project}.{self.dataset.dataset_id}.{table_name}'
        _table = bigquery.Table(table_id, schema=self.table_schema)
        self.table = self.client.create_table(_table, timeout=30, exists_ok=True)
    
    def load_table_to_gcp(self, data: pl.DataFrame, src_format='polars'):
        assert self._is_registered, 'BQ dataset and table are not selected for data processing.'
        match src_format:
            case 'polars':
                with io.BytesIO() as stream:
                    data.write_parquet(stream)
                    stream.seek(0)
                    job = self.client.load_table_from_file(
                        stream,
                        destination=self.table,
                        job_config=bigquery.LoadJobConfig(
                            source_format=bigquery.SourceFormat.PARQUET,
                            ignore_unknown_values=True,
                            schema=self.table_schema
                        ),
                    )
                job.result()
            case 'pandas':
                job_config = bigquery.LoadJobConfig(schema=self.table_schema, write_disposition="WRITE_APPEND")
                job = self.client.load_table_from_dataframe(data.to_pandas(), self.table, job_config=job_config)
                job.result()            
            case _:
                raise Exception('Improper option')
            
        print(f'Data loaded to {self.table.project}.{self.table.dataset_id}.{self.table.table_id}!')
    
    def create_bq_schema(self, schema: dict, override_table_schema: str = None, override_col_list: list = None):
        _bq_schema_list = []
        for col_name, data_type in schema.items():
            if isinstance(data_type, (pl.Struct, pl.List)):
                if isinstance(data_type, pl.List):
                    data_type_struct = data_type.inner
                    bq_mode = 'REPEATED'
                else:
                    data_type_struct = data_type
                    bq_mode = 'NULLABLE'
                    
                _tmp_struct_list = []
                for col_name2, data_type2 in data_type_struct.to_schema().items():
                    bq_type2, _ = self._get_bq_type(col_name2, 
                                            data_type2,
                                            col_name,
                                            override_table_schema,
                                            override_col_list)
                    if col_name2 in ['1h_mm', '3h_mm']: col_name2 = '_'.join([col_name2.split('_')[-1], col_name2.split('_')[0]])
                    _tmp_struct_list.append(bigquery.SchemaField(col_name2, bq_type2))
                    
                bq_type = 'RECORD'
            else:
                bq_type, bq_mode = self._get_bq_type(col_name, 
                                            data_type, 
                                            col_name,
                                            override_table_schema,
                                            override_col_list)
                
            if isinstance(data_type, pl.Struct):
                _field = bigquery.SchemaField(col_name, bq_type, mode=bq_mode, fields=tuple(_tmp_struct_list))
            elif isinstance(data_type, pl.List):
                if isinstance(data_type.inner, pl.Struct):
                    _field = bigquery.SchemaField(col_name, bq_type, mode=bq_mode, fields=tuple(_tmp_struct_list))
                else:
                    _field = bigquery.SchemaField(col_name, bq_type, mode=bq_mode)
            else:
                _field = bigquery.SchemaField(col_name, bq_type, mode=bq_mode)
            _bq_schema_list.append(_field)
        if len(_bq_schema_list) > 0: self.table_schema = _bq_schema_list
    
    @staticmethod
    def _get_bq_type(col_name: str, data_type, main_col_name: str, override_table_schema: str, override_col_list: list):
        bq_mode = 'NULLABLE'
        bq_type = None
        match data_type:
            case pl.Float64:
                bq_type = 'FLOAT64'
            case pl.Int32 | pl.Int64:
                bq_type = 'INT64'
            case pl.Utf8:
                bq_type = 'STRING'
            case pl.Boolean:
                bq_type = 'BOOL'
            case pl.Datetime:
                bq_type = 'DATETIME'
                # bq_type = 'TIMESTAMP'
                bq_mode = 'REQUIRED'
            case pl.Date:
                bq_type = 'DATE'
                bq_mode = 'REQUIRED'
            case pl.Time:
                bq_type = 'TIME'
                bq_mode = 'REQUIRED'            
            case _:
                pass
                    
        if override_col_list is not None:
            if main_col_name in override_col_list:
                match col_name:
                    case 'feels_like_c' :
                        bq_type = 'FLOAT64'
                    case '1h_mm' | '3h_mm' | 'lat' | 'lon':
                        bq_type = 'FLOAT64'
                        if override_table_schema == 'current':
                            bq_type = 'INT64'
                    case 'part_of_day':
                        bq_type = 'STRING'
                    case _:
                        raise Exception('Error processing BQ data type')
        return bq_type, bq_mode