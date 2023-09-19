import pandas as pd
from multiextractor.apis.db import neondb_connection


def sql_insert_articles(df: pd.DataFrame, constraint_col: str | None):
    '''
    Creates and executes SQL script for article insertion
    
    :params:
    df: DataFrame object - table containing data to be inserted
    constraint_col: str - column name indicating key column upon detecting duplicattes
    '''
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

def sql_create_table(df: pd.DataFrame, table_name: str = 'articles', unique_col: str | None = None):
    '''
    Creates and executes SQL script for article table creation
    
    :params:
    df: DataFrame object - data table to extract table schema for SQL table creation query
    table_name: str - name of table to be created in Postgres database
    unique_col: str - name of column to set as unique index in created table
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
    with neondb_connection() as conn:
        try:
            cur = conn.cursor()
            cur.execute(table_script)
        except Exception as e:
            print(e)
            print('Error encountered in table creation process')


