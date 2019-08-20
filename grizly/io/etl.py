import boto3
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import pandas as pd
import csv

from grizly.core.utils import (
    read_config, 
    check_if_exists
)


config = read_config()
os.environ["HTTPS_PROXY"] = config["https"]


def to_csv(qf,csv_path, sql, engine, sep='\t'):
    """
    Writes table to csv file.

    Parameters:
    ----------
    csv_path : string
        Path to csv file.
    sql : string
        SQL query.
    engine : str
        Engine string.
    sep : string, default '\t'
        Separtor/delimiter in csv file.
    """
    engine = create_engine(engine, encoding='utf8', poolclass=NullPool)
        
    try:
        con = engine.connect().connection
        cursor = con.cursor()
        cursor.execute(sql)
    except:
        con = engine.connect().connection
        cursor = con.cursor()
        cursor.execute(sql)

    with open(csv_path, 'w', newline='', encoding = 'utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=sep)
        writer.writerow(qf.data["select"]["sql_blocks"]["select_aliases"]) 
        writer.writerows(cursor.fetchall())

    cursor.close()
    con.close()


def create_table(qf, table, engine, schema=''):
    """
    Creates a new table in database if the table doesn't exist.

    Parameters:
    ----------
    qf : QFrame object
    table : string
        Name of SQL table.
    engine : str
        Engine string.
    schema : string, optional
        Specify the schema.
    """
    engine = create_engine(engine, encoding='utf8', poolclass=NullPool)

    table_name = f'{schema}.{table}' if schema else f'{table}' 

    if check_if_exists(table, schema):
        print("Table {} already exists.".format(table_name))

    else:
        sql_blocks = qf.data["select"]["sql_blocks"]
        columns = []
        for item in range(len(sql_blocks["select_aliases"])):
            column = sql_blocks["select_aliases"][item] + ' ' + sql_blocks["types"][item]
            columns.append(column)

        columns_str = ", ".join(columns)  
        sql = "CREATE TABLE {} ({})".format(table_name, columns_str)
        
        con = engine.connect()
        con.execute(sql)
        con.close()
    
        print("Table {} has been created successfully.".format(table_name))


def csv_to_s3(csv_path, s3_name):
    """
    Writes csv file to s3 in 'teis-data' bucket.

    Parameters:
    ----------
    csv_path : string
        Path to csv file.
    s3_name : string
        Name of s3. 
    """
    s3 = boto3.resource('s3', aws_access_key_id=config["akey"], aws_secret_access_key=config["skey"], region_name=config["region"])
    bucket = s3.Bucket('teis-data')

    if s3_name[-4:] != '.csv': s3_name = s3_name + '.csv'

    bucket.upload_file(csv_path, 'bulk/' + s3_name)
    print('{} file uploaded to s3 as {}'.format(os.path.basename(csv_path), s3_name))


def s3_to_csv(s3_name, csv_path):
    """
    Writes s3 in 'teis-data' bucket to csv file .

    Parameters:
    ----------
    s3_name : string
        Name of s3. 
    csv_path : string
        Path to csv file.
    """
    s3 = boto3.resource('s3', aws_access_key_id=config["akey"], aws_secret_access_key=config["skey"], region_name=config["region"])
    bucket = s3.Bucket('teis-data')

    if s3_name[-4:] != '.csv': s3_name = s3_name + '.csv'

    with open(csv_path, 'wb') as data:
        bucket.download_fileobj('bulk/' + s3_name, data)
    print('{} uploaded to {}'.format(s3_name, csv_path))



def df_to_s3(df, table_name, schema, dtype="", sep='\t', engine=None, keep_csv=False):

    """Copies a dataframe inside a Redshift schema.table
        using the bulk upload via this process:
        df -> local csv -> s3 csv -> redshift table

        NOTE: currently this function performs a delete * in
        the target table, append is in TODO list, also we
        need to add a timestamp column

        COLUMN TYPES: right now you need to do a DROP TABLE to
        change the column type, this needs to be changed TODO
    """

    ACCESS_KEY = config['akey']
    SECRET_KEY = config['skey']
    REGION = config['region']

    if engine is None:
        engine = create_engine("mssql+pyodbc://Redshift", poolclass=NullPool)

    s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name=REGION)
    bucket = s3.Bucket('teis-data')

    print('s3 bucket object created')


    filename = table_name + '.csv'
    filepath = os.path.join(os.getcwd(), filename)

    df.columns = df.columns.str.strip().str.replace(" ", "_") # Redshift won't accept column names with spaces
    df.to_csv(filepath, sep=sep, encoding='utf-8', index=False)
    print(f'{filename} created in {filepath}')

    bucket.upload_file(filepath, f"bulk/{filename}")
    print(f'bulk/{filename} file uploaded to s3')

    try:
        if dtype !="":
            df.head(1).to_sql(table_name, schema=schema, index=False, con=engine, dtype=dtype)
        else:
            df.head(1).to_sql(table_name, schema=schema, index=False, con=engine)
    except:
        engine = create_engine("mssql+pyodbc://Redshift")
        if dtype !="":
            df.head(1).to_sql(table_name, schema=schema, index=False, con=engine, dtype=dtype)
        else:
            df.head(1).to_sql(table_name, schema=schema, index=False, con=engine)


def s3_to_rds_qf(qf, table, s3_name, schema='', if_exists='fail', sep='\t', use_col_names=True):
    """
    Writes s3 to Redshift database.

    Parameters:
    -----------
    qf : {None, QFrame}, default None
        QFrame object or None  
    table : string
        Name of SQL table.
    s3_name : string
        Name of s3. 
    schema : string, optional
        Specify the schema.
    if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.
            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values. NOTE: It won't drop the table.
            * append: Insert new values to the existing table.
    sep : string, default '\t'
        Separator/delimiter in csv file.
    use_col_names : boolean, default True
        If True the data will be loaded by the names of columns.
    """
    engine = create_engine("mssql+pyodbc://Redshift", encoding='utf8', poolclass=NullPool)
    
    table_name = f'{schema}.{table}' if schema else f'{table}'

    if check_if_exists(table, schema):
        if if_exists == 'fail':
            raise ValueError("Table {} already exists".format(table_name))
        elif if_exists == 'replace':
            sql ="DELETE FROM {}".format(table_name)
            engine.execute(sql)
            print('SQL table has been cleaned up successfully.')
        else:
            pass
    else:
        create_table(qf, table, engine="mssql+pyodbc://Redshift", schema=schema)

    if s3_name[-4:] != '.csv': s3_name += '.csv'
    
    col_names = '(' + ', '.join(qf.data['select']['sql_blocks']['select_aliases']) + ')' if use_col_names else ''

    print("Loading {} data into {} ...".format(s3_name,table_name))

    sql = """
        COPY {} {} FROM 's3://teis-data/bulk/{}' 
        access_key_id '{}' 
        secret_access_key '{}'
        delimiter '{}'
        NULL ''
        IGNOREHEADER 1
        REMOVEQUOTES
        ;commit;
        """.format(table_name, col_names, s3_name, config["akey"], config["skey"], sep)

    engine.execute(sql)
    print('Data has been copied to {}'.format(table_name))


def s3_to_rds(table, s3_name, schema='', if_exists='fail', sep='\t'):
    """
    Writes s3 to Redshift database.

    Parameters:
    -----------
    table : string
        Name of SQL table.
    s3_name : string
        Name of s3. 
    schema : string, optional
        Specify the schema.
    if_exists : {'fail', 'replace', 'append'}, default 'fail'
            How to behave if the table already exists.
            * fail: Raise a ValueError.
            * replace: Clean table before inserting new values. NOTE: It won't drop the table.
            * append: Insert new values to the existing table.
    sep : string, default '\t'
        Separator/delimiter in csv file.
    """
    engine = create_engine("mssql+pyodbc://Redshift", encoding='utf8', poolclass=NullPool)
    
    table_name = f'{schema}.{table}' if schema else f'{table}'

    if check_if_exists(table, schema):
        if if_exists == 'fail':
            raise ValueError("Table {} already exists".format(table_name))
        elif if_exists == 'replace':
            sql ="DELETE FROM {}".format(table_name)
            engine.execute(sql)
            print('SQL table has been cleaned up successfully.')
        else:
            pass

    if s3_name[-4:] != '.csv': s3_name += '.csv'

    print("Loading {} data into {} ...".format(s3_name,table_name))

    sql = """
        COPY {} FROM 's3://teis-data/bulk/{}' 
        access_key_id '{}' 
        secret_access_key '{}'
        delimiter '{}'
        NULL ''
        IGNOREHEADER 1
        REMOVEQUOTES
        ;commit;
        """.format(table_name, s3_name, config["akey"], config["skey"], sep)

    engine.execute(sql)
    print('Data has been copied to {}'.format(table_name))