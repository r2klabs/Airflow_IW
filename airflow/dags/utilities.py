import mysql.connector
import pandas as pd
import requests
import json
from datetime import date
import datetime
import time
import snowflake.connector
from snowflake.connector import DictCursor

"""This file contains utility functions for connecting to Polygon API and Snowflake."""

def get_bulk_stock_data(ticker, start_day, end_day):
    '''Uses the Polygon API to get stock data for a given ticker symbol and date range.'''
    request_string='https://api.polygon.io/v2/aggs/ticker/'+ticker+'/range/1/day/'+start_day+'/'+end_day+'?adjusted=true&sort=asc&apiKey=46ipIrH5BrzkTqZj7OUvik7XAFOpu8GJ'

    api_response = requests.get(request_string)
    data = api_response.text
    time.sleep(15) #Slows down the requests to avoid being blocked by the API.
    return json.loads(data)
    
def insert_data(ticker, row, local_cursor, cnx):
    '''Inserts stock data into a MySQL database.'''
    symbol = ticker
    local_date = str(row['t'])[0:-3]
    ticker_date = datetime.datetime.fromtimestamp(int(local_date)).date()
    open_price = row['o']
    high_price = row['h']
    low_price = row['l']
    close_price = row['c']
    volume = row['v']

    #print(symbol, ticker_date, open_price, high_price, low_price, close_price, volume)
                     
    sql = "INSERT INTO stock (symbol, ticker_date, open_price, high_price, low_price, close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (symbol, ticker_date, open_price, high_price, low_price, close_price, volume)
    local_cursor.execute(sql, val)
    cnx.commit()


def connect_mysql():
    '''Connects to a MySQL database.'''
    connection = mysql.connector.connect(user='stock_user', password='letmein',
                                host='127.0.0.1',
                                database='stock_data')
    return connection


def get_snowflake_connection(config_path='snowflake_config.json'):
    """Load configuration from the JSON file and establish connection 
    to Snowflake using the loaded configuration"""
    
    with open(config_path, 'r') as file:
        config = json.load(file)

    conn = snowflake.connector.connect(
        user=config['user'],
        password=config['password'],
        account=config['account'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema']
    )
    
    return conn

def close_snowflake_connection(cursor, conn):
    """CLoses both the shared cursor and connection to Snowflake"""
    cursor.close()
    conn.close()
    
    
def get_polygon_data(ticker, timestamp, cnx, local_cursor):
    request_string = 'https://api.polygon.io/v1/open-close/'+ticker+'/'+timestamp+'?adjusted=true&apiKey=46ipIrH5BrzkTqZj7OUvik7XAFOpu8GJ'
    api_response = requests.get(request_string)
    data = api_response.text
    parse_json = json.loads(data)
    db_row=[(v) for k, v in parse_json.items()]
    
    #for item in db_row:
        #print(item)
    
    if(db_row[0] != 'NOT_FOUND'):
    
        symbol = db_row[2]
        ticker_date = db_row[1]
        open_price = db_row[3]
        high_price = db_row[4]
        low_price = db_row[5]
        close_price = db_row[6]
        volume = db_row[7]

        sql = "INSERT INTO stock (symbol, ticker_date, open_price, high_price, low_price, close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (symbol, ticker_date, open_price, high_price, low_price, close_price, volume)
        
        local_cursor.execute(sql, val)
        cnx.commit()

        print(local_cursor.rowcount, f"record inserted for {ticker} on {timestamp}")
    time.sleep(15) #slows down the requests to avoid being blocked by the API.