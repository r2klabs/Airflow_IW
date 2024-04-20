from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import snowflake.connector
from snowflake.connector import DictCursor
import utilities as util
from datetime import date
from datetime import datetime


# DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}
dag = DAG(
    'polygon_to_snowflake_dag',
    default_args=default_args,
    schedule_interval='0 21 * * 1-5',
)

#Function definitions
def retrieve_stock_data():
    '''Function to retrieve the specified stock data from the Polygon API and store it in the Snowflake database.'''
    companies = ['AAPL','AMZN','GOOG','META','MSFT','NFLX', 'TSLA']  #company symbols to retrieve.
    
    today = date.today()
    stock_date = '2024-04-08'  #can manually change for testing purposes
    #stock_date = today   <-- would use in the live version
    
    #Establish connection to Snowflake
    connection = util.get_snowflake_connection()
    
    #Create a cursor object using a dictionary cursor to fetch rows as dictionaries
    global_cursor = connection.cursor(DictCursor)

    for company in companies:
        util.get_polygon_data(company, stock_date ,connection, global_cursor)
    
    global_cursor.close()
    connection.close()
    
def polygon_log():
    '''Logs if data retrieval is successful.'''
    now = datetime.now()
    dt_string = now.strftime("%Y-%d-%m,%H:%M:%S")
    with open('polygon.log', 'a') as f:
        f.write(f"{dt_string},AAPL, AMZN, GOOG, META, MSFT, NFLX, TSLA\n")  
        f.close()
        
# Task definitions    
retrieve_stock_data = PythonOperator(
    task_id='retrieve_stock_data',
    python_callable=retrieve_stock_data,
    dag=dag
)

polygon_log = PythonOperator(
    task_id='polygon_log',
    python_callable=polygon_log,
    dag=dag
)

# Task dependencies
retrieve_stock_data >> polygon_log