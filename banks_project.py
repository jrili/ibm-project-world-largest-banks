import pandas as pd
import sqlite3
import numpy as np


import config
from utils.logging_utils import logging_utils
from utils.etl_utils import extract_utils

exchange_rate_df = pd.DataFrame()

############ Transform Methods ############

# convert_currency: converts from USD to target_currency
# NOTE: download csv first:
#       wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
#
#   Possible values for target_currency: EUR, GBP, INR
def convert_currency(target_currency):
    #log_progress(f"convert_currency(): started, target_currency={target_currency}")

    global exchange_rate_df
    if exchange_rate_df.empty:
        exchange_rate_df = pd.read_csv(config.PATH_TO_EXCHANGE_RATE_CSV, index_col="Currency")
        exchange_rate_df.loc[:, "Rate"] = exchange_rate_df.loc[:,"Rate"].astype(float)

    exchange_rate = exchange_rate_df.loc[target_currency]["Rate"]

    #log_progress(f"convert_currency(): finished")
    return exchange_rate

def transform(df):
    logging_utils.log_progress("transform(): started")
    transformed_df = pd.DataFrame(columns=config.OUTPUT_COLUMNS)

    # Copy Name and MC_USD_Billion coDlumns
    transformed_df.loc[:, "Name"] = df.loc[:, "Name"].copy()
    transformed_df.loc[:, "MC_USD_Billion"] = df.loc[:, "MC_USD_Billion"].copy()

    # Compute the GBP, EUR, and INR column values based from the USD column values
    transformed_df.loc[:, "MC_GBP_Billion"] = np.round(df.loc[:, "MC_USD_Billion"].copy() * convert_currency('GBP'), 2)
    transformed_df.loc[:, "MC_EUR_Billion"] = np.round(df.loc[:, "MC_USD_Billion"].copy() * convert_currency('EUR'), 2)
    transformed_df.loc[:, "MC_INR_Billion"] = np.round(df.loc[:, "MC_USD_Billion"].copy() * convert_currency('INR'), 2)
    
    print(f"transform(): Transformed df:\n{transformed_df.head(10)}")
    print(f"For quiz: transformed_df['MC_EUR_Billion'][4]={transformed_df['MC_EUR_Billion'][4]}")
    logging_utils.log_progress(f"transform(): finished")
    return transformed_df


############ Load Methods ############
def load_to_csv(output_filename, transformed_df):
    logging_utils.log_progress("load_to_csv(): started")
    transformed_df.to_csv(output_filename)
    logging_utils.log_progress(f"Data saved to CSV file '{output_filename}'")

def load_to_db(sql_connection, output_table_name, transformed_df):
    logging_utils.log_progress("load_to_db(): started")
    transformed_df.to_sql(output_table_name, sql_connection, if_exists='replace', index='False')
    logging_utils.log_progress(f"Data loaded to Database as a table, Executing queries'")

def load(transformed_df, sql_connection):
    logging_utils.log_progress("load(): started")
    load_to_csv(config.PATH_TO_OUTPUT_CSV, transformed_df)
    load_to_db(sql_connection, config.OUTPUT_TABLE_NAME, transformed_df)
    logging_utils.log_progress("load(): finished")


############ DB Methods ############
def run_query(query_statement, sql_connection):
    logging_utils.log_progress(f"run_query(): started, query='{query_statement}'")
    query_output = pd.read_sql(query_statement, sql_connection)

    print(f"run_query(): query: {query_statement}")
    print(f"run_query(): output:\n{query_output.to_string()}")
    logging_utils.log_progress(f"Process Complete")


############ Main Process ############
logging_utils.set_path_to_logfile(config.PATH_TO_LOGFILE)

logging_utils.log_progress("Preliminaries complete. Initiating ETL process")

output_df = extract_utils.extract(url=config.WEBSCRAPING_TARGET_URL,
                        columns_upon_extraction=config.COLUMNS_UPON_EXTRACTION)

logging_utils.log_progress("Data extraction complete. Initiating Transformation process")
output_df = transform(output_df)

logging_utils.log_progress("Data transformation complete. Initiating Loading process")
sql_connection = sqlite3.connect(config.PATH_TO_OUTPUT_DB)
logging_utils.log_progress(f"SQL Connection initiated, '{config.PATH_TO_OUTPUT_DB}'")
load(output_df, sql_connection)

run_query(f"SELECT * FROM {config.OUTPUT_TABLE_NAME}", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {config.OUTPUT_TABLE_NAME}", sql_connection)
run_query(f"SELECT Name FROM {config.OUTPUT_TABLE_NAME} LIMIT 5", sql_connection)

sql_connection.close()
logging_utils.log_progress("SQL Connection closed")
logging_utils.log_progress("ETL process finished\n")