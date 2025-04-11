import pandas as pd
import sqlite3

import config
from utils.logging_utils import logging_utils
from utils.etl_utils import extract_utils
from utils.etl_utils import transform_utils

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

    logging_utils.log_progress(f"run_query(): query: {query_statement}")
    logging_utils.log_progress(f"run_query(): output:\n{query_output.to_string()}")
    logging_utils.log_progress(f"Process Complete")


############ Main Process ############
logging_utils.set_path_to_logfile(config.PATH_TO_LOGFILE)

logging_utils.log_progress("Preliminaries complete. Initiating ETL process")

extracted_df = extract_utils.extract(url=config.WEBSCRAPING_TARGET_URL)

logging_utils.log_progress("Data extraction complete. Initiating Transformation process")
transformed_df = transform_utils.transform(extracted_df, config.PATH_TO_EXCHANGE_RATE_CSV)

logging_utils.log_progress("Data transformation complete. Initiating Loading process")
sql_connection = sqlite3.connect(config.PATH_TO_OUTPUT_DB)
logging_utils.log_progress(f"SQL Connection initiated, '{config.PATH_TO_OUTPUT_DB}'")
load(transformed_df, sql_connection)

run_query(f"SELECT * FROM {config.OUTPUT_TABLE_NAME}", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {config.OUTPUT_TABLE_NAME}", sql_connection)
run_query(f"SELECT Name FROM {config.OUTPUT_TABLE_NAME} LIMIT 5", sql_connection)

sql_connection.close()
logging_utils.log_progress("SQL Connection closed")
logging_utils.log_progress("ETL process finished\n")