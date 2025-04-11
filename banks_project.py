import pandas as pd
import sqlite3

import config
from utils.logging_utils import logging_utils
from utils.etl_utils import extract_utils
from utils.etl_utils import transform_utils
from utils.etl_utils import load_utils

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
load_utils.load(transformed_df, config.PATH_TO_OUTPUT_CSV, config.OUTPUT_TABLE_NAME, sql_connection)

run_query(f"SELECT * FROM {config.OUTPUT_TABLE_NAME}", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {config.OUTPUT_TABLE_NAME}", sql_connection)
run_query(f"SELECT Name FROM {config.OUTPUT_TABLE_NAME} LIMIT 5", sql_connection)

sql_connection.close()
logging_utils.log_progress("SQL Connection closed")
logging_utils.log_progress("ETL process finished\n")