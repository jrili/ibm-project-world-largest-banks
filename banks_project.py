import sqlite3

from utils import config
from utils import logging_utils
from utils import extract_utils
from utils import transform_utils
from utils import load_utils
from utils import db_utils


# Extract data
logging_utils.log_progress("ETL prcess started. Extracting data...")
extracted_df = extract_utils.extract(url=config.WEBSCRAPING_TARGET_URL)

# Transform data
logging_utils.log_progress("Data extraction complete. Transforming data...")
transformed_df = transform_utils.transform(extracted_df, config.PATH_TO_EXCHANGE_RATE_CSV)

# Create connection to output DB
logging_utils.log_progress(f"Data transformation complete. \
                           Connecting to DB in '{config.PATH_TO_OUTPUT_DB}'...")
sql_connection = sqlite3.connect(config.PATH_TO_OUTPUT_DB)

# Load data
logging_utils.log_progress("DB connection established. Loading data...")
load_utils.load(transformed_df, config.PATH_TO_OUTPUT_CSV, config.OUTPUT_TABLE_NAME, sql_connection)

# Check DB contents by querying
db_utils.run_query(f"SELECT * FROM {config.OUTPUT_TABLE_NAME}", sql_connection)
db_utils.run_query(f"SELECT AVG(MC_GBP_Billion) FROM {config.OUTPUT_TABLE_NAME}", sql_connection)
db_utils.run_query(f"SELECT Name FROM {config.OUTPUT_TABLE_NAME} LIMIT 5", sql_connection)

# Finalize processing
sql_connection.close()
logging_utils.log_progress("SQL Connection closed")
logging_utils.log_progress("ETL process finished\n")
