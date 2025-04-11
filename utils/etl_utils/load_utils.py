import pandas as pd

from ..logging_utils import logging_utils

def load_to_csv(output_filename, transformed_df:pd.DataFrame):
    logging_utils.log_progress("load_to_csv(): started")
    transformed_df.to_csv(output_filename)
    logging_utils.log_progress(f"load_to_csv(): Data saved to CSV file '{output_filename}'")

def load_to_db(sql_connection, output_table_name, transformed_df):
    logging_utils.log_progress("load_to_db(): started")
    transformed_df.to_sql(output_table_name, sql_connection, if_exists='replace', index='False')
    logging_utils.log_progress(f"load_to_db(): Data loaded to Database as a table, Executing queries'")

def load(transformed_df, path_to_csv, output_table_name, sql_connection):
    logging_utils.log_progress("load(): started")
    load_to_csv(path_to_csv, transformed_df)
    load_to_db(sql_connection, output_table_name, transformed_df)
    logging_utils.log_progress("load(): finished")