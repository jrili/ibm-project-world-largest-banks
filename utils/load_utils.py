import pandas as pd

from . import logging_utils

def load_to_csv(output_filename, transformed_df:pd.DataFrame):
    """Performs the transform process for the needed bank data

    Keyword Arguments:
    output_filename -- file path to output CSV file
    transformed_df -- pandas dataframe containing ready-to-load data

    Return:
    None
    """

    logging_utils.log_progress("load_to_csv(): started")
    transformed_df.to_csv(output_filename)
    logging_utils.log_progress(f"load_to_csv(): Data saved to CSV file '{output_filename}'")

def load_to_db(sql_connection, output_table_name, transformed_df):
    """Load data to target DB

    Keyword Arguments:
    sql_connection -- SQL connection object for loading to DB
    output_table_name -- target table name for loading to DB
    transformed_df -- pandas dataframe containing ready-to-load data

    Return:
    None
    """
    logging_utils.log_progress("load_to_db(): started")
    transformed_df.to_sql(output_table_name, sql_connection, if_exists='replace', index='False')
    logging_utils.log_progress(f"load_to_db(): Data loaded to Database in table '{output_table_name}'")

def load(transformed_df, path_to_csv, output_table_name, sql_connection):
    """Performs the transform process for the needed bank data

    Keyword Arguments:
    transformed_df -- pandas dataframe containing ready-to-load data
    path_to_csv -- file path to output CSV file
    output_table_name -- target table name for loading to DB
    sql_connection -- SQL connection object for loading to DB

    Return:
    None
    """
    logging_utils.log_progress("load(): started")
    load_to_csv(path_to_csv, transformed_df)
    load_to_db(sql_connection, output_table_name, transformed_df)
    logging_utils.log_progress("load(): finished")
