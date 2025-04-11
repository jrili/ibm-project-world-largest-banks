import pandas as pd

from . import logging_utils

def run_query(query_statement, sql_connection):
    logging_utils.log_progress(f"run_query(): started, query='{query_statement}'")
    query_output = pd.read_sql(query_statement, sql_connection)

    logging_utils.log_progress(f"run_query(): query: {query_statement}")
    logging_utils.log_progress(f"run_query(): output:\n{query_output.to_string()}\n")
