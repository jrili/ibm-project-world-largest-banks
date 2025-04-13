import pandas as pd

from . import logging_utils

def run_query(query_statement, sql_connection):
    """Run an SQL query on a DB

    Keyword Arguments:
    query_statement -- string containing query statement
    sql_connection -- SQL connection object to target DB

    Return:
    Pandas Dataframe containing query result
    """
    logging_utils.log_progress(f"run_query(): started, query='{query_statement}'")
    query_output = pd.read_sql(query_statement, sql_connection)

    logging_utils.log_progress(f"run_query(): output:\n{query_output.to_string()}\n")

    return query_output
