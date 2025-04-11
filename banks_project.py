import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url="https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"

exchange_rate_csv="./exchange_rate.csv"
exchange_rate_df = pd.DataFrame()

columns_upon_extraction = ["Name", "MC_USD_Billion"]
output_columns = columns_upon_extraction.extend(["MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"])

output_csv_path = "./Largest_banks_data.csv"
output_db_name = "Banks.db"
output_table_name = "Largest_banks"

log_file = "code_log.txt"

############ Logging Methods ############
def log_progress(msg):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp_str = now.strftime(timestamp_format)

    with open(log_file, "a") as f:
        log_str = timestamp_str + ":" + msg + "\n"
        f.write(log_str)


############ Extract Methods ############
def extract():
    log_progress("extract(): started")
    output_df = pd.DataFrame(columns=columns_upon_extraction)

    log_progress(f"extract(): sending GET request to {url}")
    r = requests.get(url)
    log_progress(f"extract(): got response for GET request, status_code={r.status_code}")
    html_page = r.text
    data = BeautifulSoup(html_page, "html.parser")
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    row_count = 0
    for row in rows:
        # Skip empty rows
        row_data = row.find_all('td')
        if len(row_data) < 3:
            continue

        # Skip rows without bank name links
        bank_links = row_data[1].find_all('a')
        if len(bank_links) < 2:
            continue

        bank_name = bank_links[1].string
        marketcap = float(row_data[2].text)

        current_df = pd.DataFrame({"Name":bank_name, "MC_USD_Billion":marketcap},
                                index=[0])

        if output_df.empty:
            output_df = current_df.copy()
            # For Task 2a: print HTML content of 1st row
            print(f"For Task 2a: 1st row:{row}")
        else:
            output_df = pd.concat([output_df, current_df], ignore_index=True)
        row_count = row_count + 1

    print(f"Extracted data:\n{output_df.head(10)}")
    log_progress(f"extract(): finished")
    return output_df



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
        exchange_rate_df = pd.read_csv(exchange_rate_csv, index_col="Currency")
        exchange_rate_df.loc[:, "Rate"] = exchange_rate_df.loc[:,"Rate"].astype(float)

    exchange_rate = exchange_rate_df.loc[target_currency]["Rate"]

    #log_progress(f"convert_currency(): finished")
    return exchange_rate

def transform(df):
    log_progress("transform(): started")
    transformed_df = pd.DataFrame(columns=output_columns)

    # Copy Name and MC_USD_Billion coDlumns
    transformed_df.loc[:, "Name"] = df.loc[:, "Name"].copy()
    transformed_df.loc[:, "MC_USD_Billion"] = df.loc[:, "MC_USD_Billion"].copy()

    # Compute the GBP, EUR, and INR column values based from the USD column values
    transformed_df.loc[:, "MC_GBP_Billion"] = np.round(df.loc[:, "MC_USD_Billion"].copy() * convert_currency('GBP'), 2)
    transformed_df.loc[:, "MC_EUR_Billion"] = np.round(df.loc[:, "MC_USD_Billion"].copy() * convert_currency('EUR'), 2)
    transformed_df.loc[:, "MC_INR_Billion"] = np.round(df.loc[:, "MC_USD_Billion"].copy() * convert_currency('INR'), 2)
    
    print(f"transform(): Transformed df:\n{transformed_df.head(10)}")
    print(f"For quiz: transformed_df['MC_EUR_Billion'][4]={transformed_df['MC_EUR_Billion'][4]}")
    log_progress(f"transform(): finished")
    return transformed_df


############ Load Methods ############
def load_to_csv(output_filename, transformed_df):
    log_progress("load_to_csv(): started")
    transformed_df.to_csv(output_filename)
    log_progress(f"Data saved to CSV file '{output_filename}'")

def load_to_db(sql_connection, output_table_name, transformed_df):
    log_progress("load_to_db(): started")
    transformed_df.to_sql(output_table_name, sql_connection, if_exists='replace', index='False')
    log_progress(f"Data loaded to Database as a table, Executing queries'")

def load(transformed_df, sql_connection):
    log_progress("load(): started")
    load_to_csv(output_csv_path, transformed_df)
    load_to_db(sql_connection, output_table_name, transformed_df)
    log_progress("load(): finished")


############ DB Methods ############
def run_query(query_statement, sql_connection):
    log_progress(f"run_query(): started, query='{query_statement}'")
    query_output = pd.read_sql(query_statement, sql_connection)

    print(f"run_query(): query: {query_statement}")
    print(f"run_query(): output:\n{query_output.to_string()}")
    log_progress(f"Process Complete")


############ Main Process ############
log_progress("Preliminaries complete. Initiating ETL process")

output_df = extract()

log_progress("Data extraction complete. Initiating Transformation process")
output_df = transform(output_df)

log_progress("Data transformation complete. Initiating Loading process")
sql_connection = sqlite3.connect(output_db_name)
log_progress(f"SQL Connection initiated, '{output_db_name}'")
load(output_df, sql_connection)

run_query(f"SELECT * FROM {output_table_name}", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {output_table_name}", sql_connection)
run_query(f"SELECT Name FROM {output_table_name} LIMIT 5", sql_connection)

sql_connection.close()
log_progress("SQL Connection closed")
log_progress("ETL process finished\n")