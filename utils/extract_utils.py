import requests
from bs4 import BeautifulSoup
import pandas as pd

from . import logging_utils

def extract(url):
    """Performs the transform process for the needed bank data

    Keyword Arguments:
    url -- target url to be scraped

    """
    logging_utils.log_progress("extract(): started")

    logging_utils.log_progress(f"extract(): sending GET request to {url}")
    r = requests.get(url)
    logging_utils.log_progress(f"extract(): got response for GET request, status_code={r.status_code}")
    html_page = r.text
    data = BeautifulSoup(html_page, "html.parser")
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    # Initialize list to hold dicts (1 dict = 1 row)
    output_rows_list = []

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

        # Create data dict from extracted data, then append to list of rows
        data_dict = {
                    "Name":bank_name,
                     "MC_USD_Billion":marketcap
                    }

        output_rows_list.append(data_dict)
        row_count = row_count + 1

    output_df = pd.DataFrame.from_dict(output_rows_list)

    logging_utils.log_progress(f"extract(): extracted datarame head:\n{output_df.head()}\n")
    logging_utils.log_progress(f"extract(): finished")
    return output_df
