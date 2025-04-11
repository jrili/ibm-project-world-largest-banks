import requests
from bs4 import BeautifulSoup
import pandas as pd

from ..logging_utils import logging_utils

columns_upon_extraction = ["Name", "MC_USD_Billion"]

def extract(url, columns_upon_extraction):
    logging_utils.log_progress("extract(): started")
    output_df = pd.DataFrame(columns=columns_upon_extraction)

    logging_utils.log_progress(f"extract(): sending GET request to {url}")
    r = requests.get(url)
    logging_utils.log_progress(f"extract(): got response for GET request, status_code={r.status_code}")
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
            logging_utils.log_progress(f"For Task 2a: 1st row:\n{row}\n")
        else:
            output_df = pd.concat([output_df, current_df], ignore_index=True)
        row_count = row_count + 1

    print(f"Extracted data:\n{output_df.head(10)}")
    logging_utils.log_progress(f"extract(): finished")
    return output_df