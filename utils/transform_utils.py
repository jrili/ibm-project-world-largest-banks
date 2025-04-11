import pandas as pd
import numpy as np

from . import logging_utils

"""List of string column names expected after transform process"""
OUTPUT_COLUMNS = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]

def get_exchange_rate_df(path_to_exchange_rate_csv):
    """Return a dataframe containing exchange rate information from `path_to_exchange_rate_csv`"""
    logging_utils.log_progress("get_exchange_rate_df(): started")
    try:
        exchange_rate_df = pd.read_csv(path_to_exchange_rate_csv, index_col="Currency")
        exchange_rate_df.loc[:, "Rate"] = exchange_rate_df.loc[:,"Rate"].astype(float)
    except FileNotFoundError as e:
        logging_utils.log_progress(f"ERROR: Cannot find exchange rate CSV file: '{path_to_exchange_rate_csv}'.")

    logging_utils.log_progress(f"get_exchange_rate_df(): finished, got exchange rates:\n{exchange_rate_df.head()}\n")
    return exchange_rate_df


def convert_usd(tgt_currency,  src_usd_series, exchange_rate_df, round_decimals=2):
    """Returns converted and rounded values from USD to `tgt_currency` using exchange rate from `exchange_rate_df`"""
    try:
        converted_series = src_usd_series * exchange_rate_df.loc[tgt_currency]['Rate']
        converted_series = np.round(converted_series, round_decimals)
    except IndexError as e:
        logging_utils.log_progress(f"ERROR: '{tgt_currency}' not found in exchange rate file provided. Supported currencies are: {exchange_rate_df['Currency']}")

    return converted_series
    

def transform(df, path_to_exchange_rate_csv):
    logging_utils.log_progress("transform(): started")
    transformed_df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Copy Name and MC_USD_Billion columns
    transformed_df["Name"] = df.loc[:, "Name"].copy()
    transformed_df["MC_USD_Billion"] = df.loc[:, "MC_USD_Billion"].copy()
    logging_utils.log_progress(f"transform(): transformed dataframe head:\n{transformed_df.head()}")

    # Read exchange rate info from `path_to_exchange_rate_csv`
    exchange_rate_df = get_exchange_rate_df(path_to_exchange_rate_csv)

    # Compute the GBP, EUR, and INR column values based from the USD column values
    transformed_df["MC_GBP_Billion"] = convert_usd('GBP',
                                                   src_usd_series=df.loc[:, "MC_USD_Billion"],
                                                   exchange_rate_df=exchange_rate_df)
    transformed_df["MC_EUR_Billion"] = convert_usd('EUR',
                                                   src_usd_series=df.loc[:, "MC_USD_Billion"],
                                                   exchange_rate_df=exchange_rate_df)
    transformed_df["MC_INR_Billion"] = convert_usd('INR',
                                                   src_usd_series=df.loc[:, "MC_USD_Billion"],
                                                   exchange_rate_df=exchange_rate_df)

    logging_utils.log_progress(f"transform(): transformed dataframe head:\n{transformed_df.head()}\n")
    logging_utils.log_progress(f"transform(): finished")
    return transformed_df