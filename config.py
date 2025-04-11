WEBSCRAPING_TARGET_URL="https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"

PATH_TO_EXCHANGE_RATE_CSV="data/exchange_rate.csv"
PATH_TO_OUTPUT_CSV = "output/Largest_banks_data.csv"
PATH_TO_OUTPUT_DB = "output/Banks.db"
OUTPUT_TABLE_NAME = "Largest_banks"

PATH_TO_LOGFILE = "code_log.txt"

COLUMNS_UPON_EXTRACTION = ["Name", "MC_USD_Billion"]
OUTPUT_COLUMNS = COLUMNS_UPON_EXTRACTION.extend(["MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"])
