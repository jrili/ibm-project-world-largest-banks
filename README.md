# Project: Acquiring and Processing Information on the World's Largest Banks
From participation in IBM's **Python Project for Data Engineering** from Coursera


|            | Links|
| ---------- | -----|
|Course Link | [IBM: Python Project for Data Engineering (Coursera)](https://www.coursera.org/learn/python-project-for-data-engineering) |
| Webscraping Target | [Wikipedia: List of Largest Banks](https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks) |
| Other Data | [exchange_rate.csv](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv) |
| Author's Course Completion Certificate|[Certificate](https://www.coursera.org/account/accomplishments/verify/TFH7N05KO7D3) |
| Author's Data Engineer Portfolio | [jrili/data-engineer-portfolio](https://github.com/jrili/data-engineer-portfolio) |

# Dataset Details
**[Wikipedia: List of Largest Banks](https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks)** contains a collated list of the largest banks in the world, as measured by market capitalization and total assets.

The ranking is based on an analysis of the bank's operations, financial performance, and overall impact on the global economy.

# Scenario
You have been hired as a data engineer by research organization. Your boss has asked you to create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD.

Further, the data needs to be transformed and stored in GBP, EUR andINR as well, in accordance with the exchange rate information that has been made available to you as a CSV file.

The information required are:

| Column Name | Description |
| ----------- | ----------- |
| `Name` | Name of the bank as listed in target website|
| `MC_USD_Billion` | Market capitalization amount in billions of **US Dollar**, as listed in target website |
| `MC_GBP_Billion` | Market capitalization amount in billions of **Great Britain Pound**, converted from `MC_USD_Billion` using the given [exchange_rate.csv](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv) |
| `MC_EUR_Billion` | Market capitalization amount in billions of **Euro**, converted from `MC_USD_Billion` using the given [exchange_rate.csv](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv) |
| `MC_INR_Billion` | Market capitalization amount in billions of **Indian Rupee**, converted from `MC_USD_Billion` using the given [exchange_rate.csv](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv) |

The processed information table is to be saved locally in a CSV format and as a database table.

Your job is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.

# Prerequisite Steps
## 1. Gather the other required data file/s
```
cd data
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
```

> [!NOTE]
> In case of unavailability, a snapshot of `exchange_rate.csv` is also available in the root directory.
> Date of snapshot: `2025 Apr 11`

## 2. Install required libraries
```
python -m pip install -r requirements.txt
```

# Project Tasks
## Task 1: Create the Logging method
Write a function `log_progress()` to log the progress of the code at different stages in a file
`code_log.txt`. Use the list of log points provided to create logentries as every stage of the code.

## Task 2: Extraction
Extract the tabular information from the given URL under the heading 'By market capitalization' and save it to a dataframe.

1. Inspect the webpage and identify the position and pattern of the tabular information in the HTML code
2. Write the code for a function `extract()` to perform the required data extraction.
3. Execute a function call to `extract()` to verify the output.

## Task 3: Transformation
Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rateinformation shared as a CSV file.
1. Write the code for a function `transform()` to perform the said task.
2. Execute a function call to `transform()` and verify the output.

## Task 4: Loading to CSV
Load the transformed dataframe to an output CSV file. Write a function `load_to_csv()`, execute a function call and verify the output.

## Task 5: Loading to DB
Load the transformed dataframe to an SQL database server as a table. Write a function `load_to_db()`, execute a function call and verify the output.

## Task 6: Validation of output in DB
Run queries on the database table. Write a function `load_to_db()`, execute a given set of queries and verify the output.

## Task 7: Validation of output log
Verify that the log entries have been completed at all stages by checking the contents of the file
`code_log.txt`.

# How to execute:
TODO
<!-- _(Tested in Python 3.13)_
```
python etl_practice.py
```
_Also available with sample outputs and explanations in notebook: [etl_car_dealership.ipynb](https://github.com/jrili/ibm-etl-car-dealership/blob/master/etl_car_dealership.ipynb)_ -->


# Acknowledgements
## Course Instructors
- Ramesh Sannareddy
- Joseph Santarcangelo
- Abhishek Gagneja
## Course Offered By
* [IBM Skills Network](https://www.coursera.org/partners/ibm-skills-network)
