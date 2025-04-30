Acquiring and Processing Information on the World's Largest Banks
===================================================================
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Jupyter Notebook](https://img.shields.io/badge/jupyter-%23FA0F00.svg?style=for-the-badge&logo=jupyter&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)![SQLite](https://img.shields.io/badge/sqlite-%2307405e.svg?style=for-the-badge&logo=sqlite&logoColor=white) 
[ETL]

***Part of a Data Engineer Portfolio: [jrili/data-engineer-portfolio](https://github.com/jrili/data-engineer-portfolio)***


# Project Description
This project demonstrates a comprehensive ETL pipeline that extracts and transforms data from the public domain into a structured, analysis-ready output file and database table. The objective is to automate the extraction and processing of data related to the world's largest banks by market capitalization.

# Project Objectives
* Scrape data from a preserved version of the Wikipedia page listing the largest banks by market capitalization ([Wikipedia: List of Largest Banks](https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks))
* Transform the market capitalization figures from one currency to others using provided exchange rates
* Perform tests at each stage to verify correctness of data processing
* Export clean datasets into a CSV file for easy accessibility and sharing, and into an SQLite database for structured storage and querying
* Implement a logging mechanism to track the progress and stages of the ETL process

# Tools & Technologies Used
* Python 3.13
* Jupyter Notebook
* Pandas
* SQLite3

# Specifications
You have been hired as a data engineer by research organization. Your boss has asked you to create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD.

Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available to you as a CSV file.

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

# Workflow Overview
1. Scrape the data from a preserved version of the Wikipedia page listing the largest banks by market capitalization ([Wikipedia: List of Largest Banks](https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks))
2. Convert the market capitalization figures from USD to GBP, EUR, and INR using provided exchange rates, and round the results to two decimal places
3. Load the transformed data into two output formats:
    * A CSV file (`Largest_banks_data.csv`)
    * An SQLite datbase (`Banks.db`)
4. Implement a logging mechanism to track the progress at each stage of the workflow

# How to execute script:
## Prerequisites
### 1. Gather the exchange rates file
```
cd data
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv
```

> [!NOTE]
> In case of unavailability, a snapshot of `exchange_rate.csv` is also available in the root directory.
> Date of snapshot: `2025 Apr 11`

### 2. Install required libraries
```
python -m pip install -r requirements.txt
```

## Execution Steps
_(Tested in Python 3.13)_
```
python banks_project.py
```
_Also available with sample outputs and explanations in notebook: [banks_project.ipynb](https://github.com/jrili/ibm-project-world-largest-banks/blob/master/banks_project.ipynb)_

# Key Learning Points
* Data extraction and validation using Pandas and BeautifulSoup4
* Data transformation techniques for handling financial data
* Handling multiple output files to simulate loading into different targets

# Future Improvements
* Modify scraping target from preserved version to the live version
* Implement pytest scripts for validation at each step
* Schedule ETL runs using Apache Airflow
* Load outputs directly to a relational database (PostgreSQL/MySQL)

# Acknowledgements
## Source Course
* [IBM: Python Project for Data Engineering (Coursera)](https://www.coursera.org/learn/python-project-for-data-engineering)
* Course Instructors:
    * Ramesh Sannareddy
    * Joseph Santarcangelo
    * Abhishek Gagneja
