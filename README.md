# ETL_JPXTokyoStockExchange
The goal of this project is to create an Extract, Transform, Load (ETL) pipeline on the JPX Tolyo Stock Exchange Dataset which are available at Kaggle [here](https://www.kaggle.com/competitions/jpx-tokyo-stock-exchange-prediction/overview).


-------------------------------------------------------------------------------------------------
Steps required to run the code.

Prerequisites: Installed Python >= 3.8

From a terminal:
1. change directory to ETL_JPXTokyoStockExchange: cd ETL_JPXTokyoStockExchange
2. install libraries: pip install -r requirements.txt
3. To create the in memory Database, type: python create_database.py
4. To Run the pipeline, type: python etl_pipeline.py
5. To Run the test, type: pytest tests/ --no-header -v > test_output.log

After the execution of the pipeline, a pipeline.log file will be produced which logs the steps of the pipeline.
After the execution of the tests, a test_output.log file will be produced which logs the test results.


-------------------------------------------------------------------------------------------------
Modeling Assumptions:

- We assume that every day (or every week etc.) we receive new stock prices datasets (for both primary and secondary stocks).
  The primary and secondary stocks datasets are therefore dynamic data.

- We assume that we receive as well the stock list data every day or so which contains metadata about the stocks. We do not expect a lot of changes on
  those data from date to date. Therefore the stock list data are static in general.

- The stock list data (SecuritiesCode as primary key) has an one-to-many relationship with the stock prices data (SecuritiesCode as foreign key)


-------------------------------------------------------------------------------------------------
Data Transformation Assumptions:

- We attempt to save the received data without performing complicated transformation, i.e almost raw data.
- More complicated transformation can be applied to an ad-hoc analysis, e.g fill missing values with a specific strategy
  which takes into account all the past values of a specific stock saved in the database etc.


-------------------------------------------------------------------------------------------------
Datasets Used
 - example_test_files/stock_prices.csv
 - example_test_files/secondary_stock_prices.csv
 - stock_list.csv

 
------------------------------------------------------------------------------------------------- 
Next Steps

 - If we want to automate, schedule, monitor and raising alerts for the pipeline execution we can use a pipeline orchestrator tool like Prefect: https://docs.prefect.io/latest/
 - Only minimal changes required
