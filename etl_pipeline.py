from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from datetime import datetime
from typing import Tuple
from models import StockMetadata
from decimal import Decimal, ROUND_HALF_UP
import numpy as np
from utils import preprocess_stock_list, preprocess_stock_prices, exec_time
import json
import logging

# setup logger
logging.basicConfig(filename='pipeline.log',
                    filemode='a',
                    format='%(asctime)s | %(levelname)-8s | %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)

# load configuration data
with open('configs.json', 'r') as f:
    configs = json.load(f)

db_configs = configs['DB']
base_dir = configs['FILES_PATH']

# Connect to the database
user = db_configs['user']
password = db_configs['password']
server = db_configs['server']
dbname = db_configs['database']

# production case
#connection_url = f"mysql+pymysql://{user}:{password}@{server}/{dbname}"  # can also be configurable for better automation

# test purposes
connection_url = f"sqlite:///{dbname}.db"

engine = create_engine(connection_url)
Session = sessionmaker(bind=engine)


@exec_time
def extract() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
        Extract Stocks data (local filesystem for that task, can be a RestApi or an ftp server in real case scenarios etc.)

        Parameters:
        -----------
        None

        Returns:
        --------
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
            A Tuple where each element is a different kind of Stock data
    """

    logger.info('Start extraction of Stocks data')
	
    # Define filepaths
    primary_stock_prices_path = f'{base_dir}/stock_prices.csv'
    secondary_stock_prices_path = f'{base_dir}/secondary_stock_prices.csv'
    stock_list_path = f'{base_dir}/stock_list.csv'

    # Define specific columns to load
    keep_columns_stock_prices = ['Date', 'SecuritiesCode', 'Open', 'High', 'Low', 'Close',
                                 'Volume', 'AdjustmentFactor', 'ExpectedDividend', 'SupervisionFlag']

    keep_columns_stock_metadata = ['SecuritiesCode', 'Name', 'Section/Products',
                                   'NewMarketSegment', '33SectorName',
                                   '17SectorName', 'NewIndexSeriesSize', 'IssuedShares']
		
    # Read data		
    primary_stocks = pd.read_csv(primary_stock_prices_path, usecols=keep_columns_stock_prices, parse_dates=['Date'],
                                 dtype={'Volume': np.float128})
    secondary_stocks = pd.read_csv(secondary_stock_prices_path, usecols=keep_columns_stock_prices,
                                   parse_dates=['Date'], dtype={'Volume': np.float128})
    stocks_metadata = pd.read_csv(stock_list_path, usecols=keep_columns_stock_metadata,
                                  dtype={'IssuedShares': np.float128})

    logger.info('Successfully extracted Stocks data')

    return primary_stocks, secondary_stocks, stocks_metadata


@exec_time
def transform(extracted_data: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
        Transform Stocks raw data

        Parameters:
        -----------
        extracted_data: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
            A Tuple where each element is a different kind of Stock data

        Returns:
        --------
        Tuple[pd.DataFrame, pd.DataFrame]
            A Tuple where each element is a different kind of transformed Stock data
    """

    logger.info('Start transformation of Stocks data')
    
    primary_stocks, secondary_stocks, stocks_metadata = extracted_data[0], extracted_data[1], extracted_data[2]
	
    # Preproccess stocks raw data
    primary_stocks = preprocess_stock_prices(primary_stocks, IsPrimary=True)
    secondary_stocks = preprocess_stock_prices(secondary_stocks, IsPrimary=False)
    stocks_metadata = preprocess_stock_list(stocks_metadata)

    try:
        # check if there are new stocks in stocks prices but are not present in stocks metadata
        stock_codes_today_from_list = stocks_metadata.SecuritiesCode.values.tolist()
        stocks_codes_today_from_prices = primary_stocks.SecuritiesCode.values.tolist() + secondary_stocks.SecuritiesCode.values.tolist()
        diff = set(stocks_codes_today_from_prices) - set(stock_codes_today_from_list)

        if len(diff) > 0:
            logger.error('Stock codes found in Stock prices that are not present in Stocks Metadata')
            raise Exception("Stock codes found in Stock prices that are not present in Stocks Metadata")

        # Concatenate stock prices
        logger.info("Concatenate Primary and Secondary Stock Prices data")
        all_stock_prices = pd.concat([primary_stocks, secondary_stocks], axis=0)

        # Add IsPrimary column
        logger.info("Add IsPrimary column")
        all_stock_prices['IsPrimary'] = len(primary_stocks) * [True] + len(secondary_stocks) * [False]

        # sort dataframe
        all_stock_prices.sort_values(by=['Date', 'SecuritiesCode'], inplace=True)
        all_stock_prices.reset_index(drop=True, inplace=True)

        # Define which columns to keep
        columns_to_keep = all_stock_prices.columns.values.tolist() + ['MarketCapitalization']

        # find market capitalization per (Date, stock)
        logger.info("Calculate Market Capitalization per (Date, Stock)")
        all_stock_prices = all_stock_prices.merge(stocks_metadata, on='SecuritiesCode', how='inner')
        all_stock_prices['MarketCapitalization'] = (all_stock_prices['Close'] * all_stock_prices['IssuedShares']).map(
            lambda x: float(
                Decimal(str(x)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            ))

        # Select only necessary columns
        all_stock_prices = all_stock_prices[columns_to_keep]

        # Add creation and update date time of the record
        time_now = datetime.utcnow()
        all_stock_prices['CreatedDateTime'] = time_now
        all_stock_prices['UpdatedDateTime'] = time_now

        # check if new stock codes have arrived today
        logger.info("Check if new Stocks codes have arrived today")

        # Query database to fetch all existing Stock Codes
        session = Session()
        existing_stocks = session.query(StockMetadata.SecuritiesCode).all()
        stock_codes_existing = [code[0] for code in existing_stocks]

        # Find newly arrived Stock codes
        stock_codes_today = stocks_metadata['SecuritiesCode'].values.tolist()
        new_stock_codes = list(set(stock_codes_today) - set(stock_codes_existing))

        # add new codes (if any)
        stocks_metadata = stocks_metadata[stocks_metadata.SecuritiesCode.isin(new_stock_codes)]

        if len(stocks_metadata) > 0:
            logger.info('New Stock codes have arrived')
            stocks_metadata['CreatedDateTime'] = time_now
            stocks_metadata['UpdatedDateTime'] = time_now
        else:
            logger.info('None new Stock codes has arrived')

    except Exception as excp:
        logger.error(f'Transformation of Stocks data failed. Error: {excp}')
        raise Exception('Transformation of Stocks data failed')

    logger.info('Successfully transformed Stocks data')

    return all_stock_prices, stocks_metadata.drop(columns=['IssuedShares'])


@exec_time
def load(transformed_data: Tuple[pd.DataFrame, pd.DataFrame]) -> None:
    """
       Load transformed data to destination system

       Parameters:
       -----------
       transformed_data: Tuple[pd.DataFrame, pd.DataFrame]
           Transformed df of different kind of Stocks

       Returns:
       --------
       None
    """

    all_stock_prices, stocks_metadata = transformed_data[0], transformed_data[1]

    logger.info('Start load of Stocks data to db')
    try:
        # if new stock codes have encountered, write them to db
        if len(stocks_metadata) > 0:
            logger.info('Load new Stock codes to db')
            stocks_metadata.to_sql('StockMetadata', con=engine, index=False, if_exists='append')

        # write stock prices to db
        logger.info('Load Stock prices to db')
        all_stock_prices.to_sql('StockPrices', con=engine, index=False, if_exists='append')

    except Exception as excp:
        logger.error(f'Load of Stocks data to db failed. Error: {excp}')
        raise Exception('Load of Stocks data to db failed')

    logger.info('Successfully loaded Stocks data to db')


@exec_time
def etl_pipeline() -> None:
    """
    Extract, transform and load pipeline

    Parameters:
    -----------
    None

    Returns:
    --------
    None
    """

    try:
        logger.info("Start ETL pipeline")
        # extract
        extracted_data = extract()
        # transform
        transformed_data = transform(extracted_data)
        # load
        load(transformed_data)
        logger.info("Successful completion of ETL pipeline")
    except Exception as excp:
        logger.error('ETL pipeline failed')
        raise Exception(excp)


if __name__ == "__main__":
    etl_pipeline()
