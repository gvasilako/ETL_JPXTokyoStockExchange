import pandas as pd
import logging
from functools import wraps
import time

logger = logging.getLogger(__name__)


def preprocess_stock_list(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter, clean and transform the Stock Metadata.

    Parameters
    ----------
    data_df: pd.DataFrame
        Raw Data of Stock Metadata

    Returns
    -------
    pd.DataFrame
        Preprocessed Data
    """

    logger.info('Start preprocessing of Stocks Metadata')
    try:

        # drop rows without SecurityCode
        data_df.dropna(subset=['SecuritiesCode'], inplace=True)

        # drop duplicate rows
        data_df.drop_duplicates(inplace=True)

        # rename columns
        data_df.rename(columns={'Section/Products': 'Section',
                                '33SectorName': 'SectorName33',
                                '17SectorName': 'SectorName17',
                                }, inplace=True)

        # Strip values, replace empty strings and "-" with None
        categorical_columns = data_df.columns.values[data_df.dtypes == 'object']
        data_df[categorical_columns] = data_df[categorical_columns].apply(
            lambda x: x.str.strip().replace(r'^\s*$|[-]', None, regex=True))

    except Exception as excp:
        logger.error(f'Preprocessing of Stocks Metadata failed. Error: {excp}')
        raise Exception('Preprocessing of Stocks Metadata failed')

    logger.info(f'Successfully preprocessed Stocks Metadata')

    return data_df


def preprocess_stock_prices(data_df: pd.DataFrame, IsPrimary: bool = None) -> pd.DataFrame:
    """
    Filter, clean and transform the Stock Prices data.

    Parameters
    ----------
    data_df: pd.DataFrame
        Raw data of stock prices
    IsPrimary: boolean
        Flag value if the data are primary or secondary stocks

    Returns
    -------
    pd.DataFrame
        Preprocessed Data
    """

    kind = 'Primary' if IsPrimary else 'Secondary'

    logger.info(f'Start preprocessing of {kind} Stocks data')

    try:
        # drop rows without SecurityCode or Date
        data_df.dropna(subset=['SecuritiesCode', 'Date'], inplace=True)

        # drop duplicate rows
        data_df.drop_duplicates(inplace=True)
    except Exception as excp:
        logger.error(f'Preprocessing of {kind} Stocks data failed. Error: {excp}')
        raise Exception(f'Preprocessing of {kind} Stocks data failed')

    logger.info(f'Successfully preprocessed {kind} Stocks data')

    return data_df


def exec_time(func):
    """
        Decorator that measures the execution time of the input function.
    """

    @wraps(func)
    def exec_time_inner(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.info(f'Function {func.__name__} Took {total_time:.2f} seconds')
        return result

    return exec_time_inner
