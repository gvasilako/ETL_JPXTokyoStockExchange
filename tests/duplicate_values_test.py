from load_datasets import stock_prices, stock_metadata

def duplicate_rows(data_df):
    n_duplicates_rows = data_df.duplicated().sum()
    return n_duplicates_rows


def test_duplicates_rows_stock_prices(stock_prices):
    assert len(stock_prices) > 0, 'Stock Prices table is empty'
    assert duplicate_rows(stock_prices.drop(columns=['CreatedDateTime', 'UpdatedDateTime'])) == 0, "Stock Prices table has duplicate rows"


def test_duplicates_rows_stock_metadata(stock_metadata):
    assert len(stock_metadata) > 0, 'Stock Metadata table is empty'
    assert duplicate_rows(stock_metadata.drop(columns=['CreatedDateTime', 'UpdatedDateTime'])) == 0, "Stock Meatadata table has duplicate rows"

