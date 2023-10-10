from load_datasets import stock_prices, stock_metadata


def missing_values_column(data_df):
    missing_values = data_df.isna().sum().values
    test = [i == 0 for i in missing_values]
    results = {key: value for key, value in zip(data_df.columns, test)}

    return test, results


def test_missing_values_stock_prices(stock_prices):
    assert len(stock_prices) > 0, 'Stock Prices table is empty'
    test, results = missing_values_column(stock_prices)
    print("Columns with missing values have flag False")
    print(results)
    assert all(test)


def test_missing_values_stock_metadata(stock_metadata):
    assert len(stock_metadata) > 0, 'Stock metadata table is empty'
    test, results = missing_values_column(stock_metadata)
    print("Columns with missing values have flag False")
    print(results)
    assert all(test)
