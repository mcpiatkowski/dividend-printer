import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf


def get_raw_stock_data() -> pd.DataFrame:
    # TODO Should be parametrize by ticker, end_date, start_date
    ticker_list: list[str] = ['AAPL', 'CAT', 'CSCO', 'FDX', 'JNJ', 'KO', 'MSFT', 'NVDA', 'ORCL', 'PEP', 'V', 'WMT']
    end_date: datetime = datetime.today()
    start_date: str = (end_date - timedelta(days=40 * 365)).strftime('%Y-%m-%d')
    return yf.download(ticker_list, start=start_date, end=end_date.strftime('%Y-%m-%d'))


def transform_raw_stock_data(raw: pd.DataFrame) -> pd.DataFrame:
    transformed = raw.stack(level=1).reset_index().rename(columns={'level_1': 'TICKER'})
    transformed.columns.name = None
    transformed.columns = transformed.columns.str.replace(' ', '_').str.upper()
    transformed = transformed[["TICKER"] + [col for col in transformed if col != "TICKER"]]
    return transformed


def insert_into_db(transformed: pd.DataFrame) -> None:
    con = sqlite3.connect('market.sqlite')  # This will create a database named 'market.sqlite'.
    transformed.to_sql('stock', con, if_exists='replace', index=False)  # This will create table stock.
    con.close()


if __name__ == '__main__':
    raw_stock: pd.DataFrame = get_raw_stock_data()
    transformed: pd.DataFrame = transform_raw_stock_data(raw_stock)
    # Save copy.
    transformed.to_parquet('stock.parquet')
    insert_into_db(transformed)
