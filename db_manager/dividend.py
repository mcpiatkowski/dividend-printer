"""Dividend table management."""

import sqlite3
import requests
import pandas as pd
from collections import namedtuple

DividendData = namedtuple(
    typename="DividendData", field_names=["data", "ticker", "available"]
)


def create_dividend_table() -> None:
    """Create and replace dividend table."""
    conn = sqlite3.connect("dividend_printer.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS dividend")
    c.execute(
        """
    CREATE TABLE dividend (
        ID INTEGER PRIMARY KEY,
        RECORD_DATE TEXT,
        ADJ_DIVIDEND REAL,
        DIVIDEND REAL,
        DIVIDEND_RECORD_DATE TEXT,
        PAYMENT_DATE TEXT,
        DECLARATION_DATE TEXT,
        SYMBOL TEXT
    )
    """
    )
    conn.commit()
    conn.close()


def get_dividend_history(ticker: str) -> DividendData:
    """Get dividend history."""
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/{ticker}?apikey=c7be3bdfd7df35380203e081623718cf"
    response = requests.get(url)
    data = response.json()
    return DividendData(data=data, ticker=ticker, available=bool(data["historical"]))


def transform_(_dividends: DividendData) -> pd.DataFrame:
    """Prepare dividends to insert into table."""
    return (
        pd.DataFrame(_dividends.data["historical"])
        .sort_values(by="recordDate", ascending=False)
        .reset_index(drop=True)
        .drop(columns="label")
        .assign(ticker=_dividends.ticker)
        .rename(
            columns={
                "date": "record_date",
                "adjDividend": "adj_dividend",
                "recordDate": "dividend_record_date",
                "paymentDate": "payment_date",
                "declarationDate": "declaration_date",
                "ticker": "symbol",
            }
        )
    )


def insert_into_dividend_table(dividend: pd.DataFrame) -> None:
    """Insert into dividend table."""
    conn = sqlite3.connect("dividend_printer.db")
    dividend.to_sql(name="dividend", con=conn, if_exists="append", index=False)
    conn.commit()
    conn.close()


def update_dividends(stocks: pd.DataFrame) -> None:
    """Update dividends."""
    for stock in stocks.SYMBOL:
        print(stock)
        dividends = get_dividend_history(stock)
        if dividends.available and dividends.ticker not in ["PL", "HCP"]:  # API bug
            dividend_data: pd.DataFrame = transform_(dividends)
            dividend_data.columns = dividend_data.columns.str.upper()
            insert_into_dividend_table(dividend_data)
        else:
            print(f"Stock {stock} pays no dividend.")
