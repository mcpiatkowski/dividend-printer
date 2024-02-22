"""Info table management."""

import sqlite3
import requests
import pandas as pd


INFO_COLUMNS: list[str] = [
    "symbol",
    "companyName",
    "sector",
    "industry",
    "currency",
    "exchange",
    "exchangeShortName",
    "website",
    "country",
    "description",
    "image",
]


def create_info_table() -> None:
    """Create info table."""
    conn = sqlite3.connect("dividend_printer.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS info")
    c.execute(
        """
    CREATE TABLE info (
        ID INTEGER PRIMARY KEY,
        SYMBOL TEXT,
        COMPANY_NAME TEXT,
        SECTOR TEXT,
        INDUSTRY TEXT,
        CURRENCY TEXT,
        EXCHANGE TEXT,
        EXCHANGE_SHORT_NAME TEXT,
        WEBSITE TEXT,
        COUNTRY TEXT,
        DESCRIPTION TEXT,
        IMAGE TEXT
    )
    """
    )
    conn.commit()
    conn.close()


def get_stock_info(ticker: str) -> pd.DataFrame:
    """Get stock information."""
    response = requests.get(
        f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey=c7be3bdfd7df35380203e081623718cf"
    )
    info = pd.DataFrame(response.json())
    return info


def insert_into_info_table(info: pd.DataFrame) -> None:
    """Insert into the info table."""
    conn = sqlite3.connect("dividend_printer.db")
    info.to_sql(name="info", con=conn, if_exists="append", index=False)
    conn.commit()
    conn.close()


def update_info(stocks: pd.DataFrame) -> None:
    """Update info."""
    for stock in stocks.SYMBOL:
        print(stock)
        info = get_stock_info(stock)[INFO_COLUMNS].rename(
            columns={
                "companyName": "company_name",
                "exchangeShortName": "exchange_short_name",
            }
        )
        info.columns = info.columns.str.upper()
        insert_into_info_table(info)
