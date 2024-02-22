"""Allocation table management."""

import os
import sqlite3
import requests
import pandas as pd


def get_portfolio_allocation(api_key: str) -> pd.DataFrame:
    """Get current Trading212 portfolio."""
    url = "https://live.trading212.com/api/v0/equity/portfolio"
    headers = {"Authorization": api_key}
    response = requests.get(url, headers=headers)
    return pd.DataFrame(response.json())


def select_pie_only(portfolio: pd.DataFrame) -> pd.DataFrame:
    """Select stocks that are part of a diagram."""
    return portfolio[portfolio.pieQuantity > 0].reset_index(drop=True)


def create_symbol(portfolio: pd.DataFrame) -> pd.DataFrame:
    """Map Trading212 ticker to FMP symbol."""
    return portfolio.assign(
        SYMBOL=portfolio["ticker"]
        .str.split("_")
        .str[0]
        .replace({"HCP1": "HCP", "DMYQ": "PL"})
    )


def get_columns(portfolio: pd.DataFrame) -> pd.DataFrame:
    """Select columns needed in the database."""
    return portfolio[["SYMBOL", "quantity", "averagePrice"]].reset_index(drop=True)


def create_allocation_table() -> None:
    """Create and replace allocation table."""
    conn = sqlite3.connect("dividend_printer.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS allocation")
    c.execute(
        """
    CREATE TABLE allocation (
        ID INTEGER PRIMARY KEY,
        RECORD_DATE TEXT,
        SYMBOL TEXT,
        QUANTITY REAL,
        AVERAGE_PRICE REAL
    )
    """
    )
    conn.commit()
    conn.close()


def create_allocation_dataframe() -> pd.DataFrame:
    """Create allocation dataframe."""
    allocation: pd.DataFrame = (
        get_portfolio_allocation(os.getenv("TRADING_API_KEY"))
        .pipe(select_pie_only)
        .pipe(create_symbol)
        .pipe(get_columns)
        .rename(columns={"averagePrice": "AVERAGE_PRICE"})
        .assign(RECORD_DATE=pd.to_datetime("now").strftime("%Y-%m-%d"))
    )
    allocation.columns = allocation.columns.str.upper()
    return allocation


def insert_into_allocation_table(allocation: pd.DataFrame) -> None:
    """Insert into the allocation table."""
    conn = sqlite3.connect("dividend_printer.db")
    allocation.to_sql(name="allocation", con=conn, if_exists="append", index=False)
    conn.commit()
    conn.close()


def update_allocation_table() -> None:
    """Update the allocation table."""
    create_allocation_table()
    allocation = create_allocation_dataframe()
    insert_into_allocation_table(allocation)
