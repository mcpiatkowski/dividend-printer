"""Income statement table management."""

import sqlite3
import requests
import pandas as pd


def create_income_statement_table() -> None:
    """Create income statement table."""
    conn = sqlite3.connect("dividend_printer.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS income_statement")
    c.execute(
        """
    CREATE TABLE income_statement (
        ID INTEGER PRIMARY KEY,
        RECORD_DATE TEXT,
        SYMBOL TEXT,
        REPORTED_CURRENCY TEXT,
        FILLING_DATE TEXT,
        CALENDAR_YEAR TEXT,
        PERIOD TEXT,
        REVENUE INTEGER,
        COST_OF_REVENUE INTEGER,
        GROSS_PROFIT INTEGER,
        GROSS_PROFIT_RATIO REAL
    )
    """
    )
    conn.commit()
    conn.close()


def get_quarterly_income_statement(ticker: str) -> pd.DataFrame:
    """Get quarterly income statement information."""
    response = requests.get(
        f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=quarter&apikey=c7be3bdfd7df35380203e081623718cf"
    )
    income_statement = pd.DataFrame(response.json())
    return (
        income_statement[
            [
                "date",
                "symbol",
                "reportedCurrency",
                "fillingDate",
                "calendarYear",
                "period",
                "revenue",
                "costOfRevenue",
                "grossProfit",
                "grossProfitRatio",
            ]
        ]
        .rename(
            columns={
                "date": "record_date",
                "reportedCurrency": "reported_currency",
                "fillingDate": "filling_date",
                "calendarYear": "calendar_year",
                "costOfRevenue": "cost_of_revenue",
                "grossProfit": "gross_profit",
                "grossProfitRatio": "gross_profit_ratio",
            }
        )
        .reset_index(drop=True)
    )


def insert_into_income_statement_table(income_statement: pd.DataFrame) -> None:
    """Insert into the income_statement table."""
    conn = sqlite3.connect("dividend_printer.db")
    income_statement.to_sql(
        name="income_statement", con=conn, if_exists="append", index=False
    )
    conn.commit()
    conn.close()


def update_income_statement(stocks: pd.DataFrame) -> None:
    """Update the income statement table."""
    for stock in stocks.SYMBOL:
        print(stock)
        income_statement = get_quarterly_income_statement(stock)
        income_statement.columns = income_statement.columns.str.upper()
        insert_into_income_statement_table(income_statement)
