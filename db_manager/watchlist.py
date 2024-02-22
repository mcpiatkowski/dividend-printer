"""Watchlist table manager."""

import sqlite3
import pandas as pd


def create_watchlist_table() -> None:
    """Create watchlist table."""
    conn = sqlite3.connect("dividend_printer.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS watchlist")
    c.execute(
        """
    CREATE TABLE watchlist (
        SYMBOL PRIMARY KEY,
        DATE_ADDED TEXT
    )
    """
    )
    conn.commit()
    conn.close()


def insert_into_watchlist_table(watchlist: pd.DataFrame) -> None:
    """Insert into the watchlist table."""
    conn = sqlite3.connect("dividend_printer.db")
    watchlist.to_sql(name="watchlist", con=conn, if_exists="append", index=False)
    conn.commit()
    conn.close()


def update_watchlist(symbols: list[str]) -> None:
    """Update the watchlist table."""
    create_watchlist_table()
    watchlist: pd.DataFrame = pd.DataFrame({"SYMBOL": symbols}).assign(
        DATE_ADDED=pd.to_datetime("now").strftime("%Y-%m-%d")
    )
    insert_into_watchlist_table(watchlist)
