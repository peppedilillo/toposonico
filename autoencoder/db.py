"""Database utilities for querying large Spotify SQLite databases."""

import sqlite3
from pathlib import Path

import pandas as pd


def get_connection(database_path: Path) -> sqlite3.Connection:
    """
    Get a read-only connection to the database.

    Returns:
        sqlite3.Connection configured for read-only access
    """
    uri = f"file:{database_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def get_tables(conn: sqlite3.Connection) -> list[str]:
    """List all tables in a database."""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    return [row[0] for row in cursor.fetchall()]


def get_columns(conn: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
    """
    Get column names and types for a table.

    Returns:
        List of (column_name, column_type) tuples
    """
    cursor = conn.execute(f"PRAGMA table_info({table})")
    return [(row[1], row[2]) for row in cursor.fetchall()]


def head_table(
    conn: sqlite3.Connection,
    table: str,
    n: int = 10,
) -> pd.DataFrame:
    """Get the first n rows from a table (fast, non-random)."""
    query = f"SELECT * FROM {table} LIMIT {n}"
    return pd.read_sql_query(query, conn)
