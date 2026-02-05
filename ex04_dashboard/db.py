import os
import pandas as pd
import psycopg2


def get_conn():
    """Create and return a PostgreSQL connection using env vars."""
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "bigdata_db"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
    )


def read_sql(query: str, params=None) -> pd.DataFrame:
    """Run a SQL query and return a pandas DataFrame."""
    with get_conn() as conn:
        return pd.read_sql_query(query, conn, params=params)
