"""
This service is about methods that aid in calculation of usage metrics
"""

from dataclasses import dataclass, field
from typing import List

import duckdb

from src.validation import data_validate

init_query = """BEGIN TRANSACTION;
                CREATE TABLE IF NOT EXISTS duck_store_staging (
                id UUID PRIMARY KEY,
                hash VARCHAR,
                store_name VARCHAR NULL,
                item_code VARCHAR NULL,
                item_barcode VARCHAR NULL,
                supplier VARCHAR NULL,
                description TEXT NULL,
                category VARCHAR NULL,
                department VARCHAR NULL,
                sub_department VARCHAR NULL,
                section VARCHAR NULL,
                quantity HUGEINT NULL,
                total_sales DOUBLE NULL,
                rrp DOUBLE NULL,
                date_of_sale DATE NULL
                );

                CREATE TABLE IF NOT EXISTS duck_store (
                id UUID,
                hash VARCHAR UNIQUE,
                store_name VARCHAR,
                item_code VARCHAR,
                item_barcode VARCHAR,
                supplier VARCHAR,
                description TEXT,
                category VARCHAR,
                department VARCHAR,
                sub_department VARCHAR,
                section VARCHAR,
                quantity HUGEINT,
                total_sales DOUBLE,
                rrp DOUBLE,
                sale_price DOUBLE,
                margin DOUBLE,
                date_of_sale DATE
                );

                CREATE TABLE IF NOT EXISTS validation_errors (
                id UUID DEFAULT uuidv7(),
                row_id UUID,
                hash VARCHAR,
                field_name VARCHAR NULL,                
                cell_value VARCHAR NULL,
                message VARCHAR NULL,
                note VARCHAR NULL
                );

                COMMIT;
                """


@dataclass
class DuckDB:
    """
    A class to connect with a DuckDB database.

    Attributes:
    -----------
    db : str
        The name of the database file (default is "data/duckdb.duckdb").
    conn : duckdb.DuckDBPyConnection
        The connection object to the DuckDB database (initialized after instantiation).

    Methods:
    --------
    __db__():
        Initializes the database by connecting to DuckDB and configuring access to S3 storage.
    commit():
        Saves changes to the S3 bucket by copying the updated data.
    """

    db: str = field(default="data/duckdb.duckdb")
    conn: duckdb.DuckDBPyConnection = field(init=False)

    def __post_init__(self):
        self.conn = self.__db__()

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object."""
        self.conn.close()

    def __db__(self):
        """
        Reloads the database connection and configures the DuckDB environment to access S3.

        Returns:
        --------
        duckdb.DuckDBPyConnection
            The initialized DuckDB connection object.
        """

        conn = duckdb.connect(self.db)
        conn.sql(init_query)

        return conn

    def process_validation(self) -> List[str]:
        rows = self.conn.execute("SELECT * FROM duck_store_staging").fetchdf()
        rows["id"] = rows["id"].apply(lambda x: x.hex)
        ids, errors = data_validate(rows.to_dict("records"))

        if errors:
            self.conn.executemany(
                """INSERT INTO validation_errors VALUES(?, ?, ?, ?, ?, ?, ?)""",
                [
                    (
                        e["id"],
                        e["row_id"],
                        e["hash"],
                        e["field_name"],
                        e["cell_value"],
                        e["message"],
                        e["note"],
                    )
                    for e in errors
                ],
            )

        return ids


def read_query(query: str):
    """
    Runs a query and returns df.

    Returns:
    --------
    pd.DataFrame
        A DataFrame containing all selected records.
    """
    with DuckDB() as ddb:
        return ddb.conn.sql(query).fetchall()


def run_query(query: str):
    """
    Executes a query.

    Parameters:
    -----------
    query : str
        The SQL query to be executed.
    """

    with DuckDB() as ddb:
        ddb.conn.sql(query)
