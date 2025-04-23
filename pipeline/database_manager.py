from typing import List
import os
import argparse
import logging

from duckdb import DuckDBPyConnection
import duckdb as ddb

# Use type hinting to guide

def connect_to_database(path: str) -> DuckDBPyConnection:
    """
    Establish a connection to a DuckDB database located at the given path.

    Args:
        path (str): Path to the DuckDB database file.

    Returns:
        DuckDBPyConnection: An active connection to the DuckDB database.
    """

    # give users feedback on what they're doing
    logging.info(f"Connecting to database at {path}")

    con = ddb.connect(path)
    # set placeholder S3 credentials in case remote files are accessed
    con.sql("""
        SET s3_access_key_id='';
        SET s3_secret_access_key='';
        SET s3_region='';
        """)
    return con


def close_database_connection(con: DuckDBPyConnection) -> None:
    """
    Closes an existing DuckDB database connection.

    Args:
        con (DuckDBPyConnection): The active DuckDB connection to close.
    """

    logging.info(f"Closing database connection")
    con.close()


def collect_query_paths(parent_dir: str) -> List[str]:
    """
    Recursively search for `.sql` files within a parent directory.

    Args:
        parent_dir (str): The root directory to begin searching for SQL files.

    Returns:
        List[str]: A sorted list of full paths to found SQL files.
    """
    
    sql_files = []

    # walk through the directory tree rooted at parent_dir
    for root, _, files in os.walk(parent_dir):
        for file in files:
            # check if the file has a .sql extension
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                sql_files.append(file_path)
    
    logging.info(f"Found {len(sql_files)} sql scripts at location {parent_dir}")
    # return paths sorted alphabetically
    return sorted(sql_files)


def read_query(path: str) -> str:
    """
    Reads a SQL file from the given file path and returns 
    its contents as a string.

    Args:
        path (str): Full path to the SQL file.

    Returns:
        str: SQL query content as a string.
    """

    with open(path, "r") as f:
        query = f.read()
        f.close()
    return query


def execute_query(con: DuckDBPyConnection, query: str) -> None:
    """
    Executes a SQL query using the provided DuckDB connection.

    Args:
        con (DuckDBPyConnection): The database connection object.
        query (str): The SQL query to be executed.

    Returns:
        None
    """

    con.execute(query)


def setup_database(database_path: str, ddl_query_parent_dir: str) -> None:
    """
    Initializes the DuckDB database by executing all SQL files found 
    in the given directory.

    Args:
        database_path (str): Path where the DuckDB database will be 
            created or connected.
        ddl_query_parent_dir (str): Directory containing DDL (schema) 
            SQL scripts.

    Returns:
        None
    """
    
    query_paths = collect_query_paths(ddl_query_parent_dir)
    con = connect_to_database(database_path)

    # iterate through each of the queries and excute them
    for query_path in query_paths:
        query = read_query(query_path)
        execute_query(con, query)
        logging.info(f"Executed query from {query_path}")
    
    # disconnect database
    close_database_connection(con)


def destroy_database(database_path: str) -> None:
    """
    Deletes the DuckDB database file from disk if it exists.

    Args:
        database_path (str): Path to the DuckDB database file to be deleted.

    Returns:
        None
    """

    if os.path.exists(database_path):
        os.remove(database_path)


def main():
    """
    Entry point for the command line instruction (CLI) tool.
    Handles command-line arguments to either set up or destroy the database.
    
    Returns:
        None
    """
    
    logging.getLogger().setLevel(logging.INFO)

    # set new command line argument parser
    parser = argparse.ArgumentParser(description=
        "CLI tool to setup or destroy a database.")

    # make sure it's a mutually exclusive argument
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--create", action="store_true", 
                       help="Create the database")
    group.add_argument("--destroy", action="store_true", 
                       help="Destroy the database")

    parser.add_argument("--database-path", type=str, 
                        help="Path to the database")
    parser.add_argument("--ddl-query-parent-dir", type=str, 
                        help="Path to the parent directory of the ddl queries")

    args = parser.parse_args()

    if args.create:
        setup_database(database_path=args.database_path, 
                       ddl_query_parent_dir=args.ddl_query_parent_dir)
    elif args.destroy:
        destroy_database(database_path=args.database_path)

# ensures the script runs when executed directly
if __name__ == "__main__":
    main()