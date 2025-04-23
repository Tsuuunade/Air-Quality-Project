import argparse
import logging

from database_manager import (
    connect_to_database,
    close_database_connection,
    execute_query,
    collect_query_paths,
    read_query,
)

# Use type hinting to guide

def transform_data(args) -> None:
    """
    Execute a series of SQL transformation queries against a database.

    Args:
        args: Parsed command‑line arguments with attributes:
            - database_path (str): Path to the DuckDB (or other) database file.
            - query_directory (str): Directory containing SQL files to run.
    """

    database_path = args.database_path
    con = connect_to_database(path=database_path)
    # gather all SQL file paths from the specified directory
    query_paths = collect_query_paths(args.query_directory)

    # iterate over each SQL file and execute its contents
    for query_path in query_paths:
        query = read_query(query_path)
        # execute the SQL against the open connection
        execute_query(con, query)

        logging.info(f"Executed query from {query_path}")

    close_database_connection(con)


def main():
    logging.getLogger().setLevel(logging.INFO)

    # define the command‑line interface
    parser = argparse.ArgumentParser(
        description="CLI for Data Transformation"
    )
    parser.add_argument(
        "--database-path", type=str, required=True, 
        help="Path to the DuckDB database"
    )
    parser.add_argument(
        "--query-directory",
        type=str,
        required=True,
        help="Directory containing SQL transformation queries",
    )

    args = parser.parse_args()
    transform_data(args)
    
if __name__ == "__main__":
    main()