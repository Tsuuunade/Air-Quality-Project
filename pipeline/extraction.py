"""
Example usage: python extraction.py --locations-file-path ../locations.json --start-date 2024-01 --end-date 2024-03 --database-path ../air_quality.db --extract-query-template-path ../sql/data_manipulation_language/raw/0_raw_air_quality_insert.sql --source-base-path s3://openaq-data-archive/records/csv.gz
"""

import argparse
import json
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List

from duckdb import IOException
from jinja2 import Template

from database_manager import (
    connect_to_database,
    close_database_connection,
    execute_query,
    read_query
)

# Use type hinting to guide

def read_location_ids(file_path: str) -> List[str]:
    """
    Read a JSON file containing location IDs and return them 
    as a list of strings.

    Args:
        file_path (str): Path to the JSON file. The JSON is expected to 
            be an object whose keys are the location IDs.

    Returns:
        List[str]: A list of location IDs as strings.
    """

    with open(file_path, "r") as f:
        # parse JSON into a Python dict
        locations = json.load(f)
        f.close()

    # extract the dictionary keys (IDs)
    location_ids = [str(id) for id in locations.keys()]
    return location_ids


def compile_data_file_paths(
    data_file_path_template: str, location_ids: List[str], 
    start_date: str, end_date: str
) -> List[str]:
    """
    Generate a list of data file paths for each location and month in 
    the date range.

    Args:
        data_file_path_template (str): Jinja2 template string for the file path.
            It should use placeholders: {{location_id}}, {{year}}, {{month}}.
        location_ids (List[str]): List of location IDs to substitute 
            into the template.
        start_date (str): Inclusive start date in "YYYY-MM" format.
        end_date (str): Inclusive end date in "YYYY-MM" format.

    Returns:
        List[str]: A list of rendered file path strings for every month between
            start_date and end_date (inclusive) for each location.
    """
    
    # parse the input date strings into datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m")
    end_date = datetime.strptime(end_date, "%Y-%m")

    data_file_paths = []

    # for each location, iterate month by month through the date range
    for location_id in location_ids:
        index_date = start_date

        while index_date <= end_date:
            # render the template with the current location/year/month
            data_file_path = Template(data_file_path_template).render(
                location_id=location_id,
                year=str(index_date.year),
                month=str(index_date.month).zfill(2) # ensure two-digit month
            )
            data_file_paths.append(data_file_path)

            # move to the next month
            index_date += relativedelta(months=1)

    return data_file_paths

def compile_data_file_query(
    base_path: str, data_file_path: str, extract_query_template: str
) -> str:
    """
    Render the SQL extraction query for a given data file path.

    Args:
        base_path (str): Root path where data files are stored.
        data_file_path (str): Relative path template for the data file.
        extract_query_template (str): Jinja2 template of the SQL extraction 
            query, with a placeholder {{data_file_path}}.

    Returns:
        str: The fully rendered SQL query string.
    """
    # combine base path and relative file path and render the SQL query template
    extract_query = Template(extract_query_template).render(
        data_file_path=f"{base_path}/{data_file_path}"
    )
    return extract_query


def extract_data(args):
    """
    1. Reads location IDs from a JSON file.
    2. Compiles all data file paths for the given date range.
    3. Reads the SQL query template.
    4. Connects to the database.
    5. Executes the rendered query for each data file path.
    6. Closes the database connection.

    Args:
        args: Parsed CLI arguments with attributes:
            - locations_file_path (str)
            - start_date (str in YYYY-MM)
            - end_date (str in YYYY-MM)
            - extract_query_template_path (str)
            - database_path (str)
            - source_base_path (str)
    """
    
    # step 1: get list of location IDs
    location_ids = read_location_ids(args.locations_file_path)

    # step 2: build file path template and compile paths
    data_file_path_template = "locationid={{location_id}}/year={{year}}/month={{month}}/*"

    data_file_paths = compile_data_file_paths(
        data_file_path_template=data_file_path_template,
        location_ids=location_ids,
        start_date=args.start_date,
        end_date=args.end_date
    )

    # step 3: load the SQL extraction template from file
    extract_query_template = read_query(path=args.extract_query_template_path)

    # step 4: open database connection
    con = connect_to_database(path=args.database_path)

    # step 5: loop over every file path and run the query
    for data_file_path in data_file_paths:
        logging.info(f"Extracting data from {data_file_path}")
        query = compile_data_file_query(
            base_path=args.source_base_path,
            data_file_path=data_file_path,
            extract_query_template=extract_query_template
        )

        try:
            # execute the rendered SQL
            execute_query(con, query)
            logging.info(f"Extracted data from {data_file_path}!")
        except IOException as e:
            logging.warning(f"Could not find data from {data_file_path}: {e}")
    
    # step 6: close the database connection
    close_database_connection(con)


def main():
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description="CLI for ELT Extraction")
    parser.add_argument(
        "--locations-file-path",
        type=str,
        required=True,
        help="Path to the locations JSON file",
    )
    parser.add_argument(
        "--start-date", type=str, required=True, 
        help="Start date in YYYY-MM format"
    )
    parser.add_argument(
        "--end-date", type=str, required=True, 
        help="End date in YYYY-MM format"
    )
    parser.add_argument(
        "--extract-query-template-path",
        type=str,
        required=True,
        help="Path to the SQL extraction query template",
    )
    parser.add_argument(
        "--database-path", type=str, required=True, help="Path to the database"
    )
    parser.add_argument(
        "--source-base-path",
        type=str,
        required=True,
        help="Base path for the remote data files",
    )

    args = parser.parse_args()
    extract_data(args)

if __name__ == "__main__":
    main()