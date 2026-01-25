"""
Data ingestion script for loading CSV and Parquet files into PostgreSQL database.

This script provides functionality to ingest taxi trip data and zone lookup data
from local files into PostgreSQL tables.
"""
from sqlalchemy import create_engine, Engine
import argparse
import pandas as pd
import os

DATA_DIR = 'data'
FILE_TABLE_MAP = {
    "green_tripdata": "green_tripdata_2025-11.parquet",
    "taxi_zone_lookup": "taxi_zone_lookup.csv",
}

def _ingest_df_to_db(df: pd.DataFrame, table_name: str, engine: Engine):
    """
    Internal helper function to ingest a pandas DataFrame into a PostgreSQL table.
    
    Creates the table schema first (if it doesn't exist) and then appends the data.
    This two-step approach ensures the table structure is created correctly before
    inserting data.
    
    Args:
        df: pandas DataFrame containing the data to be ingested
        table_name: Name of the target PostgreSQL table
        engine: SQLAlchemy Engine instance connected to the database
    """
    print(f"Ingesting data into table {table_name}")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print(f"Data ingested into table {table_name}")

def ingest_parquet_file(file_path: str, table_name: str, engine: Engine):
    """
    Ingests a Parquet file into a PostgreSQL table.
    
    Reads a Parquet file from the specified path, loads it into a pandas DataFrame,
    and then ingests it into the specified database table.
    
    Args:
        file_path: Path to the Parquet file to be ingested
        table_name: Name of the target PostgreSQL table
        engine: SQLAlchemy Engine instance connected to the database
    """
    df = pd.read_parquet(file_path)
    _ingest_df_to_db(df, table_name, engine)

def ingest_csv_file(file_path: str, table_name: str, engine: Engine):
    """
    Ingests a CSV file into a PostgreSQL table.
    
    Reads a CSV file from the specified path, loads it into a pandas DataFrame,
    and then ingests it into the specified database table.
    
    Args:
        file_path: Path to the CSV file to be ingested
        table_name: Name of the target PostgreSQL table
        engine: SQLAlchemy Engine instance connected to the database
    """
    df = pd.read_csv(file_path)
    _ingest_df_to_db(df, table_name, engine)


def ingest_data(args: argparse.Namespace):
    """
    Main data ingestion function that processes all files defined in FILE_TABLE_MAP.
    
    Creates a database connection and iterates through the file mapping to ingest
    each file into its corresponding PostgreSQL table. Automatically detects file
    type (Parquet or CSV) and uses the appropriate ingestion function.
    
    Args:
        args: argparse.Namespace containing database connection parameters:
            - user: PostgreSQL username
            - password: PostgreSQL password
            - host: PostgreSQL host address
            - port: PostgreSQL port number
            - database: PostgreSQL database name
    """
    engine = create_engine(f'postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}')
    for table_name, file_name in FILE_TABLE_MAP.items():
        file_path = os.path.join(DATA_DIR, file_name)
        if file_name.endswith('.parquet'):
            ingest_parquet_file(file_path, table_name, engine)
        elif file_name.endswith('.csv'):
            ingest_csv_file(file_path, table_name, engine)

def parse_args():
    """
    Parses command-line arguments for database connection parameters.
    
    Returns:
        argparse.Namespace: Parsed arguments containing:
            - user: PostgreSQL username (required)
            - password: PostgreSQL password (required)
            - host: PostgreSQL host address (required)
            - port: PostgreSQL port number (required, int)
            - database: PostgreSQL database name (required)
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', type=str, default='postgres')
    parser.add_argument('--password', type=str, default='postgres')
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=5432)
    parser.add_argument('--database', type=str, default='ny_taxi')
    return parser.parse_args()

def main():
    """
    Main entry point for the data ingestion script.
    
    Parses command-line arguments and initiates the data ingestion process.
    """
    args = parse_args()
    ingest_data(args)

if __name__ == '__main__':
    main()