"""
    Question 3. Loading data to BigQuery

    Using etl_gcs_to_bq.py as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

    The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

    Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

    Make any other necessary changes to the code for it to function as required.

    Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

    Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table.

    How many rows did your flow code process?
"""


from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def get_the_number_of_rows(path: Path) -> int:
    """Get the number of rows in the DataFrame"""
    df = pd.read_parquet(path)
    return len(df)


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp2.rides",
        project_id="tenacious-veld-402613",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(path):
    """Main ETL flow to load data into Big Query"""
    df = pd.read_parquet(path)
    write_bq(df)

@flow()
def parent_flow(months: list[int] = [2,3], year: int = 2019, color: str = "yellow"):
    number = 0
    for month in months:
        path = extract_from_gcs(color, year, month)
        number += get_the_number_of_rows(path)
        
        etl_gcs_to_bq(path)
    
    print("the total number of scripts is: ", number)

if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    parent_flow(months, year, color)
