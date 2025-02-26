import pandas as pd
import time
import httpx
import datetime
import os
from google.cloud import storage
from google.cloud import bigquery
from nhlpy.nhl_client import NHLClient
import concurrent.futures
import threading
from dotenv import load_dotenv
from pathlib import Path
import logging

dotenv_path = Path("creds/nhl-env-var.env")
load_dotenv(dotenv_path=dotenv_path)

SEASON_ID = "20242025" # Set to 2024/25 Season
REGULAR_SEASON = 2 # Game type for fetching stats
PLAYOFFS = 3 # Game type for fetching stats

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
SKATERS_FILE_NAME = os.getenv("SKATERS_FILE_NAME")
GOALIE_FILE_NAME = os.getenv("GOALIE_FILE_NAME")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
STAGING_DATASET_ID = os.getenv("STAGING_DATASET_ID")
PROD_DATASET_ID = os.getenv("PROD_DATASET_ID")

client = NHLClient(verbose=True)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", 
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO)

# Function to get default values for English spelling of players names
def get_default_value(column, get_value):
    return column.apply(lambda x: x.get(get_value) if isinstance(x, dict) else x)

# Function that returns list of franchises
def get_team_info():
    # Load franchises data for the most recent date into a dataframe
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    team_info = client.teams.teams_info(date=date)
    df_team_info = pd.DataFrame(team_info)

    # Return a list of franchises for future iterations
    teams_list = df_team_info["abbr"].to_list()
    return teams_list

# Funtion to get team rosters for a given season
def get_team_roster(team_abbr, season_id):
    
    # Load roster forwards, defensemen and goalies data into a dataframe 
    data = client.teams.roster(team_abbr=team_abbr, season=season_id)
    data_combined = data["forwards"] + data["defensemen"] + data ["goalies"]
    df = pd.DataFrame(data_combined)

    # Get default English player and geographical names and drop columns that aren't needed
    if df.shape[1] > 0:
        # Clean columns
        columns_to_change = ["firstName", "lastName", "birthCity", "birthStateProvince"]
        for column in columns_to_change:
            df[column] = get_default_value(df[column], "default")

        # Add a column for the current team a player is signed to
        df["currentTeam"] = team_abbr

        # Drop columns
        df.drop(
            columns=["heightInInches", "weightInPounds", "birthCity", "birthStateProvince"],
            axis=1,
            inplace=True
        )

    return df

# Function to get game logs for a player for a given season and game type - pre-season, regular season or playoffs
def get_game_logs(player_id, season_id, game_type):
    
    # Get the data and load it into a dataframe
    data = client.stats.player_game_log(player_id=player_id, season_id=season_id, game_type=game_type)
    df = pd.DataFrame(data)
    if df.shape[1] > 0:
        # Drop columns that aren't needed
        df.drop(columns=["commonName", "opponentCommonName"], inplace=True)
        
        # Convert gameDate column to date data type
        df["gameDate"] = pd.to_datetime(df["gameDate"], format="%Y-%m-%d")
        
        # Convert time on ice to seconds and drop the original column
        df["toiInSeconds"] = df["toi"].apply(
            lambda x: 
                int(x.split(":")[0]) * 60 + int(x.split(":")[1]) 
                if len(x.split(":")) == 2 
                else int(x.split(":")[0]) * 60 * 60 + int(x.split(":")[1]) * 60 + int(x.split(":")[2]))
        df.drop(columns=["toi"], inplace=True)

        # Append player and season ids
        df["playerId"] = player_id
        df["seasonId"] = season_id

    return df

# Global lock for rate-limiting
lock = threading.Lock()

# Helper function for dynamic rate-limiting
def rate_limit(api_calls_per_second):
    time_between_calls = 1 / api_calls_per_second
    time.sleep(time_between_calls)

# Function for fetching game logs
def get_combined_game_logs(players_list, season_id, game_type, max_workers=10, api_calls_per_second=5):
    
    def fetch_game_logs(player_id):
        with lock:
            rate_limit(api_calls_per_second)  # Enforce rate-limiting
        try:
            df = get_game_logs(player_id, season_id, game_type)
            return df
        except httpx.RequestError as e:
            logging.error(f"Request failed for player {player_id}: {e}")
            return None

    # Using ThreadPoolExecutor for concurrency
    game_logs_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_game_logs, player): player for player in players_list}

        for future in concurrent.futures.as_completed(futures):
            player_id = futures[future]
            try:
                result = future.result()
                if result is not None:
                    game_logs_list.append(result)
            except Exception as e:
                logging.error(f"An error occurred for player {player_id}: {e}")

    # Combine all game logs into a single DataFrame
    if game_logs_list:
        return pd.concat(game_logs_list, ignore_index=True)
    else:
        return pd.DataFrame()

def load_data_to_gcs(bucket_name, df, file_name):

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="creds/creds.json"
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f'{file_name}.csv')
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')

def get_schema(df):
    schema = []
    for col, dtype in zip(df.columns, df.dtypes):
        if dtype == "int64":
            field_type = "INT64"
        elif dtype == "float64":
            field_type = "FLOAT"
        elif dtype == "object":
            field_type = "STRING"
        elif dtype.name.startswith("datetime"):
            field_type = "DATETIME"
        else:
            field_type = "STRING"
        schema.append(bigquery.SchemaField(col, field_type))
    return schema    

def load_data_to_bq(bucket_name, project_id, dataset_id, schema, file_name):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "creds/creds.json"
    client = bigquery.Client()
    try:
        # Ensure dataset exists
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(dataset_id)
    
    table_id = f"{project_id}.{dataset_id}.{file_name}"
    job_config = bigquery.LoadJobConfig(
        schema=schema, 
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
    )

    uri = f"gs://{bucket_name}/{file_name}.csv"
    try:
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        destination_table = client.get_table(table_id)
        logging.info(f"Loaded {destination_table.num_rows} rows into {table_id}.")

        expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            days=5
        )
        destination_table.expires = expiration
        destination_table = client.update_table(destination_table, ["expires"])
        logging.info(f"Updated {table_id}, expires {destination_table.expires}.")
    except Exception as e:
        logging.error(f"Error loading data to BigQuery: {e}")


def sync_table_schema(project_id, prod_dataset_id, prod_table, staging_dataset_id, staging_table):
    client = bigquery.Client()
    prod_table_id = f"{project_id}.{prod_dataset_id}.{prod_table}"
    staging_table_id = f"{project_id}.{staging_dataset_id}.{staging_table}"

    # Get existing production table schema
    try:
        prod_table_ref = client.get_table(prod_table_id)
        existing_columns = {field.name: field.field_type for field in prod_table_ref.schema}
    except Exception as e:
        logging.error(f"Error fetching schema for {prod_table_id}: {e}")
        return
    
    # Identify missing columns
    try: 
        staging_table_ref = client.get_table(staging_table_id)
        new_columns = [field for field in staging_table_ref.schema if field.name not in existing_columns]
    except Exception as e:
        logging.error(f"Error fetching schema for {staging_table_id}: {e}")
        return

    if new_columns:
        logging.info(f"Adding missing columns to `{prod_table_id}`: {[col.name for col in new_columns]}")

        for col in new_columns:
            try:
                alter_table_query = f"""
                    ALTER TABLE `{prod_table_id}`
                    ADD COLUMN {col.name} {col.field_type};
                """
                client.query(alter_table_query).result()
                logging.info(f"Column `{col.name}` added successfully.")
            except Exception as e:
                logging.error(f"Error adding column `{col.name}`: {e}")
    else:
        logging.info("No new columns to add.")

def upsert_data_in_bq(project_id, staging_dataset_id, staging_table, prod_dataset_id, prod_table, key_columns):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "creds/creds.json"
    client = bigquery.Client()

    prod_table_id = f"{project_id}.{prod_dataset_id}.{prod_table}"
    staging_table_id = f"{project_id}.{staging_dataset_id}.{staging_table}"

    # Check if the production table exists
    tables = [table.table_id for table in client.list_tables(prod_dataset_id)]

    if prod_table not in tables:
        logging.warning(f"Production table `{prod_table}` does not exist. Creating it...")
        try: 
            query = f"""
                CREATE TABLE `{prod_table_id}`
                PARTITION BY DATE(gameDate)
                CLUSTER BY gameId
                AS
                SELECT * FROM `{staging_table_id}`
                WHERE 1=0;
            """
            query_job = client.query(query=query)
            query_job.result()
            logging.info(f"Created production table `{prod_table_id}`.")
        except Exception as e:
            logging.error(f"Error creating a table in BigQuery: {e}")

    # Sync schema in case new columns added
    sync_table_schema(project_id, prod_dataset_id, prod_table, staging_dataset_id, staging_table)

    staging_schema = client.get_table(staging_table_id).schema
    non_key_columns = [col.name for col in staging_schema if col.name not in key_columns]

    try:
        merge_query = f"""
            MERGE `{prod_table_id}` T
            USING `{staging_table_id}` S
            ON {" AND ".join([f"T.{col} = S.{col}" for col in key_columns])}
            WHEN MATCHED THEN
            UPDATE SET
                {", ".join([f"T.{col} = S.{col}" for col in non_key_columns]) if non_key_columns else ""}
            WHEN NOT MATCHED THEN
            INSERT ({", ".join([col.name for col in staging_schema])})
            VALUES ({", ".join([f"S.{col.name}" for col in staging_schema])});
        """

        logging.info("Executing MERGE query...")
        merge_job = client.query(query=merge_query)
        merge_job.result()
        logging.info(f"Data upserted into `{prod_table_id}`.")
    except Exception as e:
        logging.error(f"Error upserting date into a table in BigQuery: {e}")

def main():

    teams = get_team_info()

    # Fetch team rosters and combine them
    rosters = [get_team_roster(team, SEASON_ID) for team in teams]
    df_team_roster_combined = pd.concat(rosters)

    # Save player ids as lists
    skater_ids = df_team_roster_combined.loc[df_team_roster_combined['positionCode'] != "G"]['id'].to_list()
    goalie_ids = df_team_roster_combined.loc[df_team_roster_combined['positionCode'] == "G"]['id'].to_list()

    # Fetch combined game logs for all players
    df_game_logs_skaters = get_combined_game_logs(skater_ids, SEASON_ID, REGULAR_SEASON)
    df_game_logs_goalies = get_combined_game_logs(goalie_ids, SEASON_ID, REGULAR_SEASON)

    # Join game logs data with player attributes from the team roster dataframe.
    df_skaters_performance = df_game_logs_skaters.merge(df_team_roster_combined, how="left", left_on="playerId", right_on="id")
    df_goalies_performance = df_game_logs_goalies.merge(df_team_roster_combined, how="left", left_on="playerId", right_on="id")

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    for df, name in [(df_skaters_performance, SKATERS_FILE_NAME), (df_goalies_performance, GOALIE_FILE_NAME)]:
        file_name = f"{name}_{timestamp}"
        prod_table = f"{name}"
        load_data_to_gcs(GCS_BUCKET_NAME, df, file_name)
        schema = get_schema(df)
        load_data_to_bq(
            GCS_BUCKET_NAME, 
            GCP_PROJECT_ID, 
            STAGING_DATASET_ID, 
            schema, 
            file_name)
        upsert_data_in_bq(
            GCP_PROJECT_ID, 
            STAGING_DATASET_ID, 
            file_name, 
            PROD_DATASET_ID, 
            prod_table,
            ["gameDate", "gameId", "playerId"])

if __name__ == "__main__":
    main()