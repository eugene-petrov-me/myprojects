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

SEASON_ID = "20242025" # Set to 2024/25 Season
REGULAR_SEASON = 2 # Game type for fetching stats
PLAYOFFS = 3 # Game type for fetching stats
GCS_BUCKET_NAME = "nhl-api-bucket"
SKATERS_FILE_NAME = "skaters_performance"
GOALIE_FILE_NAME = "goalies_performance"
DATASET_ID = "stg_nhl_data"

client = NHLClient(verbose=True)

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
            print(f"Request failed for player {player_id}: {e}")
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
                print(f"An error occurred for player {player_id}: {e}")

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

def load_data_to_bq(bucket_name, dataset_id, schema, file_name):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "creds/creds.json"
    client = bigquery.Client()
    try:
        # Ensure dataset exists
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(dataset_id)
    
    table_id = f"silent-effect-287314.{dataset_id}.{file_name}"
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
        print(f"Loaded {destination_table.num_rows} rows into {table_id}.")
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")

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
        load_data_to_gcs(GCS_BUCKET_NAME, df, f"{name}_{timestamp}")
        schema = get_schema(df)
        load_data_to_bq(GCS_BUCKET_NAME, DATASET_ID, schema, f"{name}_{timestamp}")


if __name__ == "__main__":
    main()