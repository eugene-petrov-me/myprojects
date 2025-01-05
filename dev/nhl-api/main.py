import pandas as pd
import time
import httpx
import datetime
import os
from google.cloud import storage
from google.cloud import bigquery
from nhlpy.nhl_client import NHLClient

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

    # Get default English names for conferences and divisions
    # columns_to_change = ["conference", "division"]
    # for column in columns_to_change:
    #     df_team_info[column] = get_default_value(df_team_info[column], "name")

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

# Function to fetch game logs for a list of player ids and return a dafaframe
def get_combined_game_logs(players_list, season_id, game_type):

    game_logs_list = []
    for player in players_list:
        try:
            df = get_game_logs(player, season_id, game_type)
            game_logs_list.append(df)
            time.sleep(0.5)
        except httpx.RequestError as e:
            print(f"Request failed for player {id}: {e}")
    return pd.concat(game_logs_list)

def load_data_to_gcs(bucket_name, df, file_name):

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="creds/creds.json"
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f'{file_name}.csv')
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')

def load_skaters_data_to_bq(bucket_name, dataset_id, file_name):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="creds/creds.json"
    client = bigquery.Client()
    try: 
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(dataset_id)
    
    table_id = f"silent-effect-287314.{dataset_id}.{file_name}"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('gameId', "INT64"),
            bigquery.SchemaField('teamAbbrev', "STRING"),
            bigquery.SchemaField('homeRoadFlag', "STRING"),
            bigquery.SchemaField('gameDate', "DATE"),
            bigquery.SchemaField('goals', "INT64"),
            bigquery.SchemaField('assists', "INT64"),
            bigquery.SchemaField('points', "INT64"),
            bigquery.SchemaField('plusMinus', "INT64"),
            bigquery.SchemaField('powerPlayGoals', "INT64"),
            bigquery.SchemaField('powerPlayPoints', "INT64"),  
            bigquery.SchemaField('gameWinningGoals', "INT64"),
            bigquery.SchemaField('otGoals', "INT64"),
            bigquery.SchemaField('shots', "INT64"),
            bigquery.SchemaField('shifts', "INT64"),
            bigquery.SchemaField('shorthandedGoals', "INT64"),
            bigquery.SchemaField('shorthandedPoints', "INT64"),
            bigquery.SchemaField('opponentAbbrev', "STRING"),
            bigquery.SchemaField('pim', "INT64"),
            bigquery.SchemaField('toiInSeconds', "INT64" ),
            bigquery.SchemaField('playerId', "INT64" ),
            bigquery.SchemaField('seasonId', "INT64" ),
            bigquery.SchemaField('id', "INT64" ),
            bigquery.SchemaField('headshot', "STRING"),
            bigquery.SchemaField('firstName', "STRING"),
            bigquery.SchemaField('lastName', "STRING"),
            bigquery.SchemaField('sweaterNumber', "INT64" ),
            bigquery.SchemaField('positionCode', "STRING"),
            bigquery.SchemaField('shootsCatches', "STRING"),
            bigquery.SchemaField('heightInCentimeters', "INT64" ),
            bigquery.SchemaField('weightInKilograms', "INT64" ),
            bigquery.SchemaField('birthDate', "DATE"),
            bigquery.SchemaField('birthCountry', "STRING")
        ], 
        skip_leading_rows=1
    )

    uri = f"gs://{bucket_name}/{file_name}.csv"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
        )
    load_job.result()
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))    

def main():

    # Creating a list to store team rosters dataframes
    list_of_roster_dfs = []

    # Save each team roster into a list and convert to a dataframe
    for team in get_team_info():
        df = get_team_roster(team, SEASON_ID)
        list_of_roster_dfs.append(df)
    df_team_roster_combined = pd.concat(list_of_roster_dfs)

    # Save player ids as lists
    skater_ids = df_team_roster_combined.loc[df_team_roster_combined['positionCode'] != "G"]['id'].to_list()
    goalie_ids = df_team_roster_combined.loc[df_team_roster_combined['positionCode'] == "G"]['id'].to_list()

    # Fetch combined game logs for all players
    df_game_logs_skaters = get_combined_game_logs(skater_ids, SEASON_ID, REGULAR_SEASON)
    df_game_logs_goalies = get_combined_game_logs(goalie_ids, SEASON_ID, REGULAR_SEASON)

    # Join game logs data with player attributes from the team roster dataframe.
    df_skaters_performance = df_game_logs_skaters.merge(df_team_roster_combined, how="left", left_on="playerId", right_on="id")
    df_goalies_performance = df_game_logs_goalies.merge(df_team_roster_combined, how="left", left_on="playerId", right_on="id")

    ts = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    # Load data to Google Cloud Storage as CSV
    load_data_to_gcs(GCS_BUCKET_NAME, df_skaters_performance, f"{SKATERS_FILE_NAME}_{ts}")
    load_data_to_gcs(GCS_BUCKET_NAME, df_goalies_performance, f"{GOALIE_FILE_NAME}_{ts}")

    # Load skaters data
    load_skaters_data_to_bq(GCS_BUCKET_NAME, DATASET_ID, f"{SKATERS_FILE_NAME}_{ts}")

if __name__ == "__main__":
    main()