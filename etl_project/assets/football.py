from etl_project.connectors.football_api import FootballDataAPI 
import time
from loguru import logger
import pandas as pd
import numpy as np


def extract_competitions(football_api_client: FootballDataAPI):
    """
    Perform extraction of all available competitions.
    """

    data = football_api_client.get_competitions()

    comp_ids = []
    for comp in data:
        comp_ids.append(comp.get("id"))

    return comp_ids

def extract_matches_full(football_api_client: FootballDataAPI, comp_ids: list[int]): #, min_datetime: str ???
    """
    Perform extraction of all matches for competitions, for a list of competitions
    """

    logger.info("The free API only allows 10 calls per minute, so we will stagger our calls...")

    matches = []
    for comp_id in comp_ids:
        logger.info(f"Getting match data for comp_id:{comp_id}")
        matches.extend(football_api_client.get_matches(competition_id = comp_id))
        time.sleep(7) #the free api only allows 10 calls per minute, so we will stagger our calls

    df = pd.json_normalize(matches, sep="_")
    
    return df

def extract_matches_incremental(football_api_client: FootballDataAPI, comp_ids: list[int], ): #, min_datetime: str ???
    """
    Perform extraction of all matches after date_value, for a list of competitions
    """

    logger.info("The free API only allows 10 calls per minute, so we will stagger our calls...")

    matches = []
    for comp_id in comp_ids:
        logger.info(f"Getting match data for comp_id:{comp_id}")
        matches.extend(football_api_client.get_matches(competition_id = comp_id))
        time.sleep(7) #the free api only allows 10 calls per minute, so we will stagger our calls

    df = pd.json_normalize(matches, sep="_")
    
    return df

def football_transform(df_football: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""

    columns_to_drop = [
        "referees",
        "area_flag"
        "odds_msg",
        "homeTeam_crest",
        "competition_emblem",
        "awayTeam_crest"
    ]
    df_transformed = df_football.drop(columns=columns_to_drop, errors="ignore")

    datetime_columns = ["utcDate", "lastUpdated"]
    for col in datetime_columns:
        df_transformed[col] = pd.to_datetime(df_transformed[col], utc=True)

    date_columns = ["season_startDate", "season_endDate"]
    for col in date_columns:
        df_transformed[col] = pd.to_datetime(df_transformed[col]).dt.date


    df_transformed["match_date"] = df_transformed["utcdate"].dt.date

    df_transformed = df_transformed.replace({np.nan:None})
    df_transformed.columns = df_transformed.columns.str.lower()

    return df_transformed



