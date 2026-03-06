from etl_project.connectors.football_api import FootballDataAPI 
from etl_project.connectors.postgresql import PostgreSqlClient
import time
from loguru import logger
from sqlalchemy import Table, MetaData
import pandas as pd
#from datetime import datetime, timezone


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



def transform(df_football: pd.DataFrame) -> pd.DataFrame:
    """Transform the raw dataframes."""

    columns_to_drop = [
        "referees",
        "odds_msg",
        "homeTeam_crest",
        "competition_emblem",
        "awayTeam_crest"
    ]

    df_transformed = df_football.drop(columns=columns_to_drop, errors="ignore")

    return df_transformed


def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
) -> None:
    """
    Load dataframe to either a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
