from etl_project.connectors.football_api import FootballDataAPI 
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from etl_project.assets.football import (
    extract_competitions,
    extract_matches_full,
    transform,
    load
)
from loguru import logger
from dotenv import load_dotenv
import os

# Replace with your API token
api_token = "8f139e0850984b6c93db30ca80d836c6"

if __name__ == "__main__":

    load_dotenv()

    logger.info("Starting pipeline run")

    logger.info("Getting pipeline environment variables")
    # environment variables for api
    API_TOKEN = os.environ.get("API_TOKEN")

    # environment variables for database
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    logger.info("Creating Football API Client")
    football_api_client = FootballDataAPI(api_token=API_TOKEN)

    logger.info("Extracting data from Football API")
    #comp_ids = extract_competitions(football_api_client=football_api_client)
    comp_ids = [2016]
    df_football = extract_matches_full(football_api_client=football_api_client, comp_ids=comp_ids)

    logger.info("Transforming dataframe")
    df_transformed = transform(df_football=df_football)

    # load
    logger.info("Loading data to postgres")
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )

    metadata = MetaData()
    table = Table(
        "football_data",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("utcDate", String),
        Column("status", String),
        Column("matchday", Integer),
        Column("stage", String),
        Column("group", String),
        Column("lastUpdated", String),
        #Column("referees", String),
        Column("area_id", Integer),
        Column("area_name", String),
        Column("area_code", String),
        Column("area_flag", String),
        Column("competition_id", Integer),
        Column("competition_name", String),
        Column("competition_code", String),
        Column("competition_type", String),
        #Column("competition_emblem", String),
        Column("season_id", Integer),
        Column("season_startDate", String),
        Column("season_endDate", String),
        Column("season_currentMatchday", Integer),
        Column("season_winner", String),
        Column("homeTeam_id", Integer),
        Column("homeTeam_name", String),
        Column("homeTeam_shortName", String),
        Column("homeTeam_tla", String),
        #Column("homeTeam_crest", String),
        Column("awayTeam_id", Integer),
        Column("awayTeam_name", String),
        Column("awayTeam_shortName", String),
        Column("awayTeam_tla", String),
        #Column("awayTeam_crest", String),
        Column("score_winner", String),
        Column("score_duration", String),
        Column("score_fullTime_home", Float),
        Column("score_fullTime_away", Float),
        Column("score_halfTime_home", Float),
        Column("score_halfTime_away", Float)
        #Column("odds_msg", String)
    )

    load(
        df=df_transformed,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="insert",
    )

    logger.success("Pipeline run successful")

    # print("Competition IDs:", comp_ids)
    # print(len(match_data))
