from etl_project.connectors.football_api import FootballDataAPI 
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, DateTime, Date
from etl_project.assets.football import (
    extract_competitions,
    extract_matches_full,
    transform
)
from loguru import logger
from dotenv import load_dotenv
import os
from pathlib import Path
import schedule
import time
import yaml


def run_football_pipeline(pipeline_config:dict):
    
    logger.info("Starting football pipeline run")

    logger.info("Getting football pipeline environment variables")

    # environment variables for api
    FOOTBALL_API_TOKEN = os.environ.get("FOOTBALL_API_TOKEN")

    # environment variables for database
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    FOOTBALL_DATABASE_NAME = os.environ.get("FOOTBALL_DATABASE_NAME")
    PORT = os.environ.get("PORT")

    try:
        logger.info("Creating database client")
        postgresql_client = PostgreSqlClient(
            server_name=SERVER_NAME,
            database_name=FOOTBALL_DATABASE_NAME,
            username=DB_USERNAME,
            password=DB_PASSWORD,
            port=PORT,
        )
                
        logger.info("Creating Football API Client")
        football_api_client = FootballDataAPI(api_token=FOOTBALL_API_TOKEN)

        logger.info("Extracting data from Football API")
        comp_ids = extract_competitions(football_api_client=football_api_client)

        df_football = extract_matches_full(football_api_client=football_api_client, comp_ids=comp_ids)

        logger.info("Transforming football dataframe")
        df_transformed = transform(df_football=df_football)

        logger.info("Loading football data to postgres")
        metadata = MetaData()
        table = Table(
            "matches",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("utcdate", DateTime(timezone=True)),
            Column("match_date", Date),
            Column("status", String),
            Column("matchday", Integer),
            Column("stage", String),
            Column("group", String),
            Column("lastupdated", DateTime(timezone=True)),
            #Column("referees", String),
            Column("area_id", Integer),
            Column("area_name", String),
            Column("area_code", String),
            #Column("area_flag", String),
            Column("competition_id", Integer),
            Column("competition_name", String),
            Column("competition_code", String),
            Column("competition_type", String),
            #Column("competition_emblem", String),
            Column("season_id", Integer),
            Column("season_startdate", Date),
            Column("season_enddate", Date),
            Column("season_currentmatchday", Integer),
            Column("season_winner", String),
            Column("hometeam_id", Integer),
            Column("hometeam_name", String),
            Column("hometeam_shortname", String),
            Column("hometeam_tla", String),
            #Column("hometeam_crest", String),
            Column("awayteam_id", Integer),
            Column("awayteam_name", String),
            Column("awayteam_shortname", String),
            Column("awayteam_tla", String),
            #Column("awayteam_crest", String),
            Column("score_winner", String),
            Column("score_duration", String),
            Column("score_fulltime_home", Float),
            Column("score_fulltime_away", Float),
            Column("score_halftime_home", Float),
            Column("score_halftime_away", Float),
            Column("score_regulartime_home", Float),
            Column("score_regulartime_away", Float),
            Column("score_extratime_home", Float),
            Column("score_extratime_away", Float),
            Column("score_penalties_home", Float),
            Column("score_penalties_away", Float)      
            #Column("odds_msg", String)
        )

        # align dataframe with target table schema
        df_aligned = df_transformed.reindex(columns=table.columns.keys())

        postgresql_client.upsert_in_chunks(data=df_aligned.to_dict(orient="records"), table=table, metadata=metadata)

        logger.success("Football pipeline run successful")

    except Exception as e:
        logger.error(f"Football pipeline failed with exception {e}")


if __name__ == "__main__":

    load_dotenv()

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    # set schedule
    schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
        run_football_pipeline,
        pipeline_config=pipeline_config,
    )

    while True:
        schedule.run_pending()
        time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
  
