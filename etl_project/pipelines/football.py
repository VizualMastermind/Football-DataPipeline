from etl_project.connectors.football_api import FootballDataAPI 
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, DateTime, Date
from etl_project.assets.football import (
    extract_competitions,
    extract_matches_full,
    football_transform
)
from etl_project.assets.database_etl import calculate_chunk_size
from loguru import logger
from dotenv import load_dotenv
import os
from pathlib import Path
# import schedule
# import time
import yaml
import pandas as pd
import numpy as np


def run_pipeline(config: dict = None):
    
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
        # these are available with the free plan: [2013, 2016, 2021, 2001, 2018, 2015, 2002, 2019, 2003, 2017, 2152, 2014, 2000]
        #comp_ids = [2014, 2000]

        df_football = pd.DataFrame()
        df_football = extract_matches_full(football_api_client=football_api_client, comp_ids=comp_ids)

        logger.info("Transforming football dataframe")
        df_transformed = football_transform(df_football=df_football)

        logger.info("Loading football data to postgres")
        metadata = MetaData()
        table = Table(
            "matches",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("utcdate", DateTime(timezone=True)),
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
            Column("score_fulltime_home", Integer),
            Column("score_fulltime_away", Integer),
            Column("score_halftime_home", Integer),
            Column("score_halftime_away", Integer),
            Column("score_regulartime_home", Integer),
            Column("score_regulartime_away", Integer),
            Column("score_extratime_home", Integer),
            Column("score_extratime_away", Integer),
            Column("score_penalties_home", Integer),
            Column("score_penalties_away", Integer)      
            #Column("odds_msg", String)
        )

        # align dataframe with target table schema
        df_aligned = df_transformed.reindex(columns=table.columns.keys())

        # postgres doesn't like the nan values that get populated from reindex, so we will replace them with None, which it can correctly read as null
        df_aligned = df_aligned.replace(np.nan, None)
 
        chunksize = calculate_chunk_size(df_aligned)

        postgresql_client.upsert_in_chunks(data=df_aligned.to_dict(orient="records"), table=table, metadata=metadata, chunksize=chunksize)
        
        logger.success("Football pipeline run successful")

    except Exception as e:
        logger.error(f"Football pipeline failed with exception {e}")


# # if we want to this pipeline continuously, without cloud scheduler
# if __name__ == "__main__":

#     load_dotenv()

#     # get config variables
#     yaml_file_path = __file__.replace(".py", ".yaml")
#     if Path(yaml_file_path).exists():
#         with open(yaml_file_path) as yaml_file:
#             pipeline_config = yaml.safe_load(yaml_file)
#     else:
#         raise Exception(
#             f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
#         )

#     # set schedule
#     schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
#         run_pipeline,
#         config=pipeline_config.get("config"),
#     )

#     while True:
#         schedule.run_pending()
#         time.sleep(pipeline_config.get("schedule").get("poll_seconds"))


