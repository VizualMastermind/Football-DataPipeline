from etl_project.connectors.football_api import FootballDataAPI 
from etl_project.assets.football import (
    extract_competitions,
    extract_matches_full
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
    API_TOKEN = os.environ.get("API_TOKEN")
    # DB_USERNAME = os.environ.get("DB_USERNAME")
    # DB_PASSWORD = os.environ.get("DB_PASSWORD")
    # SERVER_NAME = os.environ.get("SERVER_NAME")
    # DATABASE_NAME = os.environ.get("DATABASE_NAME")
    # PORT = os.environ.get("PORT")

    logger.info("Creating Football API Client")
    football_api_client = FootballDataAPI(api_token=API_TOKEN)

    logger.info("Extracting data from Football API")
    comp_ids = extract_competitions(football_api_client=football_api_client)
    match_data = extract_matches_full(football_api_client=football_api_client, comp_ids=comp_ids)
    logger.info("Data extracted from Football API")

    print("Competition IDs:", comp_ids)
    print(len(match_data))
