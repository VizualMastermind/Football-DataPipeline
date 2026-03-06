from etl_project.connectors.football_api import FootballDataAPI 
import time
from loguru import logger
from datetime import datetime, timezone


def extract_competitions(football_api_client: FootballDataAPI):
    """
    Perform extraction of all available competitions.
    """

    data = football_api_client.get_competitions()

    comp_ids = []
    for comp in data:
        comp_ids.append(comp.get("id"))

    return comp_ids

def extract_matches_full(football_api_client: FootballDataAPI, comp_ids: list[int]): #, min_datetime: str
    """
    Perform extraction of all matches for competitions, for a list of competitions
    """



    logger.info("The free API only allows 10 calls per minute, so we will stagger our calls...")

    matches = []
    for comp_id in comp_ids:
        matches.append(football_api_client.get_matches(competition_id = comp_id))
        logger.info(f"Getting match data for comp_id:{comp_id}")
        time.sleep(7) #the free api only allows 10 calls per minute, so we will stagger our calls

    return matches
