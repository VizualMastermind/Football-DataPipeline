import requests
from typing import List, Dict


class FootballDataAPI:
    BASE_URL = "http://api.football-data.org/v4"

    def __init__(self, api_token: str):
        """
        Initialize the API client with a personal API token.

        Args:
            api_token: Your football-data.org API key.

        Raises:
            Exception if no API token is provided.
        """
        if api_token is None:
            raise Exception("API token cannot be set to None.")
        self.api_key = api_token
        
        self.headers = {
            "X-Auth-Token": api_token,
            "X-Unfold-Goals": "true"
        }

    def get_competitions(self) -> List[int]:
        """
        Get all available competition IDs and available info, including name, type, seasons avilable, etc.. If using the free API plan, there are 12 competitions available.

        Args:

        Returns:
            list of all available competitions and info.

        Raises:
            requests.exceptions.HTTPError: If the API response code is not 200.
        """
        url = f"{self.BASE_URL}/competitions"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        data = response.json()
    
        competitions = data.get("competitions")
        return competitions

    def get_matches(self, competition_id: int) -> List[Dict]:
        """
        Get all matches for a specific competition.

        Args:
            competition_id: ID of the competition to fetch matches for.

        Returns:
            List of match dictionaries for the given competition.

        Raises:
            requests.exceptions.HTTPError: If the API response code is not 200.
        """
        url = f"{self.BASE_URL}/competitions/{competition_id}/matches"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        data = response.json()

        matches = data.get("matches")

        return matches