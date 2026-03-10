from etl_project.assets.football import extract_competitions, football_transform
from etl_project.connectors.football_api import FootballDataAPI 
from etl_project.assets.alpaca_markets import transform
import pytest
import pandas as pd
from pandas import Timestamp
from dotenv import load_dotenv
import os
import datetime
import numpy as np
from io import StringIO



def test_competition():
  """Unit test for the extract_competitions function. Will need to provide a football api token. Asserts that we successfully extracted the all available (free) competition ids into a list."""

  # Assemble
  load_dotenv()
  API_TOKEN = os.environ.get("FOOTBALL_API_TOKEN")
  football_api_client = FootballDataAPI(api_token=API_TOKEN)
  
  expected_competition_ids = [2013, 2016, 2021, 2001, 2018, 2015, 2002, 2019, 2003, 2017, 2152, 2014, 2000]

  actual_competition_ids = extract_competitions(football_api_client=football_api_client)
  
  assert set(expected_competition_ids) == set(actual_competition_ids)
    


def test_football_transform():
  """Unit test for the football_transform function. The football_transform function takes the output of football api matches requests and transforms it, preparing for load into database."""

  # Assemble
  test_dataframe = pd.DataFrame([
      {
          'id': 554740,
          'utcDate': '2026-01-28T22:00:00Z',
          'status': 'FINISHED',
          'matchday': 1.0,
          'stage': 'REGULAR_SEASON',
          'group': None,
          'lastUpdated': '2026-03-09T00:20:58Z',
          'referees': [{'id': 206800, 'name': 'Bruno de Araújo', 'type': 'REFEREE', 'nationality': 'Brazil'}],
          'area_id': 2032,
          'area_name': 'Brazil',
          'area_code': 'BRA',
          'area_flag': 'https://crests.football-data.org/764.svg',
          'competition_id': 2013,
          'competition_name': 'Campeonato Brasileiro Série A',
          'competition_code': 'BSA',
          'competition_type': 'LEAGUE',
          'competition_emblem': 'https://crests.football-data.org/bsa.png',
          'season_id': 2474,
          'season_startDate': '2026-01-28',
          'season_endDate': '2026-12-02',
          'season_currentMatchday': 5,
          'season_winner': np.nan,
          'homeTeam_id': 1766.0,
          'homeTeam_name': 'CA Mineiro',
          'homeTeam_shortName': 'Mineiro',
          'homeTeam_tla': 'CAM',
          'homeTeam_crest': 'https://crests.football-data.org/1766.png',
          'awayTeam_id': 1769.0,
          'awayTeam_name': 'SE Palmeiras',
          'awayTeam_shortName': 'Palmeiras',
          'awayTeam_tla': 'PAL',
          'awayTeam_crest': 'https://crests.football-data.org/1769.png',
          'score_winner': 'DRAW',
          'score_duration': 'REGULAR',
          'score_fullTime_home': 2.0,
          'score_fullTime_away': 2.0,
          'score_halfTime_home': 1.0,
          'score_halfTime_away': 1.0,
          'odds_msg': 'Activate Odds-Package in User-Panel to retrieve odds.',
          'score_regularTime_home': np.nan,
          'score_regularTime_away': np.nan,
          'score_extraTime_home': np.nan,
          'score_extraTime_away': np.nan,
          'score_penalties_home': np.nan,
          'score_penalties_away': np.nan
      },
      {
          'id': 554744,
          'utcDate': '2026-01-28T22:00:00Z',
          'status': 'FINISHED',
          'matchday': 1.0,
          'stage': 'REGULAR_SEASON',
          'group': None,
          'lastUpdated': '2026-03-09T00:20:58Z',
          'referees': [{'id': 206954, 'name': 'Paulo Zanovelli da Silva', 'type': 'REFEREE', 'nationality': 'Brazil'}],
          'area_id': 2032,
          'area_name': 'Brazil',
          'area_code': 'BRA',
          'area_flag': 'https://crests.football-data.org/764.svg',
          'competition_id': 2013,
          'competition_name': 'Campeonato Brasileiro Série A',
          'competition_code': 'BSA',
          'competition_type': 'LEAGUE',
          'competition_emblem': 'https://crests.football-data.org/bsa.png',
          'season_id': 2474,
          'season_startDate': '2026-01-28',
          'season_endDate': '2026-12-02',
          'season_currentMatchday': 5,
          'season_winner': np.nan,
          'homeTeam_id': 4241.0,
          'homeTeam_name': 'Coritiba FBC',
          'homeTeam_shortName': 'Coritiba',
          'homeTeam_tla': 'COR',
          'homeTeam_crest': 'https://crests.football-data.org/4241.png',
          'awayTeam_id': 4286.0,
          'awayTeam_name': 'RB Bragantino',
          'awayTeam_shortName': 'Bragantino',
          'awayTeam_tla': 'RBB',
          'awayTeam_crest': 'https://crests.football-data.org/4286.png',
          'score_winner': 'AWAY_TEAM',
          'score_duration': 'REGULAR',
          'score_fullTime_home': 0.0,
          'score_fullTime_away': 1.0,
          'score_halfTime_home': 0.0,
          'score_halfTime_away': 0.0,
          'odds_msg': 'Activate Odds-Package in User-Panel to retrieve odds.',
          'score_regularTime_home': np.nan,
          'score_regularTime_away': np.nan,
          'score_extraTime_home': np.nan,
          'score_extraTime_away': np.nan,
          'score_penalties_home': np.nan,
          'score_penalties_away': np.nan
      },
      {
          'id': 554746,
          'utcDate': '2026-01-28T22:00:00Z',
          'status': 'FINISHED',
          'matchday': 1.0,
          'stage': 'REGULAR_SEASON',
          'group': None,
          'lastUpdated': '2026-03-09T00:20:58Z',
          'referees': [{'id': 11237, 'name': 'Flavio Rodrigues De Souza', 'type': 'REFEREE', 'nationality': 'Brazil'}],
          'area_id': 2032,
          'area_name': 'Brazil',
          'area_code': 'BRA',
          'area_flag': 'https://crests.football-data.org/764.svg',
          'competition_id': 2013,
          'competition_name': 'Campeonato Brasileiro Série A',
          'competition_code': 'BSA',
          'competition_type': 'LEAGUE',
          'competition_emblem': 'https://crests.football-data.org/bsa.png',
          'season_id': 2474,
          'season_startDate': '2026-01-28',
          'season_endDate': '2026-12-02',
          'season_currentMatchday': 5,
          'season_winner': np.nan,
          'homeTeam_id': 6684.0,
          'homeTeam_name': 'SC Internacional',
          'homeTeam_shortName': 'Internacional',
          'homeTeam_tla': 'SCI',
          'homeTeam_crest': 'https://crests.football-data.org/6684.png',
          'awayTeam_id': 1768.0,
          'awayTeam_name': 'CA Paranaense',
          'awayTeam_shortName': 'Paranaense',
          'awayTeam_tla': 'CAP',
          'awayTeam_crest': 'https://crests.football-data.org/1768.png',
          'score_winner': 'AWAY_TEAM',
          'score_duration': 'REGULAR',
          'score_fullTime_home': 0.0,
          'score_fullTime_away': 1.0,
          'score_halfTime_home': 0.0,
          'score_halfTime_away': 1.0,
          'odds_msg': 'Activate Odds-Package in User-Panel to retrieve odds.',
          'score_regularTime_home': np.nan,
          'score_regularTime_away': np.nan,
          'score_extraTime_home': np.nan,
          'score_extraTime_away': np.nan,
          'score_penalties_home': np.nan,
          'score_penalties_away': np.nan
      }
  ])

  # could cast to correct datatypes (rather than relying on pd.dataframe's assumptions) and then remove check_dtype=False in the assert
  test_transform_expected = pd.DataFrame([
    {
        "id": 554740,
        "utcdate": pd.Timestamp("2026-01-28 22:00:00+00:00"),
        "status": "FINISHED",
        "matchday": 1,
        "stage": "REGULAR_SEASON",
        "group": None,
        "lastupdated": pd.Timestamp("2026-03-09 00:20:58+00:00"),
        "area_id": 2032,
        "area_name": "Brazil",
        "area_code": "BRA",
        "competition_id": 2013,
        "competition_name": "Campeonato Brasileiro Série A",
        "competition_code": "BSA",
        "competition_type": "LEAGUE",
        "season_id": 2474,
        "season_startdate": datetime.date(2026, 1, 28),
        "season_enddate": datetime.date(2026, 12, 2),
        "season_currentmatchday": 5,
        "season_winner": None,
        "hometeam_id": 1766,
        "hometeam_name": "CA Mineiro",
        "hometeam_shortname": "Mineiro",
        "hometeam_tla": "CAM",
        "awayteam_id": 1769,
        "awayteam_name": "SE Palmeiras",
        "awayteam_shortname": "Palmeiras",
        "awayteam_tla": "PAL",
        "score_winner": "DRAW",
        "score_duration": "REGULAR",
        "score_fulltime_home": 2,
        "score_fulltime_away": 2,
        "score_halftime_home": 1,
        "score_halftime_away": 1,
        "score_regulartime_home": pd.NA,
        "score_regulartime_away": pd.NA,
        "score_extratime_home": pd.NA,
        "score_extratime_away": pd.NA,
        "score_penalties_home": pd.NA,
        "score_penalties_away": pd.NA
    },
    {
        "id": 554744,
        "utcdate": pd.Timestamp("2026-01-28 22:00:00+00:00"),
        "status": "FINISHED",
        "matchday": 1,
        "stage": "REGULAR_SEASON",
        "group": None,
        "lastupdated": pd.Timestamp("2026-03-09 00:20:58+00:00"),
        "area_id": 2032,
        "area_name": "Brazil",
        "area_code": "BRA",
        "competition_id": 2013,
        "competition_name": "Campeonato Brasileiro Série A",
        "competition_code": "BSA",
        "competition_type": "LEAGUE",
        "season_id": 2474,
        "season_startdate": datetime.date(2026, 1, 28),
        "season_enddate": datetime.date(2026, 12, 2),
        "season_currentmatchday": 5,
        "season_winner": None,
        "hometeam_id": 4241,
        "hometeam_name": "Coritiba FBC",
        "hometeam_shortname": "Coritiba",
        "hometeam_tla": "COR",
        "awayteam_id": 4286,
        "awayteam_name": "RB Bragantino",
        "awayteam_shortname": "Bragantino",
        "awayteam_tla": "RBB",
        "score_winner": "AWAY_TEAM",
        "score_duration": "REGULAR",
        "score_fulltime_home": 0,
        "score_fulltime_away": 1,
        "score_halftime_home": 0,
        "score_halftime_away": 0,
        "score_regulartime_home": pd.NA,
        "score_regulartime_away": pd.NA,
        "score_extratime_home": pd.NA,
        "score_extratime_away": pd.NA,
        "score_penalties_home": pd.NA,
        "score_penalties_away": pd.NA
    },
    {
        "id": 554746,
        "utcdate": pd.Timestamp("2026-01-28 22:00:00+00:00"),
        "status": "FINISHED",
        "matchday": 1,
        "stage": "REGULAR_SEASON",
        "group": None,
        "lastupdated": pd.Timestamp("2026-03-09 00:20:58+00:00"),
        "area_id": 2032,
        "area_name": "Brazil",
        "area_code": "BRA",
        "competition_id": 2013,
        "competition_name": "Campeonato Brasileiro Série A",
        "competition_code": "BSA",
        "competition_type": "LEAGUE",
        "season_id": 2474,
        "season_startdate": datetime.date(2026, 1, 28),
        "season_enddate": datetime.date(2026, 12, 2),
        "season_currentmatchday": 5,
        "season_winner": None,
        "hometeam_id": 6684,
        "hometeam_name": "SC Internacional",
        "hometeam_shortname": "Internacional",
        "hometeam_tla": "SCI",
        "awayteam_id": 1768,
        "awayteam_name": "CA Paranaense",
        "awayteam_shortname": "Paranaense",
        "awayteam_tla": "CAP",
        "score_winner": "AWAY_TEAM",
        "score_duration": "REGULAR",
        "score_fulltime_home": 0,
        "score_fulltime_away": 1,
        "score_halftime_home": 0,
        "score_halftime_away": 1,
        "score_regulartime_home": pd.NA,
        "score_regulartime_away": pd.NA,
        "score_extratime_home": pd.NA,
        "score_extratime_away": pd.NA,
        "score_penalties_home": pd.NA,
        "score_penalties_away": pd.NA
    }
  ])
  
  test_transform = football_transform(test_dataframe)

  #assert
  pd.testing.assert_frame_equal(left=test_transform, right=test_transform_expected, check_dtype=False) 



def test_alpaca_transform():
  """Unit test for the transform function in the alpaca assets file. The tranform function a dataframe of api response data, as well as a dataframe with exchange codes (read from csv)."""

  # Assemble
  csv_data = """exchange_code,exchange_name
  A,NYSE American (AMEX)
  B,NASDAQ OMX BX
  C,National Stock Exchange
  D,FINRA ADF
  E,Market Independent
  H,MIAX
  I,International Securities Exchange
  J,Cboe EDGA
  K,Cboe EDGX
  L,Long Term Stock Exchange
  M,Chicago Stock Exchange
  N,New York Stock Exchange
  P,NYSE Arca
  Q,NASDAQ OMX
  S,NASDAQ Small Cap
  T,NASDAQ Int
  U,Members Exchange
  V,IEX
  W,CBOE
  X,NASDAQ OMX PSX
  Y,Cboe BYX
  Z,Cboe BZX
  """

  df_exchange_codes = pd.read_csv(StringIO(csv_data), skipinitialspace=True)
  
  #df_exchange_codes = pd.read_csv("etl_project\data\exchange_codes.csv")

  test_dataframe = pd.DataFrame([
    {
      "c": [" ", "9"],
      "i": 52983811626570,
      "p": 17.65,
      "s": 0,
      "t": "2026-03-06T00:00:00.003559894Z",
      "x": "N",
      "z": "A"
    },
    {
      "c": [" ", "M"],
      "i": 52983811626571,
      "p": 17.65,
      "s": 26022,
      "t": "2026-03-06T00:00:00.003562919Z",
      "x": "N",
      "z": "A"
    },
    {
      "c": [" ", "T", "I"],
      "i": 52983678186037,
      "p": 17.78,
      "s": 1,
      "t": "2026-03-06T00:01:05.974261529Z",
      "x": "P",
      "z": "A"
    }
  ])


  test_transform_expected = pd.DataFrame([
    {
      "price": 17.65,
      "size": 0,
      "timestamp": "2026-03-06T00:00:00.003559894Z",
      "exchange": "New York Stock Exchange"
    },
    {
      "price": 17.65,
      "size": 26022,
      "timestamp": "2026-03-06T00:00:00.003562919Z",
      "exchange": "New York Stock Exchange"
    },
    {
      "price": 17.78,
      "size": 1,
      "timestamp": "2026-03-06T00:01:05.974261529Z",
      "exchange": "NYSE Arca"
    }
  ])

  test_transform = transform(df=test_dataframe, df_exchange_codes=df_exchange_codes)

  #assert
  pd.testing.assert_frame_equal(left=test_transform, right=test_transform_expected)


if __name__ == "__main__":
    
  test_football_transform()
  test_competition()
  test_alpaca_transform()
