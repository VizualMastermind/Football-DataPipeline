from etl_project.assets.football import extract_competitions, extract_matches_full, football_transform
from etl_project.connectors.football_api import FootballDataAPI 
import pytest
import pandas as pd
from dotenv import load_dotenv
import os
import datetime
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, DateTime, Date



def test_competition():
    
    # Assemble
    format = """%b %d %Y %I:%M%p"""
    load_dotenv()
    API_TOKEN = os.environ.get("API_TOKEN")
    football_api_client = FootballDataAPI(api_token=API_TOKEN)
    
    test_competition_ids = [{'id': 2013, 'area': {'id': 2032, 
                                                 'name': 'Brazil', 'code': 'BRA', 
                                                  'flag': 'https://crests.football-data.org/764.svg'}, 
                              'name': 'Campeonato Brasileiro Série A', 
                              'code': 'BSA', 
                              'type': 'LEAGUE', 
                              'emblem': 'https://crests.football-data.org/bsa.png', 
                              'plan': 'TIER_ONE', 
                               'currentSeason': {'id': 2474, 
                                                'startDate': '2026-01-28', 
                                                'endDate': '2026-12-02', 
                                                 'currentMatchday': 5, 
                                                 'winner': None}, 
                                'numberOfAvailableSeasons': 10, 
                                'lastUpdated': '2024-09-13T16:55:53Z'}]
    


    # Act
    actual_competition_ids = extract_competitions(football_api_client=football_api_client)
    
    assert (test_competition_ids[0]['id'] in actual_competition_ids)
    


def test_football_transform():
    # Assemble

    test_dataframe = pd.DataFrame(
        [
      {
        "id": 540712,
        "utcDate": pd.to_datetime('2025-08-08 19:00:00+00:00'),
        "status": "FINISHED",
        "matchday": 1,
        "stage": "REGULAR_SEASON",
        "group": "",
        "lastUpdated": "2026-03-07 00:20:58+00:00",
        "area_id": 2072,
        "area_name": "England",
        "area_code": "ENG",
        "competition_id": 2016,
        "competition_name": "Championship",
        "competition_code": "ELC",
        "competition_type": "LEAGUE",
        "season_id": 2416,
        "season_startDate": "8/8/2025",
        "season_endDate": "12/29/2026",
        "season_currentmatchday": 36,
        "season_winner": "",
        "hometeam_id": 332,
        "hometeam_name": "Birmingham City FC",
        "hometeam_shortname": "Birmingham",
        "hometeam_tla": "BIR",
        "awayteam_id": 349,
        "awayteam_name": "Ipswich Town FC",
        "awayteam_shortname": "Ipswich Town",
        "awayteam_tla": "IPS",
        "score_winner": "DRAW",
        "score_duration": "REGULAR",
        "score_fulltime_home": 1,
        "score_fulltime_away": 1,
        "score_halftime_home": 0,
        "score_halftime_away": 0,
        "score_regulartime_home": "",
        "score_regulartime_away": "",
        "score_extratime_home": "",
        "score_extratime_away": "",
        "score_penalties_home": "",
        "score_penalties_away": ""
      },
      {
        "id": 540713,
        "utcdate": pd.to_datetime('2025-08-09 11:30:00+00:00'),
        "status": "FINISHED",
        "matchday": 1,
        "stage": "REGULAR_SEASON",
        "group": "",
        "lastUpdated": "2026-03-07 00:20:58+00:00",
        "area_id": 2072,
        "area_name": "England",
        "area_code": "ENG",
        "competition_id": 2016,
        "competition_name": "Championship",
        "competition_code": "ELC",
        "competition_type": "LEAGUE",
        "season_id": 2416,
        "season_startdate": "8/8/2025",
        "season_enddate": "12/29/2026",
        "season_currentmatchday": 36,
        "season_winner": "",
        "hometeam_id": 348,
        "hometeam_name": "Charlton Athletic FC",
        "hometeam_shortname": "Charlton",
        "hometeam_tla": "CHA",
        "awayteam_id": 346,
        "awayteam_name": "Watford FC",
        "awayteam_shortname": "Watford",
        "awayteam_tla": "WAT",
        "score_winner": "HOME_TEAM",
        "score_duration": "REGULAR",
        "score_fulltime_home": 1,
        "score_fulltime_away": 0,
        "score_halftime_home": 0,
        "score_halftime_away": 0,
        "score_regulartime_home": "",
        "score_regulartime_away": "",
        "score_extratime_home": "",
        "score_extratime_away": "",
        "score_penalties_home": "",
        "score_penalties_away": ""
      },
      {
        "id": 540714,
        "utcDate": pd.to_datetime('2025-08-09 11:30:00+00:00'),
        "status": "FINISHED",
        "matchday": 1,
        "stage": "REGULAR_SEASON",
        "group": "",
        "lastupdated": "2026-03-07 00:20:58+00:00",
        "area_id": 2072,
        "area_name": "England",
        "area_code": "ENG",
        "competition_id": 2016,
        "competition_name": "Championship",
        "competition_code": "ELC",
        "competition_type": "LEAGUE",
        "season_id": 2416,
        "season_startdate": "8/8/2025",
        "season_enddate": "12/29/2026",
        "season_currentmatchday": 36,
        "season_winner": "",
        "hometeam_id": 1076,
        "hometeam_name": "Coventry City FC",
        "hometeam_shortname": "Coventry City",
        "hometeam_tla": "COV",
        "awayteam_id": 322,
        "awayteam_name": "Hull City AFC",
        "awayteam_shortname": "Hull City",
        "awayteam_tla": "HUL",
        "score_winner": "DRAW",
        "score_duration": "REGULAR",
        "score_fulltime_home": 0,
        "score_fulltime_away": 0,
        "score_halftime_home": 0,
        "score_halftime_away": 0,
        "score_regulartime_home": "",
        "score_regulartime_away": "",
        "score_extratime_home": "",
        "score_extratime_away": "",
        "score_penalties_home": "",
        "score_penalties_away": ""
      }
    ]
    )

    test_aligned_df = pd.DataFrame(
        [
      {
        #"": 0,
        "id": 540712,
        "utcDate": "2025-08-08 19:00:00+00:00",
        "status": "FINISHED",
        "matchday": 1,
        "stage": "REGULAR_SEASON",
        "group": "",
        "lastupdated": "2026-03-07 00:20:58+00:00",
        "area_id": 2072,
        "area_name": "England",
        "area_code": "ENG",
        "competition_id": 2016,
        "competition_name": "Championship",
        "competition_code": "ELC",
        "competition_type": "LEAGUE",
        "season_id": 2416,
        "season_startdate": "8/8/2025",
        "season_enddate": "12/29/2026",
        "season_currentmatchday": 36,
        "season_winner": "",
        "hometeam_id": 332,
        "hometeam_name": "Birmingham City FC",
        "hometeam_shortname": "Birmingham",
        "hometeam_tla": "BIR",
        "awayteam_id": 349,
        "awayteam_name": "Ipswich Town FC",
        "awayteam_shortname": "Ipswich Town",
        "awayteam_tla": "IPS",
        "score_winner": "DRAW",
        "score_duration": "REGULAR",
        "score_fulltime_home": 1,
        "score_fulltime_away": 1,
        "score_halftime_home": 0,
        "score_halftime_away": 0,
        "score_regulartime_home": "",
        "score_regulartime_away": "",
        "score_extratime_home": "",
        "score_extratime_away": "",
        "score_penalties_home": "",
        "score_penalties_away": ""
      },
      {
        #"": 1,
        "id": 540713,
        "utcDate": "2025-08-09 11:30:00+00:00",
        "status": "FINISHED",
        "matchday": 1,
        "stage": "REGULAR_SEASON",
        "group": "",
        "lastupdated": "2026-03-07 00:20:58+00:00",
        "area_id": 2072,
        "area_name": "England",
        "area_code": "ENG",
        "competition_id": 2016,
        "competition_name": "Championship",
        "competition_code": "ELC",
        "competition_type": "LEAGUE",
        "season_id": 2416,
        "season_startdate": "8/8/2025",
        "season_enddate": "12/29/2026",
        "season_currentmatchday": 36,
        "season_winner": "",
        "hometeam_id": 348,
        "hometeam_name": "Charlton Athletic FC",
        "hometeam_shortname": "Charlton",
        "hometeam_tla": "CHA",
        "awayteam_id": 346,
        "awayteam_name": "Watford FC",
        "awayteam_shortname": "Watford",
        "awayteam_tla": "WAT",
        "score_winner": "HOME_TEAM",
        "score_duration": "REGULAR",
        "score_fulltime_home": 1,
        "score_fulltime_away": 0,
        "score_halftime_home": 0,
        "score_halftime_away": 0,
        "score_regulartime_home": "",
        "score_regulartime_away": "",
        "score_extratime_home": "",
        "score_extratime_away": "",
        "score_penalties_home": "",
        "score_penalties_away": ""
      }
    ]
    )

    

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

    actual_aligned_df = football_transform(test_dataframe)
    #assert (test_dataframe in actual_aligned_df)

    pd.testing.assert_frame_equal(left=actual_aligned_df, right=test_aligned_df, check_exact= True)
