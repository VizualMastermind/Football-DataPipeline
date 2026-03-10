# Football-DataPipeline

## About
A data pipeline using data from [football-data API](https://www.football-data.org/) and [Alpaca](https://alpaca.markets/) API.


## Objective:

Develop an ETL pipeline to process live sports data via football API. Another data pipeline collecting live stock data from Alpaca markets on Manchester United PLC (MANU) runs in parallel. 

## Technical Highlights:

- Built Python Pipelines to extract real time data from football-data API and Alpaca Markets's API.
- Loaded the extracted data in chunks via Upsert to Amazon RDS database.
- Scheduled Python Pipelines to run on a schedule and parallel via YAML and schedule library.
- Extract raw data from PostgreSQL database and further process it with a PostgreSQL transform making a total of 3 tables.
- Implement unit testing for two functions on the Football pipeline.

## Technologies:
- AWS, ECR, ECS, EventBridge, Docker, Amazon RDS Postgres, Python, CI/CD (Github), SQL, YAML, Pytest

## Architecture 

![Architecute.png](diagrams/architecturev2.drawio.png)

## Pipeline

![Markdown Logo](diagrams/pipeline.png)
