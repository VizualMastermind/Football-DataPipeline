from dotenv import load_dotenv
import os
import pandas as pd
from etl_project.connectors.alpaca_markets import AlpacaMarketsApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, Numeric, BigInteger
from etl_project.assets.alpaca_markets import (
    extract_alpaca_markets,
    extract_exchange_codes,
    transform
)
import yaml
from pathlib import Path
# import schedule
# import time
from loguru import logger
from jinja2 import Environment, FileSystemLoader
from etl_project.assets.database_etl import (
    extract_max_incremental,
    SqlTransform,
    dag_transform
)
from graphlib import TopologicalSorter


def run_pipeline(config: dict):

    logger.info("Starting alpaca pipeline run")

    # set up environment variables
    logger.info("Getting pipeline environment variables")
    ALPACA_API_KEY_ID = os.environ.get("ALPACA_API_KEY_ID")
    ALPACA_API_SECRET_KEY = os.environ.get("ALPACA_API_SECRET_KEY")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    ALPACA_DATABASE_NAME = os.environ.get("ALPACA_DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    try: 
        logger.info("Creating database client")        
        postgresql_client = PostgreSqlClient(
            server_name=SERVER_NAME,
            database_name=ALPACA_DATABASE_NAME,
            username=DB_USERNAME,
            password=DB_PASSWORD,
            port=PORT,
        )

        logger.info("Creating Alpaca Markets API client")
        alpaca_markets_client = AlpacaMarketsApiClient(
            api_key_id=ALPACA_API_KEY_ID, api_secret_key=ALPACA_API_SECRET_KEY
        )

        # environment for jinja templates to extract incremental values
        extract_template_environment = Environment(
            loader=FileSystemLoader(
                config.get("extract_template_path")
            )
        )
        
        template = extract_template_environment.get_template("alpaca.sql")
        jinja_config = template.module.config

        if postgresql_client.table_exists(jinja_config.get("table_name")) and jinja_config.get("extract_type") == "incremental":
            incremental_max = extract_max_incremental(template=template, postgresql_client=postgresql_client)
            start_date = (pd.Timestamp(incremental_max) + pd.Timedelta(nanoseconds = 1)).isoformat().replace("+00:00", "Z")        
        else:
            start_date = config.get("start_date")

        # get max incremental value
        logger.info("Extracting data from Alpaca API and CSV files")
        df_alpaca_markets = extract_alpaca_markets(
            alpaca_markets_client=alpaca_markets_client,
            stock_ticker=config.get("stock_ticker"),
            start_date=start_date
        )

        df_exchange_codes = extract_exchange_codes(
            exchange_codes_path=config.get("exchange_codes_path")
        )

        if df_alpaca_markets.empty:
            logger.success("No new data from Alpaca API. Ending Alpaca pipeline.")
        else:
            # transform
            logger.info("Transforming alpaca dataframes")
            df_transformed = transform(
                df=df_alpaca_markets, df_exchange_codes=df_exchange_codes
            )

            # load
            logger.info("Loading alpaca data to postgres")
            metadata = MetaData()
            table = Table(
                "alpaca",
                metadata,
                #Column("id", Numeric(20)),
                Column("record_id", BigInteger, primary_key=True, autoincrement=True),  # unique ID auto-generated
                Column("timestamp", String),
                Column("exchange", String),
                Column("price", Float),
                Column("size", BigInteger),
            )

            postgresql_client.upsert_in_chunks(data=df_transformed.to_dict(orient="records"), table=table, metadata=metadata)
            logger.info(f"Loaded {len(df_alpaca_markets)} new alpaca api records to database.")

            # transform (again)
            logger.info("Creating dependent transformation tables in database")
                
            # environment for jinja templates for tranformation tables
            extract_template_environment = Environment(
                loader=FileSystemLoader(
                    config.get("transform_template_path")
                )
            )

            serving_sales_cumulative = SqlTransform(
                postgresql_client=postgresql_client,
                environment=extract_template_environment,
                table_name="alpaca_stats_1",
            )
            serving_sales_month_end = SqlTransform(
                postgresql_client=postgresql_client,
                environment=extract_template_environment,
                table_name="alpaca_stats_2",
            )

            dag = TopologicalSorter()
            dag.add(serving_sales_cumulative)
            dag.add(serving_sales_month_end, serving_sales_cumulative)

            dag_transform(dag)

            logger.success(f"Alpaca pipeline run successful.")

    except Exception as e:
        logger.error(f"Alpaca pipeline failed with exception {e}")



# # if we want to this pipeline continuously, without cloud scheduler
# if __name__ == "__main__":
#     # set up environment variables
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

