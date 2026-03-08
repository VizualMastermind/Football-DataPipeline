from jinja2 import Environment, Template
from etl_project.assets.database_extractor import (
    SqlExtractParser,
    DatabaseTableExtractor
)
from etl_project.connectors.postgresql import PostgreSqlClient
from graphlib import TopologicalSorter
import pandas as pd
from sqlalchemy import Table, MetaData


def extract_load(
    template_environment: Environment,
    source_postgresql_client: PostgreSqlClient,
    target_postgresql_client: PostgreSqlClient,
):
    """
    Perform data extraction specified in a jinja template_environment.

    Data is extracted using a source_postgresql_client, and loaded using a target_postgresql_client.
    """
    for asset in template_environment.list_templates():
        sql_extract_parser = SqlExtractParser(
            file_path=asset, environment=template_environment
        )
        database_table_extractor = DatabaseTableExtractor(
            sql_extract_parser=sql_extract_parser,
            source_postgresql_client=source_postgresql_client,
            target_postgresql_client=target_postgresql_client,
        )
        table_schema, metadata = database_table_extractor.get_table_schema()
        table_data = database_table_extractor.extract()
        target_postgresql_client.upsert_in_chunks(
            data=table_data, table=table_schema, metadata=metadata
        )


class SqlTransform:
    def __init__(
        self,
        postgresql_client: PostgreSqlClient,
        environment: Environment,
        table_name: str,
    ):
        self.postgresql_client = postgresql_client
        self.environment = environment
        self.table_name = table_name
        self.template = self.environment.get_template(f"{table_name}.sql")

    def create_table_as(self) -> None:
        """
        Drops the table if it exists and creates a new copy of the table using the provided select statement.
        """
        exec_sql = f"""
            drop table if exists {self.table_name};
            create table {self.table_name} as (
                {self.template.render()}
            )
        """
        self.postgresql_client.execute_sql(exec_sql)


def transform(dag: TopologicalSorter):
    """
    Performs `create table as` on all nodes in the provided DAG.
    """
    dag_rendered = tuple(dag.static_order())
    for node in dag_rendered:
        node.create_table_as()


# added for this project
def extract_max_incremental(
    template: Template,
    postgresql_client: PostgreSqlClient #our source is the api, target is this table we are reading from
):
    """
    Get the max value of the incremental column (specified in Jinja template) for a table

    Data is extracted using a using a postgresql_client.
    """

    jinja_config = template.module.config

    sql = f"""
        select max({jinja_config.get("incremental_column")}) as incremental_value
        from {jinja_config.get("source_table_name")}
    """
    sql_response = postgresql_client.run_sql(sql)
    sql_response[0].get("incremental_value")
