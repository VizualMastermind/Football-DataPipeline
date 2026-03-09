from dotenv import load_dotenv
import yaml
from pathlib import Path
# import schedule
# import time
from etl_project.connectors.postgresql import PostgreSqlClient
from importlib import import_module

if __name__ == "__main__":
    # set up environment variables
    load_dotenv()

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            multi_pipeline_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    for pipeline_config in multi_pipeline_config.get("pipelines"):
        pipeline_name = pipeline_config.get("name")
        module = import_module(
            name=f".{pipeline_name}", package="etl_project.pipelines"
        )
        module.run_pipeline(config=pipeline_config.get("config"))   

    # # if we want to run both pipelines continuously, without cloud scheduler
    # for pipeline_config in multi_pipeline_config.get("pipelines"):
    #     pipeline_name = pipeline_config.get("name")
    #     module = import_module(
    #         name=f".{pipeline_name}", package="etl_project.pipelines"
    #     )
    #     schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
    #         module.run_pipeline,
    #         config=pipeline_config.get("config"),
    #     )

    # while True:
    #     schedule.run_pending()
    #     time.sleep(multi_pipeline_config.get("poll_seconds"))
