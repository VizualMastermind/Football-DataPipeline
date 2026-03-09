{% set config = {
    "extract_type": "incremental",
    "incremental_column": "timestamp",
    "table_name": "alpaca"
} %}

-- select
--     record_id
--     timestamp
--     exchange
--     price
--     size
-- from
--     {{ config["source_table_name"] }}


-- select max({{config["incremental_column"]}}) as incremental_value
-- from {{config["table_name"]}}