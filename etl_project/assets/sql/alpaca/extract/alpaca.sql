{% set config = {
    "extract_type": "incremental",
    "incremental_column": "timestamp",
    "table_name": "alpaca"
} %}

select
    record_id
    timestamp
    exchange
    price
    size
from
    {{ config["source_table_name"] }}

{% if is_incremental %}
    where {{ config["incremental_column"] }} > '{{ incremental_value }}'
{% endif %}
