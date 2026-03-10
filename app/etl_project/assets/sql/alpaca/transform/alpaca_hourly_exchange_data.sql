SELECT
    date_trunc('hour', timestamp::timestamp) AS trade_hour,  -- truncate timestamp to the start of the hour
    exchange,
    COUNT(*) AS trade_count,
    SUM(size) AS total_volume,
    SUM(SUM(size)) OVER (PARTITION BY date_trunc('hour', timestamp::timestamp)) AS total_volume_all_exchanges,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM alpaca_manu
GROUP BY date_trunc('hour', timestamp::timestamp), exchange
