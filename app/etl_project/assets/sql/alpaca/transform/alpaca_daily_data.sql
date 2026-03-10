SELECT
    date(timestamp::timestamp) AS trade_date,
    COUNT(*) AS trade_count,
    SUM(size) AS total_volume,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM alpaca_manu
GROUP BY date(timestamp::timestamp)
