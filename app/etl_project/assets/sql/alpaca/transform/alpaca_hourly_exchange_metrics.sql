SELECT
    trade_hour,
    exchange,
    trade_count,
    total_volume,
    total_volume_all_exchanges,
    avg_price,
    min_price,
    max_price,

    -- rolling 3-hour volume per exchange
    SUM(total_volume) OVER (
        PARTITION BY exchange
        ORDER BY trade_hour
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3hour_volume_by_exchange,

    -- rolling 3-hour total volume across all exchanges
    SUM(total_volume_all_exchanges) OVER (
        ORDER BY trade_hour
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3hour_volume_all_exchanges,

    -- price change (gap/jump) compared to previous hour per exchange
    avg_price - LAG(avg_price) OVER (
        PARTITION BY exchange
        ORDER BY trade_hour
    ) AS hourly_avg_price_change

FROM alpaca_hourly_exchange_data
