
--Creating table from Alpaca_Stats_1.sql

CREATE TABLE manu_trades AS
    SELECT *
    FROM max_min_price;
    where symbol = "MANU"
    order by trade_date desc;

