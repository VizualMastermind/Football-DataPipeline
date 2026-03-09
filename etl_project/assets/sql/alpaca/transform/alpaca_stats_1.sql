
--Transforming stock price data from alpaca file


--Max/Min Stock Price & Dates for MANU

with manu_data as (
    select
        id as ID,
        exchange as Exchange,
        timestamp,
        price as Price,
        size as Size
    from alpaca
    where symbol = "MANU"
),
    
    max_min_price as (
        select 
            id,
            exchange,
            timestamp,
            date(timestamp) as trade_date,
            price,
            size,
            max(price) over (partition by date(Timestamp)) as daily_high,
            min(price) over (partition by date(Timestamp)) as daily_low
    from manu_data
)

select *
from max_min_price
order by timestamp;




