import pandas as pd
from etl_project.connectors.alpaca_markets import AlpacaMarketsApiClient
from pathlib import Path
from datetime import datetime, timezone, timedelta


# def _generate_datetime_ranges(
#     start_date: str,
#     end_date: str,
# ) -> list[dict[str, datetime]]:
#     """
#     Generates a range of datetime ranges.

#     Usage example:
#         _generate_datetime_ranges(start_date="2020-01-01", end_date="2020-01-03")

#     Returns:
#             [
#                 {'start_time': datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc), 'end_time': datetime(2020, 1, 2, 0, 0, 0, tzinfo=timezone.utc)},
#                 {'start_time': datetime(2020, 1, 2, 0, 0, 0, tzinfo=timezone.utc), 'end_time': datetime(2020, 1, 3, 0, 0, 0, tzinfo=timezone.utc)}
#             ]

#     Args:
#         start_date: provide a str with the format "yyyy-mm-dd"
#         end_date: provide a str with the format "yyyy-mm-dd"

#     Returns:
#         A list of dictionaries with datetime objects

#     Raises:
#         Exception when incorrect input date string format is provided.
#     """

#     date_range = []
#     if start_date is not None and end_date is not None:
#         raw_start_time = datetime.strptime(start_date, "%Y-%m-%d")
#         raw_end_time = datetime.strptime(end_date, "%Y-%m-%d")
#         start_time = datetime(
#             year=raw_start_time.year,
#             month=raw_start_time.month,
#             day=raw_start_time.day,
#             hour=raw_start_time.hour,
#             minute=raw_start_time.minute,
#             second=raw_start_time.second,
#             tzinfo=timezone.utc,
#         )
#         end_time = datetime(
#             year=raw_end_time.year,
#             month=raw_end_time.month,
#             day=raw_end_time.day,
#             hour=raw_end_time.hour,
#             minute=raw_end_time.minute,
#             second=raw_end_time.second,
#             tzinfo=timezone.utc,
#         )
#         date_range = [
#             {
#                 "start_time": (start_time + timedelta(days=i)),
#                 "end_time": (start_time + timedelta(days=i) + timedelta(days=1)),
#             }
#             for i in range((end_time - start_time).days)
#         ]
#     else:
#         raise Exception(
#             "Please provide valid dates `YYYY-MM-DD` for start_date and end_date."
#         )
#     return date_range


def extract_alpaca_markets(
    alpaca_markets_client: AlpacaMarketsApiClient,
    stock_ticker: str,
    start_date: str,
    end_date: str = None
) -> pd.DataFrame:
    """
    Perform extraction using a filepath which contains a list of cities.
    """

    data = []

    # for dates in _generate_datetime_ranges(start_date=start_date, end_date=end_date):
    #     data.extend(
    #         alpaca_markets_client.get_trades(
    #             stock_ticker=stock_ticker,
    #             start_time=dates.get("start_time").isoformat(),
    #             end_time=dates.get("end_time").isoformat(),
    #         )
    #     )

    # modified for project
    data.extend(
        alpaca_markets_client.get_trades(
            stock_ticker=stock_ticker,
            start_time=start_date, # Format: RFC-3339 or YYYY-MM-DD
            end_time=end_date # Format: RFC-3339 or YYYY-MM-DD (or None)
        )
    )       

    df = pd.json_normalize(data=data, meta=["symbol"])
    return df


def extract_exchange_codes(exchange_codes_path: Path) -> pd.DataFrame:
    """Extracts data from the exchange codes file"""
    df = pd.read_csv(exchange_codes_path)
    return df


# modified for this project
def transform(df: pd.DataFrame, df_exchange_codes: pd.DataFrame) -> pd.DataFrame:
    """Performs transformation on dataframe produced from extract() function."""

    df_quotes_renamed = df.rename(
        columns={
            "i": "id",
            "t": "timestamp",
            "x": "exchange",
            "p": "price",
            "s": "size",
            "c": "condition_flags",
            "z": "exchange_group",
            "u": "update"
        }
    )

    keep_cols = ["timestamp", "exchange", "price", "size"]
    df_quotes_renamed.drop(columns=[col for col in df_quotes_renamed.columns if col not in keep_cols], inplace=True)

    # for padding timestamp so it is in nanosecond format - api truncates trailing 0s
    df_quotes_renamed.loc[:, 'timestamp'] = pd.to_datetime(df_quotes_renamed['timestamp'], utc=True)
    df_quotes_renamed.loc[:, 'timestamp'] = df_quotes_renamed['timestamp'].apply(
        lambda ts: f"{ts.strftime('%Y-%m-%dT%H:%M:%S.%f')}{ts.nanosecond % 1000:03d}Z"
    )

    df_exchange = (
        pd.merge(
            left=df_quotes_renamed,
            right=df_exchange_codes,
            left_on="exchange",
            right_on="exchange_code",
        )
        .drop(columns=["exchange_code", "exchange"])
        .rename(columns={"exchange_name": "exchange"})
    )
    return df_exchange


