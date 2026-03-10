import requests


class AlpacaMarketsApiClient:
    def __init__(self, api_key_id: str, api_secret_key: str):
        self.base_url = "https://data.alpaca.markets/v2/stocks"
        if api_key_id is None:
            raise Exception("API key cannot be set to None.")
        self.api_key_id = api_key_id
        if api_secret_key is None:
            raise Exception("API secret key cannot be set to None.")
        self.api_secret_key = api_secret_key

    def get_trades(
        self, stock_ticker: str, start_time: str, end_time: str = None
    ) -> list[dict]:
        """
        Get the trades data for a specific stock ticker.

        Args:
            stock_ticker: the ticker for the stock
            start_time: start time in isoformat
            end_time: end time in isoformat

        Returns:
            A list of trades for a given stock ticket between the start and end times

        Raises:
            Exception if response code is not 200.
        """
        
        url = f"{self.base_url}/{stock_ticker}/trades"
        params = {"start": start_time, "end": end_time}
        headers = {
            "APCA-API-KEY-ID": self.api_key_id,
            "APCA-API-SECRET-KEY": self.api_secret_key,
        }

        # response = requests.get(url=url, params=params, headers=headers)
        # if response.status_code == 200 and response.json().get("trades") is not None:
        #     return response.json().get("trades")
        # else:
        #     raise Exception(
        #         f"Failed to extract data from Alpaca API. Status Code: {response.status_code}. Response: {response.text}"
        #     )
        

        data = []
 
        # modified for project
        while True:
            response = requests.get(url=url, params=params, headers=headers)

            if response.status_code != 200:
                raise Exception(
                    f"Failed to extract data from Alpaca API."
                    f"Status Code: {response.status_code}. Response: {response.text}"
                )

            iteration_data = response.json()

            if iteration_data.get("trades"):
                data.extend(iteration_data["trades"])

            next_page_token = iteration_data.get("next_page_token")

            if not next_page_token:
                break

            params["page_token"] = next_page_token

        return data
