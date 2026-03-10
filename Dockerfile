FROM python:3.9-slim

WORKDIR /app

COPY /app .

RUN pip install -r requirements.txt

ENV FOOTBALL_API_TOKEN=<football_api_token>

ENV ALPACA_API_KEY_ID=<alpaca_api_key_id>
ENV ALPACA_API_SECRET_KEY=Go8kTPovdwbiNDg19FX9Sv3Tc9aAmb2sdPXkjrGBmQzF

ENV DB_USERNAME=postgres
ENV DB_PASSWORD=<db_password>
ENV SERVER_NAME=<server_name>
ENV PORT=5432

ENV FOOTBALL_DATABASE_NAME=football_data
ENV ALPACA_DATABASE_NAME=football_data

CMD ["python", "-m", "etl_project.pipelines.run"]