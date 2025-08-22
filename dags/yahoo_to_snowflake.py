from datetime import datetime
from io import StringIO

import pandas as pd
import yfinance as yf
import requests

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.hooks.base import BaseHook
from snowflake.connector.pandas_tools import write_pandas

PERIOD = "1y"
S3_BUCKET = "test-divy"
S3_KEY = "airflow/AAPL.csv"

SF_DATABASE = "DB"
SF_SCHEMA = "DATA"
SF_TABLE = "AAPL_STOCKS"

@dag(
    dag_id="yahoo_to_snowflake",
    schedule="@daily",
    start_date=datetime(2025, 8, 5),
    catchup=False,
    tags=["example"],
)
def _pipeline():
    @task
    def download_to_s3() -> str:
        df = yf.Ticker("AAPL").history(period=PERIOD).reset_index()
        if df.empty:
            raise ValueError("No data")
        df.columns = ["TRADE_DATE" if c == "Date" else c.upper().replace(" ", "_") for c in df.columns]
        S3Hook("aws_default").load_string(df.to_csv(index=False), S3_KEY, S3_BUCKET, replace=True)
        return S3_KEY

    @task
    def load_from_s3(key: str):
        s3_obj = S3Hook("aws_default").get_key(key, S3_BUCKET)
        df = pd.read_csv(StringIO(s3_obj.get()["Body"].read().decode("utf-8")))
        if df.empty:
            raise ValueError("Empty CSV")
        conn = SnowflakeHook("snowflake_conn").get_conn()
        write_pandas(
            conn,
            df,
            table_name=SF_TABLE,
            database=SF_DATABASE,
            schema=SF_SCHEMA,
            overwrite=True,
            quote_identifiers=False,
        )

    @task
    def notify_metabase():
        mb = BaseHook.get_connection("metabase_conn")
        base = f"{mb.schema}://{mb.host}" + (f":{mb.port}" if mb.port else "")
        extras = mb.extra_dejson or {}
        db_id = int(extras["database_id"])
        api_key = extras["mb_api_key"]
        headers = {"Content-Type": "application/json", "X-Metabase-Apikey": api_key}
        try:
            requests.post(
                f"{base}/api/notify/db/{db_id}/new-table",
                headers=headers,
                json={"schema_name": SF_SCHEMA, "table_name": SF_TABLE, "synchronous": True},
                timeout=30,
            )
        except Exception:
            pass
        r = requests.post(
            f"{base}/api/notify/db/{db_id}",
            headers=headers,
            json={"scan": "schema", "table_name": f"{SF_SCHEMA}.{SF_TABLE}", "synchronous": True},
            timeout=60,
        )
        r.raise_for_status()

    load_from_s3(download_to_s3()) >> notify_metabase()

dag = _pipeline()
