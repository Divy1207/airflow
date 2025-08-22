# Yahoo Finance → S3 → Snowflake with Metabase notify (Airflow)

This README describes an Airflow DAG that downloads AAPL price history from Yahoo Finance, writes a CSV to S3, loads it into Snowflake, then notifies Metabase to rescan the table so dashboards pick up fresh data.

## Flow
1. yfinance pulls AAPL OHLCV data for the configured PERIOD.
2. CSV is saved to s3://test-divy/airflow/AAPL.csv.
3. Data is written to DB.DATA.AAPL_STOCKS in Snowflake using write_pandas with overwrite=True.
4. Metabase is notified to discover or rescan DB.DATA.AAPL_STOCKS.

## Files
    dags/
      yahoo_to_snowflake.py
    requirements.txt
    README.md

## Requirements (requirements.txt)
    yfinance
    snowflake-connector-python[pandas]
    apache-airflow-providers-amazon
    apache-airflow-providers-snowflake
    requests

Astro Runtime includes common providers, the list above is sufficient for this DAG.

## Airflow connections

1) AWS, conn id: aws_default  
   Type: Amazon Web Services  
   Auth: access key, role, or your standard method  
   IAM actions for the bucket and key:
    - s3:PutObject on s3://test-divy/airflow/AAPL.csv
    - s3:GetObject on the same key

2) Snowflake, conn id: snowflake_conn  
   Type: Snowflake  
   Set Account, User, Password or Key Pair  
   Warehouse  
   Database: DB  
   Schema: DATA  
   Role with privileges to create and write to the target table

3) Metabase, conn id: metabase_conn  
   Type: HTTP  
   Host: your Metabase hostname (no path)  
   Schema: https  
   Port: if required  
   Extras JSON:
    {
      "database_id": 3,
      "mb_api_key": "REPLACE_WITH_SERVER_MB_API_KEY"
    }

Metabase server must have the environment variable MB_API_KEY set to the same secret you place in mb_api_key above.

## Snowflake setup (run once)

    CREATE DATABASE IF NOT EXISTS DB;
    CREATE SCHEMA IF NOT EXISTS DB.DATA;

    CREATE OR REPLACE TABLE DB.DATA.AAPL_STOCKS (
      TRADE_DATE DATE,
      OPEN FLOAT,
      HIGH FLOAT,
      LOW FLOAT,
      CLOSE FLOAT,
      VOLUME NUMBER,
      DIVIDENDS FLOAT,
      STOCK_SPLITS FLOAT
    );

    GRANT USAGE ON DATABASE DB TO ROLE <ROLE>;
    GRANT USAGE ON SCHEMA DB.DATA TO ROLE <ROLE>;
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE DB.DATA.AAPL_STOCKS TO ROLE <ROLE>;

## DAG configuration points (in dags/yahoo_to_snowflake.py)

    PERIOD      = "1y"              # for example "5y" or "max"
    S3_BUCKET   = "test-divy"
    S3_KEY      = "airflow/AAPL.csv"

    SF_DATABASE = "DB"
    SF_SCHEMA   = "DATA"
    SF_TABLE    = "AAPL_STOCKS"

DAG metadata:

    dag_id     = "yahoo_to_snowflake"
    schedule   = "@daily"
    start_date = datetime(2025, 8, 5)
    catchup    = False

## How it runs, task by task

- download_to_s3: fetches AAPL prices and uploads a CSV to S3.
- load_from_s3: reads the CSV and writes to DB.DATA.AAPL_STOCKS with overwrite=True.
- notify_metabase: calls Metabase notify endpoints so the single table is discovered or rescanned.

## Run with Astro

1) Start the environment:
    astro dev start

2) Open the Airflow UI and create the three connections listed above.

3) Trigger the DAG:
    astro dev run airflow dags trigger yahoo_to_snowflake

## Troubleshooting

- Empty Yahoo response  
  Increase PERIOD or re-run later. The task raises ValueError("No data") if empty.

- S3 permission errors  
  Confirm aws_default has PutObject and GetObject on the bucket and key.

- Snowflake auth or privilege errors  
  Check role grants, warehouse state, and that the connection targets the correct database and schema.

- Metabase notify errors  
  Ensure MB_API_KEY is set on the server, equals mb_api_key in the Airflow connection extras, and that database_id matches the Snowflake data source in Metabase.

## Notes

- The table is fully replaced each run. For incremental loads, set overwrite=False and implement a merge keyed by TRADE_DATE.
- To support other tickers, parameterize the symbol and table name or use task mapping.
