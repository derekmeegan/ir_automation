import os
import time
import json
import boto3
import logging
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])  # DynamoDB table name passed via env

def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

def lambda_handler(event, context):
    days_from_now = event.get('days', 7)
    today = datetime.utcnow().date()
    target_date = today + timedelta(days=int(days_from_now))
    target_str = target_date.strftime("%Y-%m-%d")
    logger.info(f"Scraping earnings for {target_str}")

    all_tables: list[pd.DataFrame] = []
    offset: int = 0
    page_size: int = 100

    # Fetch earnings data from Yahoo Finance with retries
    url = f"https://finance.yahoo.com/calendar/earnings?day={target_str}"
    max_retries = 3
    for attempt in range(1, max_retries+1):
        try:
            while True:
                url: str = (
                    f"https://finance.yahoo.com/calendar/earnings/"
                    f"?day={target_str}&offset={offset}&size={page_size}"
                )
                html_content: str = fetch_html(url)
                tables: list = pd.read_html(StringIO(html_content))
                if not tables or tables[0].empty:
                    break
                all_tables.append(tables[0])
                if len(tables[0]) < page_size:
                    break
                offset += page_size
        except Exception as e:
            logger.error(f"Attempt {attempt} failed to fetch data: {e}")
            if attempt == max_retries:
                # If all retries failed, log and stop
                logger.critical("All retries failed, aborting.")
                raise e
            time.sleep(2)
    
    if all_tables is None:
        return  # No data fetched, exit gracefully

    items = json.loads(
        pd.concat(all_tables, ignore_index=True)
        .dropna(how = 'all')
        .loc[lambda df: df["Event Name"].str.contains(r"Q\d", regex=True)]
        .loc[lambda df: df["Event Name"].str.lower().str.contains("earnings")]
        .rename(columns = {
            'Symbol': 'ticker',
            'Earnings Call Time': 'release_time',
            'Event Name': 'event_name'
        })
        .loc[lambda row: row.release_time.isin(['AMC', 'BMO'])]
        .assign(
            release_time = lambda df_: df_.release_time.map({'AMC': 'after', "BMO": "before"}),
            quarter = lambda df_: df_.event_name.copy().str.split().str[0].str[1].astype(int),
            year = lambda df_: df_.event_name.copy().str.split().str[1].astype(int),
            date = target_str,
            is_active = False
        )
        [['ticker', 'date', 'release_time', 'quarter', 'year', 'is_active']]
        .drop_duplicates(subset = ['ticker', 'date'])
        .to_json(orient = 'records')
    )
    
    # Write to DynamoDB in batches of 25
    for i in range(0, len(items), 25):
        batch: dict = {
            table.table_name: [
                {'PutRequest': {'Item': item}} for item in items[i:i+25]
            ]
        }
        response = dynamodb.meta.client.batch_write_item(RequestItems=batch)
        unprocessed: dict = response.get('UnprocessedItems', {})
        while unprocessed.get(table.table_name):
            logger.warning(f"Retrying {len(unprocessed[table.table_name])} unprocessed items...")
            response = dynamodb.meta.client.batch_write_item(RequestItems=unprocessed)
            unprocessed = response.get('UnprocessedItems', {})
    
    logger.info(f"Successfully stored earnings data for {target_str} in DynamoDB.")
