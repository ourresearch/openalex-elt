import datetime
import requests
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("CrossrefDataIngestion") \
    .getOrCreate()


def fetch_recent_crossref_data(yesterday, cursor=None):
    url = "https://api.crossref.org/works"
    params = {
        "filter": f"from-index-date:{yesterday},until-index-date:{yesterday}",
        "rows": 100,
        "cursor": cursor if cursor else "*",
        "sort": "indexed",
        "order": "desc"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def crossref_raw_data():
    cursor = None
    page = 1
    total_rows = 0
    max_rows = 2000
    batch_size = 500

    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    # Define schema for the Crossref data
    raw_data_schema = StructType([
        StructField("doi", StringType(), True),
        StructField("message", StringType(), True)
    ])

    batch_data = []

    while total_rows < max_rows:
        data = fetch_recent_crossref_data(yesterday, cursor)
        items = data.get("message", {}).get("items", [])
        rows_this_page = len(items)

        if rows_this_page == 0:
            logger.info("No more items to fetch.")
            break

        for item in items:
            batch_data.append((item['DOI'], json.dumps(item)))
            total_rows += 1

            if len(batch_data) >= batch_size or total_rows >= max_rows:
                logger.info(f"Processing batch with {len(batch_data)} rows. Total rows processed: {total_rows}")

                df = spark.createDataFrame(batch_data, schema=raw_data_schema) \
                    .withColumn("ingestion_timestamp", current_timestamp())

                # write to local parquet file
                df.write.mode("append").parquet("crossref_raw_data")

                batch_data.clear()

                if total_rows >= max_rows:
                    logger.info(f"Reached the maximum limit of {max_rows} rows. Stopping.")
                    break

        page += 1
        new_cursor = data.get("message", {}).get("next-cursor")
        cursor = new_cursor

        if new_cursor is None or total_rows >= max_rows:
            logger.info("No more items to fetch.")
            break


# Run the function and process the data in batches
crossref_raw_data()
