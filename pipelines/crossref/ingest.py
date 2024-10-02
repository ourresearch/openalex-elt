import datetime
import json
import logging

import dlt
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

raw_crossref_schema = StructType([
    StructField("doi", StringType(), True),
    StructField("message", StringType(), True)
])

def fetch_recent_crossref_data(yesterday, cursor=None):
    url = "https://api.crossref.org/works"
    params = {
        "filter": f"from-index-date:{yesterday},until-index-date:{yesterday}",
        "rows": 500,
        "cursor": cursor if cursor else "*",
        "sort": "indexed",
        "order": "desc"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

@dlt.table(
    comment="Raw Crossref works data",
    table_properties={"quality": "bronze"}
)
def crossref_raw_data():
    cursor = None
    total_rows = 0
    batch_size = 500
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    batch_data = []

    while True:
        data = fetch_recent_crossref_data(yesterday, cursor)
        items = data.get("message", {}).get("items", [])
        rows_this_page = len(items)

        if rows_this_page == 0:
            logger.info("No more items to fetch.")
            break

        for item in items:
            batch_data.append((item['DOI'], json.dumps(item)))
            total_rows += 1

            # process batch if we have reached batch size
            if len(batch_data) >= batch_size:
                logger.info(f"Processing batch with {len(batch_data)} rows. Total rows processed: {total_rows}")

                # create DataFrame for the batch
                df = spark.createDataFrame(batch_data, schema=raw_crossref_schema) \
                    .withColumn("ingestion_timestamp", current_timestamp())

                batch_data.clear()

                return df

        cursor = data.get("message", {}).get("next-cursor")
        if cursor is None or len(items) == 0:
            logger.info("No more items to fetch.")
            break

    # process the remaining data if there are leftover rows in the last small batch
    if batch_data:
        logger.info(f"Processing the final small batch with {len(batch_data)} rows. Total rows processed: {total_rows}")

        # Create DataFrame for the final batch
        df = spark.createDataFrame(batch_data, schema=raw_crossref_schema) \
            .withColumn("ingestion_timestamp", current_timestamp())

        return df
