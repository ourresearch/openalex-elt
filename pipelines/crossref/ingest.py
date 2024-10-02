import datetime
import json
import logging
import dlt
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the schema for the raw data
raw_crossref_schema = StructType([
    StructField("doi", StringType(), True),
    StructField("message", StringType(), True)
])

def fetch_recent_crossref_data(yesterday, cursor=None):
    """
    Fetch recent Crossref data using the API.
    """
    url = "https://api.crossref.org/works"
    params = {
        "filter": f"from-index-date:{yesterday},until-index-date:{yesterday}",
        "rows": 500,
        "cursor": cursor if cursor else "*",
        "sort": "indexed",
        "order": "desc"
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

def crossref_data_generator(yesterday):
    """
    Generator function that yields batches of data from Crossref API.
    """
    cursor = None
    total_rows = 0

    while True:
        # Fetch the next batch of data from Crossref
        data = fetch_recent_crossref_data(yesterday, cursor)
        items = data.get("message", {}).get("items", [])

        if not items:
            logger.info("No more items to fetch.")
            break

        logger.info(f"Fetched {len(items)} rows. Total rows fetched: {total_rows + len(items)}")

        yield [(item['DOI'], json.dumps(item)) for item in items]

        total_rows += len(items)

        # Update the cursor for pagination
        cursor = data.get("message", {}).get("next-cursor")
        if not cursor:
            logger.info("No more pages to fetch.")
            break

@dlt.table(
    comment="Raw Crossref works data (generator-based)",
    table_properties={"quality": "bronze"}
)
def crossref_raw_data():
    """
    Create the Delta table by fetching data from Crossref incrementally
    using a generator. The generator yields batches of data, and we return
    a DataFrame that contains the full result at the end of processing.
    """
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    all_data = []

    for batch_data in crossref_data_generator(yesterday):
        all_data.extend(batch_data)

    df = spark.createDataFrame(all_data, schema=raw_crossref_schema) \
        .withColumn("ingestion_timestamp", current_timestamp())

    return df
