import logging
from pyspark.sql import SparkSession

from schemas import crossref_schema, common_work_schema
from transformations import map_crossref_to_common_work, transform

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_crossref_data():
    spark = SparkSession.builder \
        .appName("CrossrefDataTransformation") \
        .getOrCreate()

    raw_df = spark.read.parquet("crossref_raw_data")

    common_work_df = transform(
        spark=spark,
        raw_df=raw_df,
        source_schema=crossref_schema,
        target_schema=common_work_schema,
        mapping_function=map_crossref_to_common_work
    )

    # Write transformed data to Parquet and JSON formats
    common_work_df.write.mode("overwrite").parquet("crossref_transformed_data")
    common_work_df.write.mode("overwrite").json("crossref_transformed_data_json")

    logger.info("Transformation complete. Data saved to crossref_transformed_data.")

if __name__ == "__main__":
    transform_crossref_data()
