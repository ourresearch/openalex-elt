import dlt
from pyspark.sql.functions import col

s3_path = "s3://openalex-ingest/ror/current/ror_snapshot.parquet"


@dlt.table(
    name="raw_ror_data",
    comment="Initial ingestion of ROR data from S3",
    table_properties={"quality": "bronze"}
)
def load_raw_ror_data():
    df = spark.read.parquet(s3_path)
    return df


@dlt.table(
    name="transformed_ror_data",
    comment="Transformed ROR data.",
    table_properties={"quality": "silver"}
)
def transform_ror_data():
    df = dlt.read("raw_ror_data")

    transformed_df = (
        df
        .withColumnRenamed("country_name", "country")
        .withColumn("is_active", col("status") == "active")
    )

    return transformed_df
