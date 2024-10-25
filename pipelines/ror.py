import dlt
from pyspark.sql import functions as F

s3_path = "s3://openalex-ingest/ror/current/ror_snapshot.parquet"

@dlt.table(
    name="raw_ror_data",
    comment="Initial ingestion of ROR data from S3",
    table_properties={"quality": "bronze"}
)
def load_raw_ror_data():
    df = spark.read.parquet(s3_path)
    return df

@dlt.view(
    name="transformed_ror_view",
    comment="Transformed ROR data for change capture."
)
def transform_ror_data():
    df = dlt.read("raw_ror_data")

    # name
    df = df.withColumn(
        "name",
        F.expr("filter(names, x -> array_contains(x.types, 'label'))[0].value")
    )

    # created and updated dates
    df = df.withColumn("created_date", F.col("created.date")) \
        .withColumn("updated_date", F.col("last_modified.date"))

    # website
    df = df.withColumn(
        "website",
        F.expr("filter(links, x -> x.type == 'website')[0].url")
    )

    # wikipedia
    df = df.withColumn(
        "wikipedia",
        F.expr("filter(links, x -> x.type == 'wikipedia')[0].url")
    )

    df = df.select(
        "id",
        "name",
        F.expr("locations[0].geonames_details.country_code").alias("country_code"),
        F.expr("locations[0].geonames_details.country_name").alias("country_name"),
        F.expr("locations[0].geonames_details.name").alias("location_name"),
        F.expr("locations[0].geonames_details.lat").alias("latitude"),
        F.expr("locations[0].geonames_details.lng").alias("longitude"),
        F.expr("locations[0].geonames_id").alias("geonames_id"),
        "types",
        "relationships",
        "website",
        "wikipedia",
        "established",
        "external_ids",
        "created_date",
        "updated_date"
    )

    return df

dlt.create_target_table(
    name="transformed_ror_data",
    comment="Up-to-date ROR data with unique IDs",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target="transformed_ror_data",
    source="transformed_ror_view",
    keys=["id"],
    sequence_by="updated_date",
    apply_as_deletes=False
)
