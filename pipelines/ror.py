from pyspark.sql import functions as F
from delta.tables import DeltaTable


s3_path = "s3://openalex-ingest/ror/current/ror_snapshot.parquet"
target_table = "transformed_ror_data"

raw_df = spark.read.parquet(s3_path)

transformed_df = raw_df.withColumn(
    "name",
    F.expr("filter(names, x -> array_contains(x.types, 'label'))[0].value")
)

transformed_df = transformed_df.withColumn("created_date", F.col("admin.created.date")) \
    .withColumn("updated_date", F.col("admin.last_modified.date"))

final_df = transformed_df.select(
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
    "links",
    "established",
    "external_ids",
    "created_date",
    "updated_date"
)

if not spark.catalog._jcatalog.tableExists(target_table):
    final_df.write.format("delta").saveAsTable(target_table)
else:
    delta_table = DeltaTable.forName(spark, target_table)

    delta_table.alias("target").merge(
        final_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll(
        condition="source.updated_date > target.updated_date"
    ).whenNotMatchedInsertAll().execute()
