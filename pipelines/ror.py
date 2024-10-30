from pyspark.sql import functions as F
from delta.tables import DeltaTable


s3_path = "s3://openalex-ingest/ror/current/ror_snapshot.parquet"
target_table_path = "/mnt/delta/transformed_ror_data"

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

if DeltaTable.isDeltaTable(spark, target_table_path):
    dlt = DeltaTable.forPath(spark, target_table_path)
    max_update_date = dlt.toDF().agg(F.max("updated_date")).collect()[0][0]

    new_data = final_df.filter(F.col("updated_date") > max_update_date)

    if not new_data.isEmpty():
        # merge the new/changed data
        dlt.alias("ror").merge(
            new_data.alias("new_data"),
            "new_data.id = ror.id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    # save as new table
    final_df.write.format("delta").mode("overwrite").save(target_table_path)
