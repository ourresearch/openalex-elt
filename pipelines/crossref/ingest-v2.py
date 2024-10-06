import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

crossref_schema = StructType([
    StructField("DOI", StringType(), True),
    StructField("title", ArrayType(StringType()), True),
    StructField("author", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("family", StringType(), True),
        StructField("given", StringType(), True),
        StructField("ORCID", StringType(), True)
    ])), True),
    StructField("abstract", StringType(), True),
    StructField("type", StringType(), True),
    # Include timestamp fields
    StructField("indexed", StructType([
        StructField("date-time", TimestampType(), True)
    ]), True),
    StructField("created", StructType([
        StructField("date-time", TimestampType(), True)
    ]), True),
    StructField("deposited", StructType([
        StructField("date-time", TimestampType(), True)
    ]), True)
])


@dlt.table(
    name="crossref_landing_zone_v2",
    comment="Landing zone for new Crossref data ingested from S3"
)
def read_landing_zone():
    s3_bucket_path = "s3a://openalex-sandbox/openalex-elt/crossref/"
    df = (
        spark.read.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "dbfs:/pipelines/crossref/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .schema(crossref_schema)
        .load(s3_bucket_path)
    )
    return df


@dlt.table(
    name="crossref_raw_data",
    comment="Accumulated Crossref data with unique DOI and indexed_date pairs"
)
def crossref_raw_data_v2():
    df_new = dlt.read("crossref_landing_zone_v2")
    df_new = df_new.withColumn("indexed_date", col("indexed.date-time"))

    df_new = df_new.dropDuplicates(["DOI", "indexed_date"])

    if spark.catalog._jcatalog.tableExists('dlt.crossref_raw_data_v2'):
        df_existing = dlt.read("crossref_raw_data_v2")
    else:
        df_existing = spark.createDataFrame([], df_new.schema)

    df_combined = df_existing.unionByName(df_new)

    df_combined = df_combined.dropDuplicates(["DOI", "indexed_date"])

    return df_combined
