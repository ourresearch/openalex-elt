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
        StructField("ORCID", StringType(), True),
        StructField("affiliation", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("id", ArrayType(StructType([
                StructField("id", StringType(), True),
                StructField("id-type", StringType(), True),
                StructField("asserted-by", StringType(), True)
            ])), True)
        ])), True)
    ])), True),
    StructField("abstract", StringType(), True),
    StructField("type", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("container-title", ArrayType(StringType()), True),
    StructField("ISSN", ArrayType(StringType()), True),
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
def crossref_landing_zone_v2():
    s3_bucket_path = "s3a://openalex-sandbox/openalex-elt/crossref/"
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "dbfs:/pipelines/crossref/schema")
        .schema(crossref_schema)
        .load(s3_bucket_path)
    )
    return df


@dlt.table(
    name="crossref_raw_data_v2",
    comment="Accumulated Crossref data with unique DOI and indexed_date pairs"
)
def crossref_raw_data_v2():
    df = dlt.read_stream("crossref_landing_zone_v2")
    
    df = df.withColumn("indexed_date", col("indexed.date-time"))
    
    return df.dropDuplicates(["DOI", "indexed_date"]).drop("indexed_date")
