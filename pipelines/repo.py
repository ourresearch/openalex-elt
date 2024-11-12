import re
import unicodedata

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    TimestampType
)

# normalize title UDF

def clean_html(raw_html):
    cleanr = re.compile('<\w+.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

def remove_everything_but_alphas(input_string):
    if input_string:
        return "".join(e for e in input_string if e.isalpha())
    return ""

def remove_accents(text):
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')

def normalize_title(title):
    if not title:
        return ""

    if isinstance(title, bytes):
        title = str(title, 'ascii')

    text = title[0:500]

    text = text.lower()

    # handle unicode characters
    text = remove_accents(text)

    # remove HTML tags
    text = clean_html(text)

    # remove articles and common prepositions
    text = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", text)

    # remove everything except alphabetic characters
    text = remove_everything_but_alphas(text)

    return text.strip()

normalize_title_udf = F.udf(normalize_title, StringType())


repository_schema = StructType([
    StructField("ns0:header", StructType([
        StructField("ns0:identifier", StringType(), True),
        StructField("ns0:datestamp", TimestampType(), True),
        StructField("ns0:setSpec", ArrayType(StringType()), True)
    ]), True),
    StructField("ns0:metadata", StructType([
        StructField("ns1:dc", StructType([
            StructField("dc:title", StringType(), True),
            StructField("dc:creator", ArrayType(StringType()), True),
            StructField("dc:contributor", ArrayType(StringType()), True),
            StructField("dc:subject", ArrayType(StringType()), True),
            StructField("dc:description", ArrayType(StringType()), True),
            StructField("dc:date", ArrayType(StringType()), True),
            StructField("dc:type", StringType(), True),
            StructField("dc:identifier", ArrayType(StringType()), True),
            StructField("dc:language", StringType(), True),
            StructField("dc:format", ArrayType(StringType()), True),
            StructField("dc:publisher", StringType(), True),
            StructField("dc:rights", ArrayType(StringType()), True)
        ]), True)
    ]), True)
])


@dlt.table(
    name="repository_landing_zone",
    comment="Landing zone for new repository data ingested from S3",
    table_properties={"quality": "bronze"}
)
def repository_landing_zone():
    s3_bucket_path = "s3://openalex-ingest/repositories/"

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("cloudFiles.schemaLocation", "dbfs:/pipelines/repository/schema")
        .option("rowTag", "ns0:record")
        .schema(repository_schema)
        .load(s3_bucket_path)
    )

    return df


@dlt.table(
    name="repository_raw_data",
    comment="Accumulated repository data with unique identifiers and metadata"
)
def repository_raw_data():
    df = dlt.read_stream("repository_landing_zone")

    # Deduplicate by identifier and datestamp
    df = df.withColumn("identifier", F.col("ns0:header.ns0:identifier"))
    df = df.withColumn("updated_date", F.col("ns0:header.ns0:datestamp"))
    df = df.dropDuplicates(["identifier", "updated_date"])

    return df


@dlt.view(
    name="repository_transformed_view",
    comment="Transformed view of the raw repository data"
)
def repository_transformed_view():
    df = dlt.read_stream("repository_raw_data")
    df = (
        df
        # basic metadata
        .withColumn("title", F.col("ns0:metadata.ns1:dc.dc:title"))
        .withColumn("normalized_title", normalize_title_udf(F.col("title")))
        .withColumn("type", F.col("ns0:metadata.ns1:dc.dc:type"))

        # authors
        .withColumn("authors", F.col("ns0:metadata.ns1:dc.dc:creator"))

        # publication info
        .withColumn("publisher", F.col("ns0:metadata.ns1:dc.dc:publisher"))
        .withColumn("language", F.col("ns0:metadata.ns1:dc.dc:language"))
    )

    return df.select(
        "identifier",
        "title",
        "normalized_title",
        "authors",
        "publisher",
        "language",
        "type",
        "updated_date"
    )



dlt.create_target_table(
    name="repository_works",
    comment="Final repository works table with unique identifiers",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target="repository_works",
    source="repository_transformed_view",
    keys=["identifier"],
    sequence_by="updated_date"
)