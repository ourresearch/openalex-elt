import re
import unicodedata

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType


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

    response = title[0:500]

    response = response.lower()

    # handle unicode characters
    response = remove_accents(response)

    # remove HTML tags
    response = clean_html(response)

    # remove articles and common prepositions
    response = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", response)

    # remove everything except alphabetic characters
    response = remove_everything_but_alphas(response)

    return response.strip()

normalize_title_udf = F.udf(normalize_title, StringType())


# crossref schema

crossref_schema = StructType([
    # all crossref fields in same order as the Crossref API response
    StructField("indexed", StructType([
        StructField("date-parts", ArrayType(ArrayType(IntegerType())), True),
        StructField("date-time", TimestampType(), True),
        StructField("timestamp", LongType(), True)
    ]), True),
    StructField("publisher", StringType(), True),
    StructField("issue", StringType(), True),
    StructField("license", ArrayType(StructType([
        StructField("URL", StringType(), True),
        StructField("start", StructType([
            StructField("date-parts", ArrayType(ArrayType(IntegerType())), True),
            StructField("date-time", TimestampType(), True),
            StructField("timestamp", LongType(), True)
        ]), True),
        StructField("delay-in-days", IntegerType(), True),
        StructField("content-version", StringType(), True),
        StructField("identifier", StringType(), True),
        StructField("type", StringType(), True)
    ])), True),
    StructField("content-domain", StructType([
        StructField("domain", ArrayType(StringType()), True),
        StructField("crossmark-restriction", BooleanType(), True)
    ]), True),
    StructField("short-container-title", ArrayType(StringType()), True),
    StructField("published-print", StructType([
        StructField("date-parts", ArrayType(ArrayType(IntegerType())), True)
    ]), True),
    StructField("abstract", StringType(), True),
    StructField("DOI", StringType(), True),
    StructField("type", StringType(), True),
    StructField("created", StructType([
        StructField("date-parts", ArrayType(ArrayType(IntegerType())), True),
        StructField("date-time", TimestampType(), True),
        StructField("timestamp", LongType(), True)
    ]), True),
    StructField("page", StringType(), True),
    StructField("update-policy", StringType(), True),
    StructField("source", StringType(), True),
    StructField("is-referenced-by-count", IntegerType(), True),
    StructField("title", ArrayType(StringType()), True),
    StructField("prefix", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("author", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("family", StringType(), True),
        StructField("given", StringType(), True),
        StructField("sequence", StringType(), True),
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
    StructField("member", StringType(), True),
    StructField("reference", ArrayType(StructType([
        StructField("key", StringType(), True),
        StructField("DOI", StringType(), True),
        StructField("doi-asserted-by", StringType(), True),
        StructField("article-title", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("first-page", StringType(), True),
        StructField("year", StringType(), True),
        StructField("journal-title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("unstructured", StringType(), True)
    ])), True),
    StructField("published-online", StructType([
        StructField("date-parts", ArrayType(ArrayType(IntegerType())), True)
    ]), True),
    StructField("container-title", ArrayType(StringType()), True),
    StructField("language", StringType(), True),
    StructField("link", ArrayType(StructType([
        StructField("URL", StringType(), True),
        StructField("content-type", StringType(), True),
        StructField("content-version", StringType(), True),
        StructField("intended-application", StringType(), True)
    ])), True),
    StructField("deposited", StructType([
        StructField("date-parts", ArrayType(ArrayType(IntegerType())), True),
        StructField("date-time", TimestampType(), True),
        StructField("timestamp", LongType(), True)
    ]), True),
    StructField("score", DoubleType(), True),
    StructField("resource", StringType(), True),
    StructField("issued", StructType([
        StructField("date-parts", ArrayType(ArrayType(IntegerType())), True)
    ]), True),
    StructField("references-count", IntegerType(), True),
    StructField("journal-issue", StructType([
        StructField("published-print", StructType([
            StructField("date-parts", ArrayType(ArrayType(IntegerType())), True)
        ]), True),
        StructField("issue", StringType(), True)
    ]), True),
    StructField("alternative-id", ArrayType(StringType()), True),
    StructField("URL", StringType(), True),
    StructField("ISSN", ArrayType(StringType()), True),
    StructField("issn-type", ArrayType(StructType([
        StructField("value", StringType(), True),
        StructField("type", StringType(), True)
    ])), True),
    StructField("published", StructType([
        StructField("date-parts", ArrayType(ArrayType(IntegerType())), True)
    ]), True),
    StructField("assertion", ArrayType(StructType([
        StructField("value", StringType(), True),
        StructField("order", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("label", StringType(), True),
        StructField("group", StructType([
            StructField("name", StringType(), True),
            StructField("label", StringType(), True)
        ]), True),
    ])), True),
])


@dlt.table(
    name="crossref_landing_zone",
    comment="Landing zone for new Crossref data ingested from S3",
    table_properties={'quality': 'bronze'}
)
def crossref_landing_zone():
    s3_bucket_path = "s3a://openalex-ingest/crossref/new-works"
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "dbfs:/pipelines/crossref/schema")
        .schema(crossref_schema)
        .load(s3_bucket_path)
    )
    return df


@dlt.table(
    name="crossref_raw_data",
    comment="Accumulated Crossref data with unique DOI and indexed_date pairs"
)
def crossref_raw_data():
    df = dlt.read_stream("crossref_landing_zone")

    df = df.withColumn("indexed_date", F.col("indexed.date-time"))

    return df.dropDuplicates(["DOI", "indexed_date"]).drop("indexed_date")


@dlt.view(
    name="crossref_transformed_view",
    comment="View of transformed Crossref data"
)
@dlt.expect_or_drop("valid_DOI", "DOI IS NOT NULL")
def crossref_transformed_view():
    df = dlt.read_stream("crossref_raw_data")

    # set root fields
    df = (
        df.withColumn("doi", F.col("DOI"))
        .withColumn("title", F.expr("element_at(title, 1)"))
        .withColumn("normalized_title", normalize_title_udf(F.col("title")))
        .withColumn("type", F.col("type"))
        .withColumn("abstract", F.col("abstract"))
        .withColumn("references", F.col("reference"))
        .withColumn("publisher", F.col("publisher"))
        .withColumn("source_name", F.expr("element_at(`container-title`, 1)"))
        .withColumn("source_issns", F.col("ISSN"))
    )

    # set authors
    df = df.withColumn(
        "authors",
        F.transform(
            "author",
            lambda author: F.struct(
                author["given"].alias("given"),
                author["family"].alias("family"),
                author["name"].alias("name"),
                author["ORCID"].alias("ORCID"),
                F.transform(author["affiliation"], lambda aff: aff["name"]).alias("affiliations")
            )
        )
    )

    # set timestamps
    df = (
        df.withColumn("indexed_date", F.col("indexed.date-time"))
        .withColumn("created_date", F.col("created.date-time"))
        .withColumn("deposited_date", F.col("deposited.date-time"))
    )

    # select and reorder columns
    df = df.select(
        "doi",
        "title",
        "normalized_title",
        "type",
        "authors",
        "publisher",
        "source_name",
        "source_issns",
        "abstract",
        "references",
        "language",
        "issue",
        "page",
        "volume",
        "deposited_date",
        "indexed_date",
        "created_date",
    )

    return df

dlt.create_target_table(
    name="crossref_works",
    comment="Final Crossref works table with unique DOIs",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target="crossref_works",
    source="crossref_transformed_view",
    keys=["doi"],
    sequence_by="indexed_date"
)
