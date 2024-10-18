import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window


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
    name="crossref_landing_zone",
    comment="Landing zone for new Crossref data ingested from S3",
    table_properties={'quality': 'bronze'}
)
def crossref_landing_zone():
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
        .withColumn("type", F.col("type"))
        .withColumn("abstract", F.col("abstract"))
        .withColumn("publisher", F.col("publisher"))
        .withColumn("source_name", F.lower(F.expr("element_at(`container-title`, 1)")))
        .withColumn("source_issns", F.col("ISSN"))
        .drop("container-title", "ISSN")
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
    ).drop("author")

    # set timestamps
    df = (
        df.withColumn("updated_date", F.col("indexed.date-time"))
        .withColumn("created_date", F.col("created.date-time"))
        .withColumn("deposited_date", F.col("deposited.date-time"))
        .drop("indexed", "created", "deposited")
    )

    # reorder columns
    df = df.select(
        "doi",
        "title",
        "type",
        "authors",
        "publisher",
        "source_name",
        "source_issns",
        "abstract",
        "deposited_date",
        "updated_date",
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
    sequence_by="updated_date"
)
