import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


crossref_schema = StructType(
    [
        StructField("DOI", StringType(), True),
        StructField("title", ArrayType(StringType()), True),
        StructField(
            "author",
            ArrayType(
                StructType(
                    [
                        StructField("given", StringType(), True),
                        StructField("family", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField("ORCID", StringType(), True),
                        StructField(
                            "affiliation",
                            ArrayType(
                                StructType([StructField("name", StringType(), True)])
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
        StructField("abstract", StringType(), True),
        StructField("type", StringType(), True),
        StructField("publisher", StringType(), True),
        StructField("container-title", ArrayType(StringType()), True),
        StructField("ISSN", ArrayType(StringType()), True),
        StructField(
            "indexed",
            StructType([StructField("date-time", TimestampType(), True)]),
            True,
        ),
        StructField(
            "created",
            StructType([StructField("date-time", TimestampType(), True)]),
            True,
        ),
        StructField(
            "deposited",
            StructType([StructField("date-time", TimestampType(), True)]),
            True,
        ),
        StructField(
            "reference",
            ArrayType(
                StructType(
                    [
                        StructField("key", StringType(), True),
                        StructField("DOI", StringType(), True),
                        StructField("first-page", StringType(), True),
                        StructField("volume", StringType(), True),
                        StructField("author", StringType(), True),
                        StructField("year", StringType(), True),
                        StructField("journal-title", StringType(), True),
                        StructField("unstructured", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)


@dlt.table(
    name="crossref_landing_zone",
    comment="Landing zone for new Crossref data ingested from S3",
    table_properties={"quality": "bronze"},
)
def crossref_landing_zone():
    s3_bucket_path = "s3a://openalex-sandbox/openalex-elt/crossref/"

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(s3_bucket_path)
    )

    df = df.withColumn(
        "indexed_parsed",
        F.from_json(
            F.col("indexed"),
            StructType([StructField("date-time", TimestampType(), True)]),
        ),
    )

    df = df.withColumn("indexed", F.col("indexed_parsed.date-time"))

    df = df.withColumn("raw_json", F.to_json(F.struct("*")))

    return df.select("DOI", "indexed", "raw_json")


@dlt.table(
    name="crossref_raw_data",
    comment="Accumulated Crossref data with unique DOI and indexed_date pairs",
)
def crossref_raw_data():
    df = dlt.read_stream("crossref_landing_zone")

    return df.dropDuplicates(["DOI", "indexed"])


@dlt.view(name="crossref_transformed_view", comment="View of transformed Crossref data")
@dlt.expect_or_drop("valid_DOI", "DOI IS NOT NULL")
def crossref_transformed_view():
    df = dlt.read_stream("crossref_raw_data")

    df = df.withColumn("parsed", F.from_json(F.col("raw_json"), crossref_schema))

    # basic fields
    df = (
        df.withColumn("doi", F.col("parsed.DOI"))
        .withColumn("title", F.expr("element_at(parsed.title, 1)"))
        .withColumn("type", F.col("parsed.type"))
        .withColumn("abstract", F.col("parsed.abstract"))
        .withColumn("publisher", F.col("parsed.publisher"))
        .withColumn(
            "source_name", F.lower(F.expr("element_at(parsed.`container-title`, 1)"))
        )
        .withColumn("source_issns", F.col("parsed.ISSN"))
    )

    # authors
    df = df.withColumn(
        "authors",
        F.transform(
            F.col("parsed.author"),
            lambda author: F.struct(
                author["given"].alias("given"),
                author["family"].alias("family"),
                author["name"].alias("name"),
                author["ORCID"].alias("ORCID"),
                F.transform(author["affiliation"], lambda aff: aff["name"]).alias(
                    "affiliations"
                ),
            ),
        ),
    ).drop("parsed.author")

    # refernces
    df = df.withColumn(
        "references",
        F.transform(
            F.col("parsed.reference"),
            lambda ref: F.struct(
                ref["key"].alias("key"),
                ref["DOI"].alias("doi"),
                ref["first-page"].alias("first_page"),
                ref["volume"].alias("volume"),
                ref["author"].alias("author"),
                ref["year"].alias("year"),
                ref["journal-title"].alias("journal_title"),
                ref["unstructured"].alias("unstructured"),
            ),
        ),
    ).drop(
        "parsed.reference"
    )  # Drop the original parsed reference column

    # Extract timestamps
    df = (
        df.withColumn("indexed_date", F.col("parsed.indexed.date-time"))
        .withColumn("created_date", F.col("parsed.created.date-time"))
        .withColumn("deposited_date", F.col("parsed.deposited.date-time"))
    )

    # Extract other fields from the parsed data
    df = (
        df.withColumn("doi", F.col("parsed.DOI"))
        .withColumn("title", F.expr("element_at(parsed.title, 1)"))
        .withColumn("type", F.col("parsed.type"))
        .withColumn("abstract", F.col("parsed.abstract"))
        .withColumn("publisher", F.col("parsed.publisher"))
        .withColumn(
            "source_name", F.lower(F.expr("element_at(parsed.`container-title`, 1)"))
        )
        .withColumn("source_issns", F.col("parsed.ISSN"))
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
        "references",
        "deposited_date",
        "indexed_date",
        "created_date",
    )
    return df


dlt.create_target_table(
    name="crossref_works",
    comment="Final Crossref works table with unique DOIs",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="crossref_works",
    source="crossref_transformed_view",
    keys=["doi"],
    sequence_by="indexed_date",
)
