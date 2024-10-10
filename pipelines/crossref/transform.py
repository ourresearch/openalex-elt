import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="crossref_works", comment="Transformed Crossref data with deduplication"
)
@dlt.expect_or_drop("valid_DOI", "DOI IS NOT NULL")
def crossref_works():
    df = dlt.read("crossref_raw_data")

    # deduplicate by DOI, keeping the most recent indexed date DOI
    df = df.withColumn("indexed_date", F.col("indexed.date-time"))
    df = df.filter(F.col("indexed_date").isNotNull())
    window_spec = Window.partitionBy("DOI").orderBy(F.col("indexed_date").desc())
    df_ranked = df.withColumn("row_number", F.row_number().over(window_spec))
    df_deduped = df_ranked.filter(F.col("row_number") == 1).drop("row_number")

    # set root fields
    df_deduped = (
        df_deduped.withColumn("doi", F.col("DOI"))
        .withColumn("title", F.expr("element_at(title, 1)"))
        .withColumn("type", F.col("type"))
        .withColumn("abstract", F.col("abstract"))
        .withColumn("publisher", F.col("publisher"))
        .withColumn("source_name", F.lower(F.expr("element_at(`container-title`, 1)")))
        .withColumn("source_issns", F.col("ISSN"))
        .drop("container-title", "ISSN")
    )

    # set authors
    df_deduped = df_deduped.withColumn(
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
    df_deduped = (
        df_deduped.withColumn("updated_date", F.col("indexed.date-time"))
        .withColumn("created_date", F.col("created.date-time"))
        .withColumn("deposited_date", F.col("deposited.date-time"))
        .drop("indexed", "created", "deposited")
    )

    # reorder columns
    df_deduped = df_deduped.select(
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

    return df_deduped
