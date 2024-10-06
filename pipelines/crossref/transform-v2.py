import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="crossref_works_v2", comment="Transformed Crossref data with deduplication"
)
@dlt.expect_or_drop("valid_DOI", "DOI IS NOT NULL")
def crossref_works_v2():
    df = dlt.read("crossref_raw_data_v2")

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
        .withColumn("source_name", F.expr("element_at(`container-title`, 1)"))
        .withColumn("source_issns", F.col("ISSN"))
    )

    # set authors
    df_deduped = df_deduped.withColumn(
        "authors",
        F.transform(
            "author",
            lambda x: F.create_map(
                F.lit("given"),
                x["given"],
                F.lit("family"),
                x["family"],
                F.lit("name"),
                x["name"],
                F.lit("ORCID"),
                x["ORCID"]
            ),
        ),
    ).drop("author")

    # set timestamps
    df_deduped = df_deduped.withColumn(
        "updated_date", F.col("indexed.date-time")
    ).withColumn("created_date", F.col("created.date-time"))

    return df_deduped
