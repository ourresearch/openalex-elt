import dlt
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

@dlt.table(
    name="crossref_works_v2",
    comment="Transformed Crossref data with deduplication"
)
@dlt.expect_or_drop("valid_DOI", "DOI IS NOT NULL")
def crossref_works_v2():
    df = dlt.read("crossref_raw_data_v2")

    # deduplicate by DOI, keeping the most recent indexed date DOI
    df = df.withColumn("indexed_date", col("indexed.date-time"))
    df = df.filter(col("indexed_date").isNotNull())
    window_spec = Window.partitionBy("DOI").orderBy(col("indexed_date").desc())
    df_ranked = df.withColumn("row_number", row_number().over(window_spec))
    df_deduped = df_ranked.filter(col("row_number") == 1).drop("row_number")

    # transform the data
    transformed_df = df_deduped.select(
        "DOI",
        df_deduped.title[0].alias("title"),
        "abstract",
        "type",
        "author"
    )

    return transformed_df
