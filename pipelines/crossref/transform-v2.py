import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window


class CrossrefWork:
    def __init__(self, df):
        self.df = df

    @property
    def field_mapping(self):
        return {
            "doi": "DOI",
            "title": "title",
            "type": "type",
            "authors": "author",
            "abstract": "abstract"
        }

    def set_root_fields(self):
        for field_name, field_value in self.field_mapping.items():
            self.df = self.df.withColumn(field_name, F.col(field_value))

    def set_title(self):
        self.df = self.df.withColumn("title", F.expr("element_at(title, 1)"))

    def set_authors(self):
        self.df = self.df.withColumn(
            "authors",
            F.transform(
                "author",
                lambda x: F.create_map(
                    F.lit("given"), x["given"],
                    F.lit("family"), x["family"],
                    F.lit("name"), x["name"],
                    F.lit("ORCID"), x["ORCID"]
                )
            )
        ).drop("author")

    def transform(self):
        self.set_root_fields()
        self.set_title()
        self.set_authors()
        return self.df

@dlt.table(
    name="crossref_works_v2",
    comment="Transformed Crossref data with deduplication"
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

    # transform the data
    transformed_df = CrossrefWork(df_deduped).transform()

    return transformed_df
