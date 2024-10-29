import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="merged_works",
    comment="Combined works from PubMed and Crossref with DOI and title matching",
    table_properties={"quality": "gold"}
)
def merged_works():
    pubmed_df = dlt.read("pubmed_works")
    crossref_df = dlt.read("crossref_works")

    # convert DOIs to lowercase for case-insensitive join
    pubmed_df = pubmed_df.withColumn("doi_lower", F.lower(F.col("doi")))
    crossref_df = crossref_df.withColumn("doi_lower", F.lower(F.col("doi")))

    # first match: join by lowercase DOI where available
    doi_matched = (
        pubmed_df
        .join(
            crossref_df,
            (pubmed_df.doi_lower == crossref_df.doi_lower) &
            (pubmed_df.doi.isNotNull()),
            "full_outer"
        )
    )

    # separate matched and unmatched records
    doi_matches = doi_matched.filter(
        (doi_matched.doi.isNotNull()) |
        (doi_matched["crossref_df.doi"].isNotNull())
    )

    pubmed_unmatched = doi_matched.filter(
        (doi_matched.doi.isNull()) &
        (doi_matched["crossref_df.doi"].isNull()) &
        (doi_matched.pmid.isNotNull())
    )

    crossref_unmatched = doi_matched.filter(
        (doi_matched.doi.isNull()) &
        (doi_matched["crossref_df.doi"].isNull()) &
        (doi_matched.pmid.isNull())
    )

    # second match: join remaining records by normalized title
    title_matched = (
        pubmed_unmatched
        .join(
            crossref_unmatched,
            (pubmed_unmatched.normalized_title == crossref_unmatched.normalized_title) &
            (F.length(pubmed_unmatched.normalized_title) > 20),
            "full_outer"
        )
    )

    # Combine DOI matches and title matches
    all_matches = doi_matches.union(title_matched)

    # select and coalesce fields
    final_df = (
        all_matches
        .select(
            F.coalesce("pmid", F.lit(None)).alias("pmid"),
            F.coalesce("doi", "crossref_df.doi").alias("doi"),
            F.coalesce("pmc_id", F.lit(None)).alias("pmc_id"),
            F.coalesce("title", "crossref_df.title").alias("title"),
            F.coalesce("abstract", "crossref_df.abstract").alias("abstract"),
            F.coalesce("type", "crossref_df.type").alias("work_type"),
            F.coalesce("authors", "crossref_df.authors").alias("authors"),
            F.coalesce("source_title", "source_name").alias("source_name"),
            F.coalesce("source_issns", "crossref_df.source_issns").alias("source_issns"),
            F.coalesce("volume", "crossref_df.volume").alias("volume"),
            F.coalesce("issue", "crossref_df.issue").alias("issue"),
            F.coalesce("publication_date", "created_date").alias("publication_date"),
            F.coalesce("updated_date", "indexed_date").alias("last_updated_date"),
            F.when(F.col("pmid").isNotNull(), "pubmed")
            .when(F.col("doi").isNotNull(), "crossref")
            .otherwise("unknown").alias("primary_source"),
            # Add match type
            F.when(F.col("doi").isNotNull() & F.col("crossref_df.doi").isNotNull(), "doi_match")
            .when(F.col("normalized_title") == F.col("crossref_df.normalized_title"), "title_match")
            .otherwise("single_source").alias("match_type")
        )
    )

    # ddd row quality score based on available fields
    final_df = final_df.withColumn(
        "quality_score",
        (
                F.when(F.col("doi").isNotNull(), 2).otherwise(0) +
                F.when(F.col("pmid").isNotNull(), 2).otherwise(0) +
                F.when(F.col("title").isNotNull(), 1).otherwise(0) +
                F.when(F.col("abstract").isNotNull(), 1).otherwise(0) +
                F.when(F.col("authors").isNotNull(), 1).otherwise(0) +
                F.when(F.col("publication_date").isNotNull(), 1).otherwise(0)
        )
    )

    # deduplicate based on quality score
    window_spec = Window.partitionBy("doi").orderBy(F.desc("quality_score"))
    final_df = (
        final_df
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    return final_df
