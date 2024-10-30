import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window


@dlt.table
def pubmed_works():
    df = spark.table("hive_metastore.pubmed.pubmed_works")
    return (df
        .selectExpr(
            "pmid",
            "doi",
            "pmc_id",
            "title",
            "normalized_title",
            "abstract",
            "type",
            """transform(authors, a -> 
                named_struct(
                    'given', a.given,
                    'family', a.family,
                    'name', concat_ws(' ', a.given, a.family),
                    'ORCID', cast(null as string),
                    'affiliations', a.affiliations
                )
            ) as authors""",
            "source_title",
            "array(source_issns) as source_issns",
            "volume",
            "issue",
            "references",
            "publication_date",
            "created_date",
            "updated_date"
        )
    )

@dlt.table
def crossref_works():
    return spark.table("hive_metastore.crossref.crossref_works")

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

    # let's select and rename columns explicitly to avoid ambiguity
    doi_matches = (
        doi_matched.filter(
            (F.col("pubmed_works.doi").isNotNull()) |
            (F.col("crossref_works.doi").isNotNull())
        )
        .select(  # Select the same columns that will match with title_matched
            F.col("pmid"),
            F.coalesce(F.col("pubmed_works.doi"), F.col("crossref_works.doi")).alias("doi"),
            F.col("pmc_id"),
            F.coalesce(F.col("pubmed_works.title"), F.col("crossref_works.title")).alias("title"),
            F.coalesce(F.col("pubmed_works.abstract"), F.col("crossref_works.abstract")).alias("abstract"),
            F.coalesce(F.col("pubmed_works.type"), F.col("crossref_works.type")).alias("work_type"),
            F.coalesce(F.col("pubmed_works.authors"), F.col("crossref_works.authors")).alias("authors"),
            F.coalesce(F.col("source_title"), F.col("source_name")).alias("source_name"),
            F.coalesce(F.col("pubmed_works.source_issns"), F.col("crossref_works.source_issns")).alias("source_issns"),
            F.coalesce(F.col("pubmed_works.volume"), F.col("crossref_works.volume")).alias("volume"),
            F.coalesce(F.col("pubmed_works.issue"), F.col("crossref_works.issue")).alias("issue"),
            F.coalesce(F.col("pubmed_works.publication_date"), F.col("crossref_works.created_date")).alias("publication_date"),
            F.coalesce(F.col("pubmed_works.updated_date"), F.col("crossref_works.indexed_date")).alias("last_updated_date"),
            F.col("pubmed_works.normalized_title").alias("normalized_title"),
            F.col("crossref_works.normalized_title").alias("crossref_normalized_title")
        )
    )

    # for unmatched records, select and rename columns to avoid ambiguity
    pubmed_unmatched = (
        doi_matched.filter(
            (F.col("pubmed_works.doi").isNull()) &
            (F.col("crossref_works.doi").isNull()) &
            (F.col("pmid").isNotNull())
        )
        .select(
            F.col("pmid"),
            F.col("pubmed_works.doi").alias("doi"),
            F.col("pmc_id"),
            F.col("pubmed_works.title").alias("title"),
            F.col("pubmed_works.abstract").alias("abstract"),
            F.col("pubmed_works.type").alias("work_type"),
            F.col("pubmed_works.authors").alias("authors"),
            F.col("pubmed_works.source_title").alias("source_name"),
            F.col("pubmed_works.source_issns").alias("source_issns"),
            F.col("pubmed_works.volume").alias("volume"),
            F.col("pubmed_works.issue").alias("issue"),
            F.col("pubmed_works.publication_date").alias("publication_date"),
            F.col("pubmed_works.updated_date").alias("last_updated_date"),
            F.col("pubmed_works.normalized_title").alias("normalized_title"),
            F.lit(None).alias("crossref_normalized_title")
        )
    )

    crossref_unmatched = (
        doi_matched.filter(
            (F.col("pubmed_works.doi").isNull()) &
            (F.col("crossref_works.doi").isNull()) &
            (F.col("pmid").isNull())
        )
        .select(
            F.lit(None).cast("long").alias("pmid"),
            F.col("crossref_works.doi").alias("doi"),
            F.lit(None).alias("pmc_id"),
            F.col("crossref_works.title").alias("title"),
            F.col("crossref_works.abstract").alias("abstract"),
            F.col("crossref_works.type").alias("work_type"),
            F.col("crossref_works.authors").alias("authors"),
            F.col("crossref_works.source_name").alias("source_name"),
            F.col("crossref_works.source_issns").alias("source_issns"),
            F.col("crossref_works.volume").alias("volume"),
            F.col("crossref_works.issue").alias("issue"),
            F.col("crossref_works.created_date").alias("publication_date"),
            F.col("crossref_works.indexed_date").alias("last_updated_date"),
            F.lit(None).alias("normalized_title"),
            F.col("crossref_works.normalized_title").alias("crossref_normalized_title")
        )
    )

    # now join with renamed columns
    title_matched = (
        pubmed_unmatched
        .join(
            crossref_unmatched,
            # Use col() with explicit DataFrame references
            (pubmed_unmatched["normalized_title"] == crossref_unmatched["normalized_title"]) &
            (F.length(pubmed_unmatched["normalized_title"]) > 20),
            "full_outer"
        )
    )

    # Combine DOI matches and title matches with the same schema
    all_matches = doi_matches.union(title_matched)

    # select and coalesce fields
    final_df = (
        all_matches
        .select(
            F.coalesce(F.col("pmid"), F.lit(None)).alias("pmid"),
            F.coalesce(F.col("pubmed_works.doi"), F.col("crossref_works.doi")).alias("doi"),
            F.coalesce(F.col("pmc_id"), F.lit(None)).alias("pmc_id"),
            F.coalesce(F.col("pubmed_works.title"), F.col("crossref_works.title")).alias("title"),
            F.coalesce(F.col("pubmed_works.abstract"), F.col("crossref_works.abstract")).alias("abstract"),
            F.coalesce(F.col("pubmed_works.type"), F.col("crossref_works.type")).alias("work_type"),
            F.coalesce(F.col("pubmed_works.authors"), F.col("crossref_works.authors")).alias("authors"),
            F.coalesce(F.col("source_title"), F.col("source_name")).alias("source_name"),
            F.coalesce(F.col("pubmed_works.source_issns"), F.col("crossref_works.source_issns")).alias("source_issns"),
            F.coalesce(F.col("pubmed_works.volume"), F.col("crossref_works.volume")).alias("volume"),
            F.coalesce(F.col("pubmed_works.issue"), F.col("crossref_works.issue")).alias("issue"),
            F.coalesce(F.col("publication_date"), F.col("created_date")).alias("publication_date"),
            F.coalesce(F.col("updated_date"), F.col("indexed_date")).alias("last_updated_date"),
            F.when(F.col("pmid").isNotNull(), "pubmed")
            .when(F.col("pubmed_works.doi").isNotNull(), "crossref")
            .otherwise("unknown").alias("primary_source"),
            F.when(F.col("pubmed_works.doi").isNotNull() & F.col("crossref_works.doi").isNotNull(), "doi_match")
            .when(F.col("pubmed_works.normalized_title") == F.col("crossref_works.normalized_title"), "title_match")
            .otherwise("single_source").alias("match_type")
        )
    )

    # add row quality score based on available fields
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
