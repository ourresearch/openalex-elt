import dlt
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
import pyspark.sql.functions as F


pubmed_schema = StructType([
    StructField("PMID", StructType([
        StructField("#text", StringType(), True),
        StructField("Version", IntegerType(), True)
    ]), True),
    StructField("DateCompleted", StructType([
        StructField("Year", StringType(), True),
        StructField("Month", StringType(), True),
        StructField("Day", StringType(), True)
    ]), True),
    StructField("DateRevised", StructType([
        StructField("Year", StringType(), True),
        StructField("Month", StringType(), True),
        StructField("Day", StringType(), True)
    ]), True),
    StructField("Article", StructType([
        StructField("Journal", StructType([
            StructField("ISSN", StructType([StructField("#text", StringType(), True)]), True),
            StructField("Title", StringType(), True),
            StructField("JournalIssue", StructType([
                StructField("Volume", StringType(), True),
                StructField("Issue", StringType(), True),
                StructField("PubDate", StructType([
                    StructField("Year", StringType(), True),
                    StructField("Month", StringType(), True),
                    StructField("Day", StringType(), True)
                ]), True)
            ]), True)
        ]), True),
        StructField("ArticleTitle", StringType(), True),
        StructField("AuthorList", ArrayType(StructType([
            StructField("LastName", StringType(), True),
            StructField("ForeName", StringType(), True),
            StructField("Initials", StringType(), True)
        ])), True),
        StructField("Language", StringType(), True),
        StructField("PublicationTypeList", ArrayType(StringType()), True)
    ]), True),
    StructField("ArticleIdList", StructType([
        StructField("ArticleId", ArrayType(StructType([
            StructField("#text", StringType(), True),
            StructField("@IdType", StringType(), True)
        ])), True)
    ]), True)
])

@dlt.table(
    name="pubmed_landing_zone",
    comment="Landing zone for new PubMed data ingested from S3",
    table_properties={'quality': 'bronze'}
)
def pubmed_landing_zone():
    s3_bucket_path = "s3a://openalex-ingest/pubmed"

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("cloudFiles.schemaLocation", "dbfs:/pipelines/pubmed/schema")
        .schema(pubmed_schema)
        .option("rowTag", "PubmedArticle")
        .load(s3_bucket_path)
    )

    return df


@dlt.table(
    name="pubmed_raw_data",
    comment="Accumulated PubMed data with unique PMID"
)
def pubmed_raw_data():
    df = dlt.read_stream("pubmed_landing_zone")

    df = df.withColumn("pmid", F.col("PubmedArticle.MedlineCitation.PMID.#text"))
    df = df.withColumn("revised_date", F.col("PubmedArticle.MedlineCitation.DateRevised"))

    return df.dropDuplicates(["pmid", "revised_date"])


dlt.create_target_table(
    name="pubmed_works",
    comment="Final PubMed articles table with unique PMIDs",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target="pubmed_works",
    source="pubmed_raw_data",
    keys=["pmid"],
    sequence_by="revised_date"
)
