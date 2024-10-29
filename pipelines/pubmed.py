import re
import unicodedata

import dlt
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    LongType,
)
import pyspark.sql.functions as F

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

    text = title[0:500]

    text = text.lower()

    # handle unicode characters
    text = remove_accents(text)

    # remove HTML tags
    text = clean_html(text)

    # remove articles and common prepositions
    text = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", text)

    # remove everything except alphabetic characters
    text = remove_everything_but_alphas(text)

    return text.strip()

normalize_title_udf = F.udf(normalize_title, StringType())

# pubmed schema

chemical_schema = StructType([
    StructField("RegistryNumber", StringType(), True),
    StructField("NameOfSubstance", StructType([
        StructField("_VALUE", StringType(), True),
        StructField("_UI", StringType(), True)
    ]), True)
])

mesh_heading_schema = StructType([
    StructField("DescriptorName", StructType([
        StructField("_VALUE", StringType(), True),
        StructField("_UI", StringType(), True),
        StructField("_MajorTopicYN", StringType(), True)
    ]), True),
    StructField("QualifierName", ArrayType(StructType([
        StructField("_VALUE", StringType(), True),
        StructField("_UI", StringType(), True),
        StructField("_MajorTopicYN", StringType(), True)
    ])), True)
])

pubmed_schema = StructType([
    StructField("MedlineCitation", StructType([
        StructField("_Status", StringType(), True),
        StructField("_IndexingMethod", StringType(), True),
        StructField("_Owner", StringType(), True),
        StructField("PMID", StructType([
            StructField("_Version", StringType(), True),
            StructField("_VALUE", LongType(), True)
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
            StructField("_PubModel", StringType(), True),
            StructField("Journal", StructType([
                StructField("ISSN", StructType([
                    StructField("_VALUE", StringType(), True),
                    StructField("_IssnType", StringType(), True)
                ]), True),
                StructField("JournalIssue", StructType([
                    StructField("_CitedMedium", StringType(), True),
                    StructField("Volume", StringType(), True),
                    StructField("Issue", StringType(), True),
                    StructField("PubDate", StructType([
                        StructField("Year", StringType(), True),
                        StructField("Month", StringType(), True),
                        StructField("Day", StringType(), True)
                    ]), True)
                ]), True),
                StructField("Title", StringType(), True),
                StructField("ISOAbbreviation", StringType(), True)
            ]), True),
            StructField("ArticleTitle", StringType(), True),
            StructField("Pagination", StructType([
                StructField("MedlinePgn", StringType(), True)
            ]), True),
            StructField("ELocationID", ArrayType(StructType([
                StructField("_EIdType", StringType(), True),
                StructField("_ValidYN", StringType(), True),
                StructField("_VALUE", StringType(), True)
            ])), True),
            StructField("Abstract", StructType([
                StructField("AbstractText", StringType(), True)
            ]), True),
            StructField("AuthorList", StructType([
                StructField("_CompleteYN", StringType(), True),
                StructField("Author", ArrayType(StructType([
                    StructField("LastName", StringType(), True),
                    StructField("ForeName", StringType(), True),
                    StructField("Initials", StringType(), True),
                    StructField("Identifier", ArrayType(StructType([
                        StructField("_VALUE", StringType(), True),
                        StructField("_Source", StringType(), True)
                    ])), True),
                    StructField("_ValidYN", StringType(), True),
                    StructField("AffiliationInfo", StructType([
                        StructField("Affiliation", StringType(), True)
                    ]), True)
                ])), True)
            ]), True),
            StructField("Language", StringType(), True),
            StructField("GrantList", StructType([
                StructField("Grant", ArrayType(StructType([
                    StructField("GrantID", StringType(), True),
                    StructField("Acronym", StringType(), True),
                    StructField("Agency", StringType(), True),
                    StructField("Country", StringType(), True)
                ])), True)
            ]), True),
            StructField("PublicationTypeList", StructType([
                StructField("PublicationType", ArrayType(StructType([
                    StructField("_VALUE", StringType(), True),
                    StructField("UI", StringType(), True)
                ])), True)
            ]), True)
        ]), True),
        StructField("MedlineJournalInfo", StructType([
            StructField("Country", StringType(), True),
            StructField("MedlineTA", StringType(), True),
            StructField("NlmUniqueID", StringType(), True),
            StructField("ISSNLinking", StringType(), True)
        ]), True),
        StructField("ChemicalList", StructType([
            StructField("Chemical", ArrayType(chemical_schema), True)
        ]), True),
        StructField("CitationSubset", StringType(), True),
        StructField("MeshHeadingList", StructType([
            StructField("MeshHeading", ArrayType(mesh_heading_schema), True)
        ]), True)
    ]), True),
    StructField("PubmedData", StructType([
        StructField("History", StructType([
            StructField("PubMedPubDate", ArrayType(StructType([
                StructField("_PubStatus", StringType(), True),
                StructField("Year", StringType(), True),
                StructField("Month", StringType(), True),
                StructField("Day", StringType(), True),
                StructField("Hour", StringType(), True),
                StructField("Minute", StringType(), True)
            ])), True)
        ]), True),
        StructField("PublicationStatus", StringType(), True),
        StructField("ArticleIdList", StructType([
            StructField("ArticleId", ArrayType(StructType([
                StructField("_VALUE", StringType(), True),
                StructField("_IdType", StringType(), True)
            ])), True)
        ]), True),
        StructField("ReferenceList", StructType([
            StructField("Reference", ArrayType(StructType([
                StructField("Citation", StringType(), True),
                StructField("ArticleIdList", StructType([
                    StructField("ArticleId", ArrayType(StructType([
                        StructField("_VALUE", StringType(), True),
                        StructField("_IdType", StringType(), True)
                    ])), True)
                ]), True)
            ])), True)
        ]), True)
    ]), True)
])


@dlt.table(
    name="pubmed_landing_zone",
    comment="Landing zone for new PubMed data ingested from S3",
    table_properties={"quality": "bronze"},
)
def pubmed_landing_zone():
    s3_bucket_path = "s3a://openalex-ingest/pubmed"

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("cloudFiles.schemaLocation", "dbfs:/pipelines/pubmed/schema")
        .option("rowTag", "PubmedArticle")
        .schema(pubmed_schema)
        .load(s3_bucket_path)
    )

    return df


@dlt.table(
    name="pubmed_raw_data",
    comment="Accumulated PubMed data with unique PMID and additional metadata",
)
def pubmed_raw_data():
    df = dlt.read_stream("pubmed_landing_zone")

    # deduplicate by PMID and revised date
    df = df.withColumn(
        "revised_date",
        F.to_date(
            F.concat_ws(
                "-",
                df.MedlineCitation.DateRevised.Year,
                df.MedlineCitation.DateRevised.Month,
                df.MedlineCitation.DateRevised.Day,
            )
        ),
    )
    df = df.withColumn("pmid", df.MedlineCitation.PMID._VALUE)
    df = df.dropDuplicates(["pmid", "revised_date"])

    return df


@dlt.view(
    name="pubmed_transformed_view",
    comment="Transformed view of the raw PubMed data",
)
def pubmed_transformed_view():
    def create_date_column(year_col, month_col, day_col):
        return F.to_date(
            F.concat_ws("-", F.col(year_col), F.col(month_col), F.col(day_col))
        )

    def extract_id_by_type(id_type):
        return F.expr(
            f"filter(PubmedData.ArticleIdList.ArticleId, x -> x._IdType = '{id_type}')[0]._VALUE"
        )

    # Start transformation
    df = (
        dlt.read_stream("pubmed_raw_data")
        # basic article metadata
        .withColumn("title", F.col("MedlineCitation.Article.ArticleTitle"))
        .withColumn(
            "normalized_title",
            normalize_title_udf(F.col("MedlineCitation.Article.ArticleTitle")),
        )
        .withColumn("abstract", F.col("MedlineCitation.Article.Abstract.AbstractText"))
        .withColumn(
            "type",
            F.col("MedlineCitation.Article.PublicationTypeList.PublicationType._VALUE"),
        )
        # author information
        .withColumn(
            "authors",
            F.expr("""
                        transform(MedlineCitation.Article.AuthorList.Author, author -> struct(
                            author.ForeName as given,
                            author.LastName as family,
                            author.Initials as initials,
                            transform(
                                case when author.AffiliationInfo is not null 
                                    then array(author.AffiliationInfo.Affiliation) 
                                    else array() 
                                end, 
                                aff -> aff
                            ) as affiliations
                        ))
                    """)
        )
        # references
        .withColumn("references", F.col("PubmedData.ReferenceList"))
        # journal information
        .withColumn("source_title", F.col("MedlineCitation.Article.Journal.Title"))
        .withColumn(
            "source_issns", F.col("MedlineCitation.Article.Journal.ISSN._VALUE")
        )
        .withColumn(
            "volume", F.col("MedlineCitation.Article.Journal.JournalIssue.Volume")
        )
        .withColumn(
            "issue", F.col("MedlineCitation.Article.Journal.JournalIssue.Issue")
        )
        # ids
        .withColumn("doi", extract_id_by_type("doi"))
        .withColumn("pmc_id", extract_id_by_type("pmc"))
        .withColumn("references", F.col("PubmedData.ReferenceList"))
        # dates
        .withColumn(
            "publication_date",
            create_date_column(
                "MedlineCitation.Article.Journal.JournalIssue.PubDate.Year",
                "MedlineCitation.Article.Journal.JournalIssue.PubDate.Month",
                "MedlineCitation.Article.Journal.JournalIssue.PubDate.Day",
            ),
        )
        .withColumn(
            "created_date",
            create_date_column(
                "MedlineCitation.DateCompleted.Year",
                "MedlineCitation.DateCompleted.Month",
                "MedlineCitation.DateCompleted.Day",
            ),
        )
        .withColumn(
            "updated_date",
            create_date_column(
                "MedlineCitation.DateRevised.Year",
                "MedlineCitation.DateRevised.Month",
                "MedlineCitation.DateRevised.Day",
            ),
        )
    )

    return df.select(
        "pmid",
        "doi",
        "pmc_id",
        "title",
        "normalized_title",
        "abstract",
        "type",
        "authors",
        "source_title",
        "source_issns",
        "volume",
        "issue",
        "references",
        "publication_date",
        "created_date",
        "updated_date",
    )


dlt.create_target_table(
    name="pubmed_works",
    comment="Final PubMed articles table with unique PMIDs",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target="pubmed_works",
    source="pubmed_transformed_view",
    keys=["pmid"],
    sequence_by="updated_date",
)
