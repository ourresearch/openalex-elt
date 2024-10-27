import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


doaj_schema = StructType([
    StructField("header", StructType([
        StructField("identifier", StringType(), True),
        StructField("datestamp", StringType(), True),
        StructField("setSpec", ArrayType(StringType()), True)
    ])),
    StructField("metadata", StructType([
        StructField("oai_dc:dc", StructType([
            StructField("dc:title", StringType(), True),
            StructField("dc:identifier", ArrayType(StringType()), True),
            StructField("dc:date", StringType(), True),
            StructField("dc:relation", ArrayType(StringType()), True),
            StructField("dc:description", StringType(), True),
            StructField("dc:creator", ArrayType(StringType()), True),
            StructField("dc:publisher", StringType(), True),
            StructField("dc:type", StringType(), True),
            StructField("dc:subject", ArrayType(StringType()), True),
            StructField("dc:language", ArrayType(StringType()), True),
            StructField("dc:source", StringType(), True)
        ]))
    ]))
])


@dlt.table(
    comment="Parsed DOAJ article XML data from S3 into Delta table"
)
def doaj_articles():
    return (
        spark.read.format("xml")
        .schema(doaj_schema)
        .load("s3://openalex-ingest/doaj/articles")
        .select(
            col("header.identifier").alias("article_id"),
            col("header.datestamp").alias("datestamp"),
            col("header.setSpec").alias("categories"),
            col("metadata.oai_dc:dc.dc:title").alias("title"),
            col("metadata.oai_dc:dc.dc:identifier").alias("identifiers"),
            col("metadata.oai_dc:dc.dc:date").alias("publication_date"),
            col("metadata.oai_dc:dc.dc:relation").alias("related_links"),
            col("metadata.oai_dc:dc.dc:description").alias("description"),
            col("metadata.oai_dc:dc.dc:creator").alias("authors"),
            col("metadata.oai_dc:dc.dc:publisher").alias("publisher"),
            col("metadata.oai_dc:dc.dc:type").alias("type"),
            col("metadata.oai_dc:dc.dc:subject").alias("subjects"),
            col("metadata.oai_dc:dc.dc:language").alias("languages"),
            col("metadata.oai_dc:dc.dc:source").alias("source")
        )
    )
