from datetime import datetime
from dataclasses import dataclass, field, asdict
import logging
from typing import List, Optional
import dlt
from pyspark.sql.functions import from_json, col, struct, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql import Row
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the schema for the Crossref raw data
crossref_schema = StructType([
    StructField("DOI", StringType(), True),
    StructField("title", ArrayType(StringType()), True),
    StructField("author", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("family", StringType(), True),
        StructField("given", StringType(), True),
        StructField("ORCID", StringType(), True)
    ])), True),
    StructField("abstract", StringType(), True),
    StructField("type", StringType(), True)
])

spark_common_work_schema = StructType([
    StructField("title", StringType(), True),
    StructField("doi", StringType(), True),
    StructField("type", StringType(), True),
    StructField("authors", ArrayType(StructType([
        StructField("given", StringType(), True),
        StructField("family", StringType(), True),
        StructField("sequence", IntegerType(), True)
    ])), True),
    StructField("created", StringType(), False),
    StructField("updated", StringType(), False)
])

class CrossrefWork:
    def __init__(self, df):
        self.df = df

    @property
    def field_mapping(self):
        return {
            "doi": "DOI",
            "title": "title",
            "type": "type",
            "authors": "author"
        }

    def set_root_fields(self):
        for field_name, field_value in self.field_mapping.items():
            self.df = self.df.withColumn(field_name, col(field_value))

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
    comment="Transformed Crossref works data",
    table_properties={"quality": "silver"}
)
def crossref_work():
    raw_df = dlt.read("crossref_raw_data")
    
    parsed_df = raw_df.select(
        from_json(col("message"), crossref_schema).alias("crossref_message")
    ).select("crossref_message.*")
    
    crossref_work = CrossrefWork(parsed_df)

    df_normalized = crossref_work.transform()
    return df_normalized