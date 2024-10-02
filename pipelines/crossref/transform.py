import logging
import sys

import dlt
from pyspark.sql.functions import from_json, col

sys.path.append('/Workspace/Shared/openalex-elt/common')
sys.path.append('/Workspace/Shared/openalex-elt/pipelines/crossref')

from schemas import Author, CommonWork, spark_common_work_schema
from mapping import crossref_schema, map_crossref_to_common_work

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dlt.table(
    comment="Transformed Crossref works data",
    table_properties={"quality": "silver"}
)
def crossref_transformed_data():
    raw_df = dlt.read("crossref_raw_data")

    parsed_df = raw_df.select(
        from_json(col("message"), crossref_schema).alias("crossref_message")
    ).select("crossref_message.*")

    # convert parsed data into common work schema
    common_work_rdd = parsed_df.rdd.map(lambda row: map_crossref_to_common_work(row.asDict()))

    # convert RDD back to DataFrame using the common work schema
    common_work_df = spark.createDataFrame(common_work_rdd, schema=spark_common_work_schema)

    return common_work_df
