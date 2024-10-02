from dataclasses import asdict
from datetime import datetime

from pyspark.sql.functions import col, from_json
import shortuuid

from common.schemas import Author, CommonWork

def map_crossref_to_common_work(row):
    record_id = shortuuid.uuid()
    created_at = datetime.now().isoformat()
    updated_at = created_at

    # map authors from Crossref to CommonWork format
    authors = []
    if 'author' in row and row['author']:
        for i, author in enumerate(row['author']):
            given = author['given'] if 'given' in author else None
            family = author['family'] if 'family' in author else None
            authors.append(Author(given=given, family=family, sequence=i + 1))

    # handle 'title' field, which is an array; get the first element if available
    title = row['title'][0] if 'title' in row and row['title'] else ""

    # create a CommonWork instance
    common_work = CommonWork(
        id=record_id,
        title=title,
        doi=row['DOI'],
        type=row['type'],
        authors=authors,
        created=created_at,
        updated=updated_at
    )

    return asdict(common_work)

def transform(spark, raw_df, source_schema, target_schema, mapping_function):
    # parse the raw data
    parsed_df = raw_df.select(
        from_json(col("message"), source_schema).alias("source_message")
    ).select("source_message.*")

    # map parsed data using the provided mapping function
    target_rdd = parsed_df.rdd.map(mapping_function)

    # convert RDD to DataFrame using the target schema
    target_df = spark.createDataFrame(target_rdd, schema=target_schema)

    return target_df
