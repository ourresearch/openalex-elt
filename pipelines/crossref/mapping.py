from dataclasses import asdict
from datetime import datetime

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from common.schemas import Author, CommonWork

crossref_schema = StructType([
    StructField("DOI", StringType(), True),
    StructField("title", ArrayType(StringType()), True),
    StructField("author", ArrayType(StructType([
        StructField("family", StringType(), True),
        StructField("given", StringType(), True),
    ])), True),
    StructField("type", StringType(), True)
])


def map_crossref_to_common_work(row):
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
        title=title,
        doi=row['DOI'],
        type=row['type'],
        authors=authors,
        created=created_at,
        updated=updated_at
    )

    return asdict(common_work)
