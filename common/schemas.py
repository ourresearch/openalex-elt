from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# CommonWork schema for transformed data
spark_common_work_schema = StructType([
    StructField("id", StringType(), False),
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

@dataclass
class Author:
    given: Optional[str] = None
    family: Optional[str] = None
    sequence: Optional[int] = None

@dataclass
class CommonWork:
    title: Optional[str] = None
    doi: Optional[str] = None
    type: Optional[str] = None
    authors: List[Author] = field(default_factory=list)
    created: str = field(default_factory=lambda: datetime.now().isoformat())
    updated: str = field(default_factory=lambda: datetime.now().isoformat())
