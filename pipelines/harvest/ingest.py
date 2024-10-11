from inspect import getmembers

import boto3
import dlt
from dataclasses import asdict, is_dataclass
from openalex_taxicab.harvest import HarvestResult

from openalex_taxicab.legacy.harvest import PublisherLandingPageHarvester

from common.utils import explode_dict_column

plp_harvester = PublisherLandingPageHarvester(boto3.client('s3'))

def dict_factory_with_properties(obj):
    if is_dataclass(obj):
        result = {}
        for key, value in obj.__dict__.items():
            result[key] = dict_factory_with_properties(value)
        for name, value in getmembers(obj.__class__, lambda o: isinstance(o, property)):
            result[name] = dict_factory_with_properties(value.__get__(obj))
        return result
    elif isinstance(obj, (list, tuple)):
        return type(obj)(dict_factory_with_properties(v) for v in obj)
    elif isinstance(obj, dict):
        return {k: dict_factory_with_properties(v) for k, v in obj.items()}
    else:
        return obj

def harvest(doi):
    result: HarvestResult = plp_harvester.harvest(doi)
    d = asdict(result, dict_factory=dict_factory_with_properties)
    d.pop('content', None)
    return d


@dlt.table(
    name="harvested_content",
    comment="Metadata about harvested URLs (S3 path, response code, etc)",
    table_properties={'quality': 'bronze'}
)
def harvested_content():
    crossref_df = dlt.read_stream("crossref_works")
    dois_df = crossref_df.select('DOI')
    harvest_result_df = dois_df.withColumn('harvest_result', harvest(dois_df['DOI']))
    exploded_df = explode_dict_column(harvest_result_df, 'harvest_result')
    return exploded_df

