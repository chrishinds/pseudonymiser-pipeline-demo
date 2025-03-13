from typing import Literal, Union, Dict, Type
from pydantic import Field, BaseModel, ConfigDict
from typing_extensions import Annotated
from argparse import ArgumentParser
from pathlib import Path
import json, yaml, os
import production, transformation, sparkio

#
# Base Model, for global pydantic config
#

class MetaBase(BaseModel):
    model_config = ConfigDict(extra='forbid')


#
# Pipeline Models
#

class KafkaSource(MetaBase):
    source_type: Literal['kafka']
    connector_id: str
    topic: str
    infraclass: Type[sparkio.KafkaSparkSource] = sparkio.KafkaSparkSource

SourceType = Annotated[Union[KafkaSource], Field(discriminator='source_type')]

class DeltaLakeSink(MetaBase):
    sink_type: Literal['deltalake']
    connector_id: str
    table: str
    trigger_seconds: int
    coalesce: int
    infraclass: Type[sparkio.DeltaRsSparkSink] = sparkio.DeltaRsSparkSink

class MemorySink(MetaBase):
    sink_type: Literal['memory']
    query_name: str
    query_interval: int
    duration_seconds: int
    infraclass: Type[sparkio.SparkMemorySink] = sparkio.SparkMemorySink

class FileSink(MetaBase):
    sink_type: Literal['file']
    filepath_env: str
    format: str
    trigger_seconds: int
    coalesce: int
    infraclass: Type[sparkio.SparkFileSink] = sparkio.SparkFileSink

SinkType = Annotated[Union[DeltaLakeSink, MemorySink, FileSink], Field(discriminator='sink_type')]

class Transform(MetaBase):
    source_column: str
    destination_column: str

class Translate(Transform):
    transform_type: Literal['translate']
    fixture_filepath_env: str
    key_column: str
    value_column: str
    infraclass: Type[transformation.TranslateTransform] = transformation.TranslateTransform

class Redact(Transform):
    transform_type: Literal['redact']
    replace_with: str
    infraclass: Type[transformation.RedactTransform] = transformation.RedactTransform

class SplitPick(Transform):
    transform_type: Literal['split_pick']
    split_by: str
    pick_index: int
    infraclass: Type[transformation.SplitPickTransform] = transformation.SplitPickTransform

TransformType = Annotated[Union[Translate, Redact, SplitPick], Field(discriminator='transform_type')]

class TransformerPipeline(MetaBase):
    id: str 
    pipeline_type: Literal['transformer']
    source: SourceType
    sink: SinkType
    audit: SinkType
    transforms: list[TransformType]
    infraclass: Type[transformation.TransformerPipeline] = transformation.TransformerPipeline

class KafkaProducerPipeline(MetaBase):
    id: str
    pipeline_type: Literal['kafka_producer']
    source_filepath_env: str
    sink_topic: str
    interval_seconds_mean: float
    interval_seconds_sd: float 
    infraclass: Type[production.KafkaFileProducer] = production.KafkaFileProducer

PipelineType = Annotated[Union[TransformerPipeline, KafkaProducerPipeline], Field(discriminator='pipeline_type')]


#
# Schema Models
#

SparkType = Literal['integer', 'short', 'long', 'float', 'double', 'date', 'timestamp', 'boolean', 'string']

class Schema(MetaBase):
    name: str
    fields: Dict[str, SparkType]
    primary_field: str 

    def sparkSchemaDict(self):
        json_schema_dict = {
            'type': 'struct',
            'fields': [{'metadata':{}, 'name':field_name, 'type':field_type, 'nullable':False} for field_name, field_type in self.fields.items()]
        }
        return json_schema_dict


#
# Connector Models
#

class KafkaTopic(MetaBase):
    topic_name: str
    topic_schema: str

class KafkaConnector(MetaBase):
    id: str
    connector_type: Literal['kafka']
    broker_env: str
    topics: list[KafkaTopic]

    def getTopic(self, name):
        return next((x for x in self.topics if x.topic_name == name), None)

class DeltaLakeTable(MetaBase):
    table_name: str
    table_schema: str
    filepath_env: str
    partition_on: str

class DeltaLakeConnector(MetaBase):
    id: str
    connector_type: Literal['deltalake']
    tables: list[DeltaLakeTable]

    def getTable(self, name):
        return next((x for x in self.tables if x.table_name == name), None)

ConnectorType = Annotated[Union[KafkaConnector, DeltaLakeConnector], Field(discriminator='connector_type')]


#
# Meta Store Model
#

class Store(MetaBase):
    schemas: list[Schema]
    connectors: list[ConnectorType] 
    pipelines: list[PipelineType]

    def getSchema(self, name):
        return next((x for x in self.schemas if x.name == name), None)

    def getConnector(self, id):
        return next((x for x in self.connectors if x.id == id), None)

    def getPipeline(self, id):
        return next((x for x in self.pipelines if x.id == id), None)

    @classmethod
    def load(cls, environment_name='METASTORE_FILEPATH', filepath=None):
        # use the filepath if given, else check environment, otherwise assume the default filename and location
        metastore_filepath = filepath if filepath else os.environ.get(environment_name, Path('./metastore.yml'))
        metastore_yaml = cls._load_yaml(metastore_filepath)
        return Store.model_validate_json(json.dumps(metastore_yaml))

    @staticmethod
    def _load_yaml(filepath): 
        with open(filepath) as f:
            return yaml.safe_load(f.read())



#
# Run a pipeline eg. %python meta.py --filepath metastore.yml --run patient_producer
#

arg_parser = ArgumentParser(prog='meta', description='load and validate metastore schema from yaml')
arg_parser.add_argument('-f', '--filepath', type=str, default='./metastore.yml', help='path to a yaml metastore file')
arg_parser.add_argument('-r', '--run', type=str, default=None, help='run a pipeline with the given metastore id')

if __name__ == '__main__':
    args = arg_parser.parse_args()
    metastore = Store.load(filepath=args.filepath)
    if args.run:
        print(f'Running "{args.run}"')
        meta_instance = metastore.getPipeline(args.run)
        pipeline = meta_instance.infraclass.bootstrap(meta_instance, metastore, {}).run()
        pipeline.audit_query.awaitTermination()
    print('Completed.')
