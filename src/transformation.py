from pyspark.sql import SparkSession
from pyspark.sql import functions as SQL
from pathlib import Path
from functools import reduce
from abc import abstractmethod
import os, asyncio
from infra import Infraclass, Pipeline, Transform


class AuditingTransform(Transform):
    def __init__(self, transform_name, source_column, destination_column, primary_column, params):
        self.primary_column = primary_column
        self.source_column = source_column
        self.destination_column = destination_column
        self.params = params
        self.transform_name = transform_name

    def getTransformTag(self, pipeline_id, transform_index):
        quoted_params = { key: (f"'{value}'" if isinstance(value, str) else value) for key, value in self.params.items()}
        param_str = ", "+ ", ".join([f"{key}={value}" for key, value in quoted_params.items()]) if self.params else ""
        return f'{self.destination_column}#{transform_index+1} = {pipeline_id}.{self.transform_name}({self.source_column}#{transform_index}{param_str})'

    def apply(self, before_stream_df, spark_session, pipeline_id, transform_index):
        # apply the transform
        after_stream_df = self.apply_transform(before_stream_df, spark_session)
        # now create the audit record
        transform_tag = self.getTransformTag(pipeline_id, transform_index)
        before_cols, after_cols = before_stream_df.columns, after_stream_df.columns
        # consider error conditions for this transform
        if self.source_column and self.source_column not in before_cols:
            raise Exception(f'the given source column {self.source_column} for transform {transform_tag} was not found in the source datastream {before_cols}')
        elif self.destination_column not in after_cols:
            raise Exception(f'the given destination column {self.destination_column} for transform {transform_tag} must be present in the destination stream {after_cols}')
        # the new_value for audit is the same for all transforms: new_value = after_stream.destination_column
        new_value_df = after_stream_df.withColumnRenamed(self.destination_column, 'new_value')[[self.primary_column, 'new_value']]
        # at this stage we reach a point of interpretation. if the specification for the audit record had contained 'source_value' 
        # rather than 'old_value' then I would have done: 'source_value = before_stream.source_column' ie the value used by the 
        # transform to produce the destination column. however, 'old_value' implies the value that was overwritten by the 
        # transform. this latter interpretation is more like a Change Data Capture record. under this latter interpretation, 
        # 'old_value' will be NULL in cases where a new column is created, and the value of before_stream.destination_column otherwise
        if self.destination_column not in before_cols:
            # then the column is new one, we have not changed any original data, so we need a null literal for old_value
            old_value_df = before_stream_df.withColumn('old_value', SQL.lit(None))[[self.primary_column, 'old_value']]
        else:
            # the destination column is changing an existing column, so old_value = before_stream.destination_column 
            old_value_df = before_stream_df.withColumnRenamed(self.destination_column, 'old_value')[[self.primary_column, 'old_value']]
        # now join the old and new value columns to form the audit records
        audit_stream_df = old_value_df.join(new_value_df, on=self.primary_column, how='inner').withColumn('transform_tag', SQL.lit(transform_tag))
        return after_stream_df, audit_stream_df

    @abstractmethod
    def apply_transform(self, stream_df, spark_session):
        return stream_df


class TranslateTransform(AuditingTransform, Infraclass):
    def __init__(self, transform_name, source_column, destination_column, primary_column, fixtures_path, key_column, value_column):
        tag_params = dict(fixture=Path(fixtures_path).stem, key_column=key_column, value_column=value_column)
        super().__init__(transform_name, source_column, destination_column, primary_column, tag_params)
        self.fixtures_path, self.key_column, self.value_column = fixtures_path, key_column, value_column

    def apply_transform(self, stream_df, spark_session):
        translation_df = spark_session.read.option('inferSchema', 'true').option('header','true').parquet(self.fixtures_path)
        translation_df = translation_df.withColumnRenamed(self.key_column, self.source_column).withColumnRenamed(self.value_column, self.destination_column)
        translation_df.cache()
        transformed_stream = stream_df.join(translation_df, on=self.source_column, how='left')
        return transformed_stream

    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        primary_column = kwargs.get('primary_column', None)
        fixture_filepath = os.environ.get(meta_instance.fixture_filepath_env, default_env.get(meta_instance.fixture_filepath_env))
        return cls(meta_instance.transform_type, meta_instance.source_column, meta_instance.destination_column, primary_column, fixture_filepath, meta_instance.key_column, meta_instance.value_column)
    

class RedactTransform(AuditingTransform, Infraclass):
    def __init__(self, transform_name, source_column, destination_column, primary_column, replace_with):
        super().__init__(transform_name, source_column, destination_column, primary_column, dict(replace_with=replace_with))
        self.replace_with = replace_with
    
    def apply_transform(self, stream_df, spark_session):
        transformed_stream = stream_df.drop(self.source_column).withColumn(self.destination_column, SQL.lit(self.replace_with))
        return transformed_stream
    
    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        primary_column = kwargs.get('primary_column', None)
        return cls(meta_instance.transform_type, meta_instance.source_column, meta_instance.destination_column, primary_column, meta_instance.replace_with)


class SplitPickTransform(AuditingTransform, Infraclass):
    def __init__(self, transform_name, source_column, destination_column, primary_column, split_by, pick_index):
        tag_params = dict(split_by=split_by, pick_index=pick_index)
        super().__init__(transform_name, source_column, destination_column, primary_column, tag_params)
        self.split_by, self.pick_index = split_by, pick_index

    def apply_transform(self, stream_df, spark_session):
        split_col = SQL.split(stream_df[self.source_column], self.split_by)
        transformed_stream = stream_df.withColumn(self.destination_column, split_col.getItem(0))
        return transformed_stream
    
    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        primary_column = kwargs.get('primary_column', None)
        return cls(meta_instance.transform_type, meta_instance.source_column, meta_instance.destination_column, primary_column, meta_instance.split_by, meta_instance.pick_index)


class TransformerPipeline(Infraclass, Pipeline):
    def __init__(self, pipeline_id, source, sink, audit_sink, transforms):
        self.sink, self.audit_sink = sink, audit_sink
        maven_coords = 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2,org.apache.kafka:kafka-clients:3.9.0,io.delta:delta-spark_2.13:4.0.0rc1'
        self.spark = SparkSession.builder.appName(pipeline_id) \
                    .config('spark.jars.packages', maven_coords) \
                    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
                    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
                    .config('spark.sql.streaming.checkpointLocation', '/tmp/checkpoints') \
                    .getOrCreate()
        # self.spark.conf.set('spark.sql.shuffle.partitions', '5')
        # self.spark.conf.set("spark.sql.streaming.checkpointLocation", '/tmp/checkpoints')
        self.spark.sparkContext.setLogLevel("WARN")
        self.transformed_stream = source.connect(self.spark)
        audit_list = []
        for transform_index, transform in enumerate(transforms):
            self.transformed_stream, audit_stream = transform.apply(self.transformed_stream, self.spark, pipeline_id, transform_index)
            audit_list.append(audit_stream)
        if audit_list:
            self.audit_stream = reduce(lambda df1, df2: df1.union(df2), audit_list[1:], audit_list[0])

    def explain(self):
        self.transformed_stream.explain()
        self.audit_stream.explain()
    
    def run(self):
        self.query = self.sink.connect(self.transformed_stream)
        self.audit_query = self.audit_sink.connect(self.audit_stream)
        return self

    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        source = meta_instance.source.infraclass.bootstrap(meta_instance.source, metastore, default_env)
        sink = meta_instance.sink.infraclass.bootstrap(meta_instance.sink, metastore, default_env)
        audit = meta_instance.audit.infraclass.bootstrap(meta_instance.audit, metastore, default_env)
        transforms = [transform_meta_instance.infraclass.bootstrap(transform_meta_instance, metastore, default_env, primary_column=source.primary_column) for transform_meta_instance in meta_instance.transforms]
        return cls(meta_instance.id, source, sink, audit, transforms)
