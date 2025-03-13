import os, asyncio, time, json
from abc import ABC, abstractmethod
from pyspark.sql.types import StructType
from pyspark.sql import functions as SQL
import infra
import deltalake
from datetime import datetime


class KafkaSpark(ABC):
    def __init__(self, broker_config, topic, schema, primary_column):
        self.broker_config = broker_config
        self.topic = topic
        self.schema = schema
        self.primary_column = primary_column

    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        topic = meta_instance.topic
        connector = metastore.getConnector(meta_instance.connector_id)
        meta_schema_instance = metastore.getSchema(connector.getTopic(topic).topic_schema)
        schema = StructType.fromJson(meta_schema_instance.sparkSchemaDict())
        broker_config = os.environ.get(connector.broker_env, default_env.get(connector.broker_env))
        return cls(broker_config, topic, schema, meta_schema_instance.primary_field)


class KafkaSparkSource(KafkaSpark, infra.Infraclass, infra.IO):
    def connect(self, spark_session):
        df = spark_session.readStream.format("kafka").option("kafka.bootstrap.servers", self.broker_config).option("subscribe", self.topic).option('includeHeaders', 'true').load()
        df = df.selectExpr("CAST(value AS STRING)")
        return df.withColumn('value', SQL.from_json('value', self.schema)).select(SQL.col('value.*'))
    

class SparkMemorySink(infra.Infraclass, infra.IO):
    def __init__(self, query_name, query_interval, duration_seconds):
        self.query_name = query_name
        self.query_interval = query_interval
        self.duration_seconds = duration_seconds

    # this async generator is neat, but it breaks the infra.IO interface and won't work in meta.__main__
    async def connect(self, stream):
        stream.writeStream.format('memory').queryName(self.query_name).outputMode('append').start()
        for seconds in [self.query_interval]*int(self.duration_seconds/self.query_interval):
            await asyncio.sleep(seconds)
            yield stream.sparkSession.sql(f'select * from {self.query_name}').toPandas()

    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        # bootstrapped classes have a timestamp attached to their query names, 
        # transforms now set checkpoints for everything, which get left behind, and memory queries don't support resumption
        return cls(f'{meta_instance.query_name}_{str(time.time()).replace('.','_')}', meta_instance.query_interval, meta_instance.duration_seconds)


class SparkFileSink(infra.Infraclass, infra.IO):
    def __init__(self, filepath, format, trigger_seconds, coalesce):
        self.filepath = filepath
        self.format = format
        self.trigger_seconds = trigger_seconds
        self.coalesce = coalesce
    
    def connect(self, stream):
        return stream.coalesce(self.coalesce).writeStream.format(self.format).option('path', self.filepath).trigger(processingTime=f'{self.trigger_seconds} seconds').outputMode('append').start()

    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        filepath = os.environ.get(meta_instance.filepath_env, default_env.get(meta_instance.filepath_env))
        return cls(filepath, meta_instance.format, meta_instance.trigger_seconds, meta_instance.coalesce)


class DeltaRsSparkSink(infra.Infraclass, infra.IO):
    def __init__(self, filepath, table, pa_schema, partition_on, trigger_seconds, coalesce, primary_column):
        self.filepath = filepath
        self.table_name = table
        self.schema = pa_schema
        self.partition_on = partition_on
        self.trigger_seconds = trigger_seconds
        self.coalesce = coalesce
        self.primary_column = primary_column
        if not deltalake.DeltaTable.is_deltatable(self.filepath):
            deltalake.write_deltalake(self.filepath, data=[], schema=self.schema, partition_by=self.partition_on)
        self.deltatable = deltalake.DeltaTable(self.filepath)

    def batchFunctionFactory(self):
        upsert_fields = { f'{k.name}': f'source.{k.name}' for k in self.schema }
        predicate = f'target.batch_id = source.batch_id AND target.{self.primary_column} = source.{self.primary_column}'
        def batchFunction(batch_df, batch_id):
            df = batch_df.toPandas()
            df['batch_id'] = batch_id
            df['inserted_at'] = datetime.now()
            self.deltatable.merge(source=df, source_alias='source', target_alias='target', predicate=predicate) \
                .when_matched_update(updates=upsert_fields) \
                .when_not_matched_insert(updates=upsert_fields) \
                .execute()
        return batchFunction
                             
    def connect(self, stream):
        return stream.coalesce(self.coalesce).writeStream \
                    .trigger(processingTime=f'{self.trigger_seconds} seconds') \
                    .outputMode('append').foreachBatch(self.batchFunctionFactory()).start()

    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        connector = metastore.getConnector(meta_instance.connector_id)
        table_name = meta_instance.table
        meta_table_instance = connector.getTable(table_name)
        meta_schema_instance = metastore.getSchema(meta_table_instance.table_schema)
        schema = deltalake.Schema.from_json(json.dumps(meta_schema_instance.sparkSchemaDict())).to_pyarrow(as_large_types=False)
        filepath = os.environ.get(meta_table_instance.filepath_env, default_env.get(meta_table_instance.filepath_env))
        return cls(filepath, table_name, schema, meta_table_instance.partition_on, meta_instance.trigger_seconds, meta_instance.coalesce, meta_schema_instance.primary_field)
