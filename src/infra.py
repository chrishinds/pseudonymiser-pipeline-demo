from abc import ABC, abstractmethod


class Infraclass(ABC):
    @classmethod
    @abstractmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        return cls()

class Pipeline(ABC):
    @abstractmethod
    def run(self):
        return None

class IO(ABC):
    @abstractmethod
    def connect(self, upstream):
        downstream = upstream
        return downstream


class Transform(ABC):
    @abstractmethod
    def apply(self, before_stream_df, spark_session, pipeline_id, transform_index):
        result_stream_df, audit_stream_df = before_stream_df, before_stream_df
        return result_stream_df, audit_stream_df

