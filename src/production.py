import os, json, random, signal, threading
from kafka import KafkaProducer
from infra import Infraclass, Pipeline

class KafkaFileProducer(Infraclass, Pipeline):
    def __init__(self, name, filepath, topic, broker, interval_mean, interval_sd, primary_field):
        self.exit = threading.Event()
        self.broker = KafkaProducer(bootstrap_servers=broker, value_serializer=self.serializer)
        self.name, self.filepath, self.topic, self.interval_mean, self.interval_sd, self.primary_field = name, filepath, topic, interval_mean, interval_sd, primary_field
    
    @staticmethod
    def serializer(msg):
        return json.dumps(msg).encode('utf-8')
    
    def exitHandlerFactory(self):
        def exit_handler(signum, _):
            print(f'Exiting Producer on signal {signum}')
            self.exit.set()
        return exit_handler

    # NOTE: this breaks the infra.IO interface: meta.__main__ assumes that we can awaitTermination() on the return value
    def run(self):
        with open(self.filepath) as f:
            for line_index, line in enumerate(f):
                if not self.exit.is_set():
                    try:
                        msg = json.loads(line)
                        record_id = int(msg[self.primary_field])
                        print(f'Pipeline {self.name} emitting record id: {record_id} to {self.topic}')
                        # not maximally efficient to have parsed to json then set a serializer
                        self.broker.send(self.topic, msg)
                        self.exit.wait(abs(random.gauss(mu=self.interval_mean, sigma=self.interval_sd)))
                    except Exception as e:
                        print(f'Unable to parse record at line {line_index} of {self.filepath}: {e}')
        if not self.exit.is_set():
            print(f'Producer Completed')
    
    @classmethod
    def bootstrap(cls, meta_instance, metastore, default_env, **kwargs):
        kafka_id, kafka_topic = meta_instance.sink_topic.split('.')
        meta_kafka = metastore.getConnector(kafka_id)
        args = dict(
            name= meta_instance.id,
            filepath = os.environ.get(meta_instance.source_filepath_env, default_env.get(meta_instance.source_filepath_env)),
            broker = os.environ.get(meta_kafka.broker_env, default_env.get(meta_kafka.broker_env)),
            topic = kafka_topic,
            interval_mean = meta_instance.interval_seconds_mean,
            interval_sd = meta_instance.interval_seconds_sd,
            primary_field = metastore.getSchema(meta_kafka.getTopic(kafka_topic).topic_schema).primary_field
        )
        producer = cls(**args)
        # connect the data source to local OS signals, so we can stop emitting events if requested
        signal.signal(signal.SIGTERM, producer.exitHandlerFactory())
        signal.signal(signal.SIGINT, producer.exitHandlerFactory())
        signal.signal(signal.SIGHUP, producer.exitHandlerFactory())
        return producer
