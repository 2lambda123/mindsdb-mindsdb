import kafka

from mindsdb.integrations.base import StreamIntegration
import mindsdb.interfaces.storage.db as db
from mindsdb.streams import KafkaStream, StreamController


class KafkaConnectionChecker:
    def __init__(self, **params):
        self.connection_info = {
            'host': params['connection']['host'],
            'port': params['connection']['port'],
            'password': params['connection']['password'],
        }

    def check_connection(self):
        try:
            client = kafka.KafkaClient(**self.connection_info)
        except Exception:
            return False
        else:
            client.close()
            return True


class Kafka(StreamIntegration, KafkaConnectionChecker):
    def __init__(self, config, name, db_info):
        StreamIntegration.__init__(self, config, name)
        self.connection_info = {
            'bootstrap_servers': [str(x) for x in db_info['connection']['bootstrap_servers']]
        }

    def _make_stream(self, s: db.Stream):
        return StreamController(
            s.name,
            s.predictor,
            self.connection_info,
            stream_in=KafkaStream(s.stream_in),
            stream_out=KafkaStream(s.stream_out),
            learning_stream=KafkaStream(s.learning_stream) if s.learning_stream is not None else None,
            anomaly_stream=KafkaStream(s.anomaly_stream) if s.anomaly_stream is not None else None,
        )
