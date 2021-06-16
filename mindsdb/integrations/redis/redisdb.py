import walrus

from mindsdb.integrations.base import StreamIntegration
import mindsdb.interfaces.storage.db as db
from mindsdb.streams import RedisStream, StreamController


class RedisConnectionChecker:
    def __init__(self, **params):
        self.connection_info = params['connection']

    def check_connection(self):
        try:
            client = walrus.Database(**self.connection_info)
            client.dbsize()
        except Exception:
            return False
        else:
            return False


class Redis(StreamIntegration, RedisConnectionChecker):
    def __init__(self, config, name, db_info):
        StreamIntegration.__init__(self, config, name)
        self.connection_info = db_info['connection']
    
    def _make_stream(self, s: db.Stream) -> StreamController:
        return StreamController(
            s.name,
            s.predictor,
            stream_in=RedisStream(s.stream_in, self.connection_info),
            stream_out=RedisStream(s.stream_out, self.connection_info),
            anomaly_stream=RedisStream(s.anomaly_stream) if s.anomaly_stream is not None else None,
            learning_stream=RedisStream(s.learning_stream) if s.learning_stream is not None else None,
        )
