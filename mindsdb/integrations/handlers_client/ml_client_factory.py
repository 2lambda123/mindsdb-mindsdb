import os
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.integrations.handlers_client.ml_grpc_client import MLClientGRPC
from mindsdb.integrations.libs.handler_helpers import get_handler
from mindsdb.utilities.log import get_log


logger = get_log(logger_name="main")


class MLClientFactory:
    def __init__(self):
        self.client_class = MLClientGRPC

        self.host = os.environ.get("MINDSDB_ML_SERVICE_HOST")
        self.port = os.environ.get("MINDSDB_ML_SERVICE_PORT")

    def __call__(self, **kwargs: dict):
        # _type = kwargs["integration_engine"] or kwargs.get("name", None)
        if self.host is None or self.port is None:
            logger.info(
                "%s.__call__: no post/port to MLService have provided. Handle all db request locally",
                self.__class__.__name__,
            )
            return BaseMLEngineExec(**kwargs)
        
        logger.info("%s.__call__: api to communicate with db services - gRPC",
                    self.__class__.__name__,
                    )

        return self.client_class(**kwargs)


MLClient = MLClientFactory()
