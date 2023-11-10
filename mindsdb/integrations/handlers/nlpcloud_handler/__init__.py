from mindsdb.integrations.libs.const import HANDLER_TYPE

from mindsdb.integrations.handlers.nlpcloud_handler.__about__ import (
    __version__ as version,
    __description__ as description,
)

try:
    from .nlpcloud_handler import NLPCloudHandler as Handler

    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "NLPCloud"
name = "nlpcloud"
type = HANDLER_TYPE.ML
permanent = True

__all__ = ["Handler", "version", "name", "type", "title", "description", "import_error"]