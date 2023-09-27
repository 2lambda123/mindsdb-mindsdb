from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities.log import get_log
from .__about__ import __version__ as version, __description__ as description

logger = get_log()

try:
    from .pytorch_tabular_handler import Pytorch_Tabular_Handler as Handler
    import_error = None
    logger.info("PyTorchTabular Handler engine successfully imported")
except Exception as e:
    Handler = None
    import_error = e

title = 'PyTorch_Tabular'
name = 'pytorch_tabular'
type = HANDLER_TYPE.ML
permanent = True

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'description', 'import_error'
]