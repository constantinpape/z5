from .file import File, N5File, ZarrFile, S3File
from .dataset import Dataset
from .group import Group
from .attribute_manager import set_json_encoder, set_json_decoder

__version__ = "2.0.15"
__version_info__ = tuple(int(i) for i in __version__.split("."))
