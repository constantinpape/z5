from .file import File, N5File, ZarrFile, S3File
from .dataset import Dataset
from .group import Group
from .attribute_manager import set_json_encoder, set_json_decoder

__all__ = ['File', 'N5File', 'ZarrFile', 'S3File', 'Dataset', 'Group',
           'set_json_encoder', 'set_json_decoder']

# Version is single-sourced from include/z5/z5.hxx. CMake generates _version.py
# from those macros at build time (see src/python/_version.py.in), covering the
# make-install and in-place build-dir layouts. For wheel and editable installs
# the generated file isn't on the import path, so fall back to the installed
# package metadata, which scikit-build-core fills from the same z5.hxx macros.
try:
    from ._version import __version__, __version_info__
except ImportError:
    from importlib.metadata import PackageNotFoundError, version as _pkg_version
    try:
        __version__ = _pkg_version("z5py")
    except PackageNotFoundError:  # unbuilt source checkout
        __version__ = "0.0.0+unknown"
    __version_info__ = tuple(
        int(p) for p in __version__.split("+")[0].split(".") if p.isdigit()
    )
