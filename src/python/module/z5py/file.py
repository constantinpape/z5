import os
import errno
import json
from shutil import rmtree

from .group import Group

# set correct json error type for python 2 / 3
try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


class File(Group):
    """ File to access zarr or n5 containers on disc.

    Supports python dict api.

    The container corresponds to a directory on the filesystem.
    Group are subdirectories and datasets are subdirectories
    that contain multi-dimensional data stored in binary format.

    Args:
        path (str): path on filesystem that holds the container
        use_zarr_format (bool): flag to determine if container is zarr or n5 (default: None)
        mode (str): file mode used to open / create the file
    """

    #: file extensions that are inferred as zarr file
    zarr_exts = {'.zarr', '.zr'}
    #: file extensions that are inferred as n5 file
    n5_exts = {'.n5'}

    @classmethod
    def infer_format(cls, path):
        """ Infer the file format from the file extension.

            Returns: `True` for zarr, `False` for n5 and `None` if the format could not be infered.
        """
        # if the path exists infer the format from the metadata
        if os.path.exists(path):
            zarr_group = os.path.join(path, '.zgroup')
            zarr_array = os.path.join(path, '.zarray')
            return os.path.exists(zarr_group) or os.path.exists(zarr_array)
        # otherwise check if we can infer it from the file ending
        else:
            is_zarr = None
            _, ext = os.path.splitext(path)
            if ext.lower() in cls.zarr_exts:
                is_zarr = True
            elif ext.lower() in cls.n5_exts:
                is_zarr = False
            return is_zarr

    def __init__(self, path, use_zarr_format=None, mode='a'):

        # infer the file format from the path
        is_zarr = self.infer_format(path)
        # check if the format that was infered is consistent with `use_zarr_format`
        if use_zarr_format is None:
            if is_zarr is None:
                raise RuntimeError("Cannot infer the file format (zarr or N5)")
            use_zarr_format = is_zarr

        elif use_zarr_format:
            if not is_zarr:
                raise RuntimeError("N5 file cannot be opened in zarr format")

        else:
            if is_zarr:
                raise RuntimeError("Zarr file cannot be opened in N5 format")
        super(File, self).__init__(path, use_zarr_format, mode)

        # check if the file already exists and load if it does
        if os.path.exists(path):

            # throw error if the file must not exist according to file mode
            if self._permissions.must_not_exist():
                raise OSError(errno.EEXIST, os.strerror(errno.EEXIST), path)

            if self._permissions.should_truncate():
                rmtree(path)
                os.mkdir(path)

            # TODO check zarr version as well
            if not use_zarr_format:
                self._check_n5_version(path)

        # otherwise create a new file
        else:
            if not self._permissions.can_create():
                raise OSError(errno.EROFS, os.strerror(errno.EROFS), path)
            os.mkdir(path)
            meta_file = os.path.join(path, '.zgroup' if use_zarr_format else 'attributes.json')
            # we only need to write meta data for the zarr format
            if use_zarr_format:
                with open(meta_file, 'w') as f:
                    json.dump({'zarr_format': 2}, f)
            else:
                with open(meta_file, 'w') as f:
                    json.dump({'n5': "2.0.0"}, f)

    @staticmethod
    def _check_n5_version(path):
        have_version_tag = False
        attrs_file = os.path.join(path, 'attributes.json')
        # check if we have an attributes
        if os.path.exists(attrs_file):
            with open(attrs_file, 'r') as f:
                try:
                    attrs = json.load(f)
                except JSONDecodeError:
                    attrs = {}
            # check if attributes have a version tag
            if 'n5' in attrs:
                tag = attrs['n5']
                have_version_tag = True
        # TODO check for proper version command with regex
        if have_version_tag:
            major_version = int(tag[0])
            if major_version > 2:
                raise RuntimeError("Can't open n5 file with major version bigger than 2")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class N5File(File):
    """ File to access n5 containers on disc.

    Args:
        path (str): path on filesystem that holds the container
        mode (str): file mode used to open / create the file
    """

    def __init__(self, path, mode='a'):
        super(N5File, self).__init__(path=path, use_zarr_format=False, mode=mode)


class ZarrFile(File):
    """ File to access zarr containers on disc.

    Args:
        path (str): path on filesystem that holds the container
        mode (str): file mode used to open / create the file
    """

    def __init__(self, path, mode='a'):
        super(ZarrFile, self).__init__(path=path, use_zarr_format=True, mode=mode)
