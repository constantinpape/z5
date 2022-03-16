import json
import os
import errno

from . import _z5py
from .group import Group


class File(Group):
    """ File to access zarr or n5 containers on disc.

    The container corresponds to a directory on the filesystem.
    Groups are subdirectories and datasets are subdirectories
    that contain multi-dimensional data stored in binary format.
    Supports python dict api.

    Args:
        path (str): path on filesystem that holds the container.
        mode (str): file mode used to open / create the file (default: 'a').
        use_zarr_format (bool): flag to determine if container is zarr or n5 (default: None).
    """

    #: file extensions that are inferred as zarr file
    zarr_exts = {'.zarr', '.zr'}
    #: file extensions that are inferred as n5 file
    n5_exts = {'.n5'}

    @classmethod
    def infer_format(cls, path):
        """ Infer the file format from the file extension.

            Returns:
                bool: `True` for zarr, `False` for n5 and `None` if the format could not be inferred.
        """
        # first, try to infer the format from the file ending
        is_zarr = None
        _, ext = os.path.splitext(path)
        if ext.lower() in cls.zarr_exts:
            is_zarr = True
        elif ext.lower() in cls.n5_exts:
            is_zarr = False
        # otherwise, infer from the existence of zarr attribute files
        if is_zarr is None:
            zarr_group = os.path.join(path, '.zgroup')
            zarr_array = os.path.join(path, '.zarray')
            is_zarr = os.path.exists(zarr_group) or os.path.exists(zarr_array)
        return is_zarr

    def __init__(self, path, mode="a", use_zarr_format=None, dimension_separator="."):

        if isinstance(path, os.PathLike):
            path = os.fspath(path)
        # infer the file format from the path
        is_zarr = self.infer_format(path)
        # check if the format that was inferred is consistent with `use_zarr_format`
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

        handle = _z5py.File(path, _z5py.FileMode(self.file_modes[mode]))
        mode = handle.mode()

        super().__init__(handle, _z5py.Group, parent=self, name="", dimension_separator=dimension_separator)

        # at some point we should move more of this logic to c++ as well
        # if we open in 'w', remove the existing file existing
        if handle.exists() and mode.should_truncate():
            handle.remove()
        if handle.exists():
            if mode.must_not_exist():
                raise OSError(errno.EEXIST, os.strerror(errno.EEXIST), handle.path())
            self._check_version()
        else:
            if not mode.can_create():
                raise OSError(errno.EROFS, os.strerror(errno.EROFS), handle.path())
            # if we don't have the file, create it
            _z5py.create_file(handle, is_zarr)

    def _check_version(self):
        metadata = self._handle.read_metadata()
        metadata = json.loads(metadata)
        if self._handle.is_zarr():
            version = metadata['zarr_format']
            if version != 2:
                raise RuntimeError("z5 only supports zarr format 2")
        else:
            version = metadata.get('n5', None)
            if version is not None:
                major_version = int(version[0])
                if major_version > 2:
                    raise RuntimeError("Can't open n5 file with major version bigger than 2")

    def close(self):
        # This function exists just for conformity with the standard file-handling procedure.
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def filename(self):
        return self._handle.path()

    # we need to override this from group
    @property
    def name(self):
        return '/'


class N5File(File):
    """ File to access n5 containers on disc.

    Args:
        path (str): path on filesystem that holds the container.
        mode (str): file mode used to open / create the file (default: 'a').
    """

    def __init__(self, path, mode='a'):
        super().__init__(path=path, use_zarr_format=False, mode=mode)


class ZarrFile(File):
    """ File to access zarr containers on disc.

    Args:
        path (str): path on filesystem that holds the container.
        mode (str): file mode used to open / create the file (default: 'a').
    """

    def __init__(self, path, mode="a", dimension_separator="."):
        super().__init__(path=path, use_zarr_format=True, mode=mode, dimension_separator=dimension_separator)


# TODO can we implement automatic zarr/n5 inference for s3?
class S3File(Group):
    """ File to access zarr/n5 file in AWS S3 bucket.

    Args:
    """
    # TODO refactor aws args in some config object?
    def __init__(self, bucket_name,
                 name_in_bucket='',
                 mode='a',
                 use_zarr_format=None):

        if not hasattr(_z5py, "S3File"):
            raise AttributeError("z5 was not compiled with s3 support")
        handle = _z5py.S3File(bucket_name, name_in_bucket, _z5py.FileMode(self.file_modes[mode]))

        # TODO handle zarr/n5 flag properly
        is_zarr = use_zarr_format

        if not handle.exists():
            raise NotImplementedError
            _z5py.create_file(handle, is_zarr)
        super().__init__(handle, _z5py.S3Group)
