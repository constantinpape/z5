"""The container entry points: :class:`File` and friends.

A :class:`File` is the root of a zarr / n5 container on the filesystem and the
usual entry point to z5py (similar to ``h5py.File``). :class:`ZarrFile` /
:class:`N5File` force a specific format, and :class:`S3File` opens a zarr
container stored in an S3 (or S3-compatible) bucket.
"""
import json
import os
import errno

from . import _z5py
from .group import Group


def _unpickle_s3_file(bucket_name, name_in_bucket, mode_str, zarr_format, dimension_separator,
                      endpoint_url, region, anon, access_key, secret_key):
    # reconstruct the handle directly (without running S3File.__init__, which
    # would truncate / (re-)create the container for mode 'w' / 'x')
    handle = _z5py.S3File(bucket_name, name_in_bucket,
                          _z5py.FileMode(Group.file_modes[mode_str]),
                          endpoint_url, region, anon, access_key, secret_key)
    obj = S3File.__new__(S3File)
    obj._init_args = (bucket_name, name_in_bucket, mode_str, zarr_format, dimension_separator,
                      endpoint_url, region, anon, access_key, secret_key)
    Group.__init__(obj, handle, _z5py.S3Group, parent=obj, name="",
                   dimension_separator=dimension_separator, zarr_format=zarr_format)
    return obj


def _unpickle_file(path, mode_str, dimension_separator):
    # reconstruct the File handle directly (without running File.__init__, which
    # would truncate / (re-)create the file) and set up the Group state.
    handle = _z5py.File(path, _z5py.FileMode(Group.file_modes[mode_str]))
    obj = File.__new__(File)
    zarr_format = 2
    if handle.is_zarr():
        try:
            zarr_format = json.loads(handle.read_metadata()).get('zarr_format', 2)
        except Exception:
            pass
    Group.__init__(obj, handle, _z5py.Group, parent=obj, name="",
                   dimension_separator=dimension_separator, zarr_format=zarr_format)
    return obj


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
        """ Infer the file format from the path.

        First the file extension is checked (see :attr:`zarr_exts` / :attr:`n5_exts`),
        then, if inconclusive, the presence of zarr metadata files in the directory.

        Args:
            path (str): path to the container.

        Returns:
            bool: True for zarr, False for n5, or None if the format could not be inferred.
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
            zarr_json = os.path.join(path, 'zarr.json')  # zarr v3
            is_zarr = (os.path.exists(zarr_group) or os.path.exists(zarr_array)
                       or os.path.exists(zarr_json))
        return is_zarr

    def __init__(self, path, mode="a", use_zarr_format=None, dimension_separator=".", zarr_format=2):

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

        super().__init__(handle, _z5py.Group, parent=self, name="",
                         dimension_separator=dimension_separator, zarr_format=zarr_format)

        # at some point we should move more of this logic to c++ as well
        # if we open in 'w', remove the existing file existing
        if handle.exists() and mode.should_truncate():
            handle.remove()
        if handle.exists():
            if mode.must_not_exist():
                raise OSError(errno.EEXIST, os.strerror(errno.EEXIST), handle.path())
            # recovers the actual on-disk zarr_format into self._zarr_format
            self._check_version()
        else:
            if not mode.can_create():
                raise OSError(errno.EROFS, os.strerror(errno.EROFS), handle.path())
            # if we don't have the file, create it
            _z5py.create_file(handle, is_zarr, zarr_format)

    def __reduce__(self):
        # pickle by storing path + mode and re-opening on unpickle
        return (_unpickle_file,
                (self._handle.path(), self._handle.mode().mode(), self._dimension_separator))

    def _check_version(self):
        metadata = self._handle.read_metadata()
        metadata = json.loads(metadata)
        if self._handle.is_zarr():
            version = metadata['zarr_format']
            if version not in (2, 3):
                raise RuntimeError("z5 only supports zarr format 2 and 3")
            self._zarr_format = version
        else:
            version = metadata.get('n5', None)
            if version is not None:
                major_version = int(version[0])
                if major_version > 4:
                    raise RuntimeError("Can't open n5 file with major version bigger than 4")

    def close(self):
        """ No-op, for API conformity with file-handling code.

        z5py containers do not need to be closed; this exists so ``File`` can be
        used as a context manager and as a drop-in for code that calls ``close``.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def filename(self):
        """ The filesystem path of this container.
        """
        return self._handle.path()

    # we need to override this from group
    @property
    def name(self):
        """ The name of the root group, always ``'/'``.
        """
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

    def __init__(self, path, mode="a", dimension_separator=".", zarr_format=2):
        super().__init__(path=path, use_zarr_format=True, mode=mode,
                         dimension_separator=dimension_separator, zarr_format=zarr_format)


class S3File(Group):
    """ File to access a zarr container in an S3 (or S3-compatible) bucket.

    Only the zarr format is supported over S3 (both zarr v2 and v3). The
    per-dataset chunk-key configuration is read from the store on open, so
    reading works regardless of the ``zarr_format`` passed here; ``zarr_format``
    determines the format used when *creating* new groups / datasets.

    Args:
        bucket_name (str): name of the S3 bucket.
        name_in_bucket (str): key prefix of the container within the bucket (default: '').
        mode (str): file mode used to open / create the container (default: 'a').
        use_zarr_format (bool): whether the container is zarr; only zarr is supported
            over S3 (default: None, treated as zarr).
        zarr_format (int): zarr format version, 2 or 3 (default: 2).
        dimension_separator (str): chunk key separator; defaults to '.' for zarr v2
            and '/' for zarr v3 (default: None).
        endpoint_url (str): custom S3 endpoint (e.g. MinIO / moto / non-AWS storage);
            None uses the default AWS endpoint (default: None).
        region (str): AWS region (default: 'us-east-1').
        anon (bool): use anonymous (unsigned) access for public buckets (default: False).
        access_key (str): AWS access key id; None uses the ambient credential chain (default: None).
        secret_key (str): AWS secret access key (default: None).
    """

    def __init__(self, bucket_name, name_in_bucket='', mode='a', use_zarr_format=None,
                 zarr_format=2, dimension_separator=None,
                 endpoint_url=None, region='us-east-1',
                 anon=False, access_key=None, secret_key=None):

        if not hasattr(_z5py, "S3File"):
            raise AttributeError("z5 was not compiled with s3 support")

        # only zarr is supported over s3
        is_zarr = True if use_zarr_format is None else use_zarr_format
        if not is_zarr:
            raise NotImplementedError("Only the zarr format is supported for S3 containers")

        if dimension_separator is None:
            dimension_separator = '/' if zarr_format == 3 else '.'

        # stored for __reduce__ (the handle does not expose the s3 configuration)
        self._init_args = (bucket_name, name_in_bucket, mode, zarr_format, dimension_separator,
                           '' if endpoint_url is None else endpoint_url, region, anon,
                           '' if access_key is None else access_key,
                           '' if secret_key is None else secret_key)

        handle = _z5py.S3File(bucket_name, name_in_bucket,
                              _z5py.FileMode(self.file_modes[mode]),
                              '' if endpoint_url is None else endpoint_url,
                              region, anon,
                              '' if access_key is None else access_key,
                              '' if secret_key is None else secret_key)
        file_mode = handle.mode()

        super().__init__(handle, _z5py.S3Group, parent=self, name="",
                         dimension_separator=dimension_separator, zarr_format=zarr_format)

        # if we open in 'w', remove the existing container
        if handle.exists() and file_mode.should_truncate():
            handle.remove()
        if handle.exists():
            if file_mode.must_not_exist():
                raise OSError(errno.EEXIST, os.strerror(errno.EEXIST), bucket_name)
        else:
            if not file_mode.can_create():
                raise OSError(errno.EROFS, os.strerror(errno.EROFS), bucket_name)
            _z5py.create_file(handle, is_zarr, zarr_format)

    def __reduce__(self):
        # without this override the Group.__reduce__ inherited here references the
        # root file itself, which cannot be pickled (RecursionError) - and with it,
        # groups / datasets under an S3File become picklable as well
        return (_unpickle_s3_file, self._init_args)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def name(self):
        return '/'
