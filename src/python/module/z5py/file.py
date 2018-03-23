import os
import errno
import json
from shutil import rmtree

from .base import Base
from .group import Group
from .dataset import Dataset

# set correct json error type for python 2 / 3
try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


class File(Base):
    zarr_exts = {'.zarr', '.zr'}
    n5_exts = {'.n5'}

    @staticmethod
    def infer_format(path):
        """
        Infer the file format from the path.
        Return `True` for zarr, `False` for n5 and `None` if the format could not be infered.
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
            if ext.lower() in File.zarr_exts:
                is_zarr = True
            elif ext.lower() in File.n5_exts:
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
            # TODO I am unsure about this, an accidental 'w' could wreak a lot of havoc ...
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
                raise RuntimeError("z5py.File: Can't open n5 file with major version bigger than 2")

    def create_group(self, key):
        if key in self:
            raise RuntimeError("Group %s is already existing" % key)
        path = os.path.join(self.path, key)
        return Group.make_group(path, self.is_zarr, self.mode)

    def __getitem__(self, key):
        if key not in self:
            raise RuntimeError("Key %s does not exist" % key)
        path = os.path.join(self.path, key)
        if self.is_group(key):
            return Group.open_group(path, self.is_zarr, self.mode)
        else:
            return Dataset.open_dataset(path, self._internal_mode)

    # TODO setitem, delete datasets ?
    # - setitem could be implemented with softlinks
    # - delete item would remove the folder (or softlink)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
