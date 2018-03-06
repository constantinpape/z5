import os
import shutil

from .base import Base
from .group import Group
from .dataset import Dataset
from .modes import FileMode
import json

# set correct json error type for python 2 / 3
try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


ZARR_EXTS = {'.zarr'}
N5_EXTS = {'.n5'}


class File(Base):

    def __init__(self, path, use_zarr_format=None, mode='a'):
        self.mode = FileMode(mode)
        exists = self.mode.check_file(path)

        # check if the file already exists
        # and load it if it does
        if exists:
            zarr_group = os.path.join(path, '.zgroup')
            zarr_array = os.path.join(path, '.zarray')
            is_zarr = os.path.exists(zarr_group) or os.path.exists(zarr_array)

            # automatically infering the format
            if use_zarr_format is None:
                use_zarr_format = is_zarr
            # file was opened as zarr file
            elif use_zarr_format:
                assert is_zarr, "z5py.File: can't open n5 file in zarr format"

            # TODO check zarr version as well
            if not is_zarr:
                self._check_n5_version(path)

        else:
            if use_zarr_format is None:
                _, ext = os.path.splitext(path)
                if ext.lower() in ZARR_EXTS:
                    use_zarr_format = True
                elif ext.lower() in N5_EXTS:
                    use_zarr_format = False

            assert use_zarr_format is not None, \
                "z5py.File: Cannot infer the file format for non existing file"
            os.mkdir(path)
            meta_file = os.path.join(path, '.zgroup' if use_zarr_format else 'attributes.json')
            # we only need to write meta data for the zarr format
            if use_zarr_format:
                with open(meta_file, 'w') as f:
                    json.dump({'zarr_format': 2}, f)
            else:
                with open(meta_file, 'w') as f:
                    json.dump({'n5': "2.0.0"}, f)

        super(File, self).__init__(path, use_zarr_format)

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
        assert key not in self.keys(), \
            "z5py.File.create_group: Group is already existing"
        path = os.path.join(self.path, key)
        self.mode.check_write(path)
        return Group.make_group(path, self.is_zarr, self.mode)

    def __getitem__(self, key):
        assert key in self, "z5py.File.__getitem__: key does not exxist"
        path = os.path.join(self.path, key)
        if self.is_group(key):
            return Group.open_group(path, self.is_zarr, self.mode)
        else:
            return Dataset.open_dataset(path, self.mode)

    # TODO setitem, delete datasets ?

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
