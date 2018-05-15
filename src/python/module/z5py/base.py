import os
import json
from .dataset import Dataset
from .attribute_manager import AttributeManager
from ._z5py import FileMode


class Base(object):
    """
    Base class of the :class:``File`` and :class:``Group`` class.

    This class should not be instantiated.
    """
    # the python / h5py file modes and the corresponding internal types
    # TODO as far as I can tell there is no difference between 'w-' and 'x',
    # so for now they get mapped to the same internal type
    file_modes = {'a': FileMode.a, 'r': FileMode.r,
                  'r+': FileMode.r_p, 'w': FileMode.w,
                  'w-': FileMode.w_m, 'x': FileMode.w_m}

    def __init__(self, path, is_zarr=True, mode='a'):
        if mode not in self.file_modes:
            raise ValueError("Invalid file mode: %s" % mode)

        # x is not a mode in c++, because it has the same properties
        # as w- (as far as I can see)
        self.mode = mode
        self._internal_mode = self.file_modes[mode]
        self._permissions = FileMode(self._internal_mode)
        self.path = path
        self.is_zarr = is_zarr
        self._attrs = AttributeManager(path, is_zarr)

    @property
    def attrs(self):
        return self._attrs

    # FIXME this is not what we wan't, because
    # a) we want to have the proper python key syntax
    # b) this will not list nested paths in file properly,
    # like 'volumes/raw'
    def keys(self):
        return os.listdir(self.path)

    def __contains__(self, key):
        return os.path.exists(os.path.join(self.path, key))

    # TODO open_dataset, open_group and close_group should also be implemented here
    def require_dataset(self, name, shape,
                        dtype=None, chunks=None,
                        n_threads=1, **kwargs):
        if not self._permissions.can_write():
            raise ValueError("Cannot create dataset with read-only permissions.")
        path = os.path.join(self.path, name)
        return Dataset.require_dataset(path, shape, dtype, chunks,
                                       n_threads, self.is_zarr, self.mode)

    def create_dataset(self, name,
                       shape=None, dtype=None,
                       data=None, chunks=None,
                       compression=None, fillvalue=0,
                       n_threads=1, **compression_options):
        """Creates a new dataset.

        Create a new chunked dataset on disc. Syntax and behaviour similar to the
        corresponding ``h5py`` function.
        In contrast to ``h5py``, there is no option to store a dataset without chunking
        (if no chunks are given, a default suitable for the dimension of the dataset will be used).
        Also, if a dataset is created with data and a dtype that is different
        from the data's is specified, the function throws a RuntimeError, instead
        of converting the data.

        :param name: name of the dataset
        :type name: ``str``
        :param shape: shape of the dataset, must be given unless created with data
        :type shape: ``tuple`` or ``None``
        :param dtype: dtype of the dataset, must be given unless created with data
        :type dtype: ``str``, ``np.dtype`` or ``None``
        :param data: optional data used to fill the dataset upon creation
        :type data: ``np.ndarray`` or ``None``
        :param chunks: size of the chunks for all axes
        :type chunks: ``tuple`` or ``None``
        :param compression: filter used to compress the chunks written to disc
        :type compression: ``str`` or ``None``
        :param fillvalue: default fillvalue to use for empty chunks (only supported by zarr)
        :type fillvalue: ``float``
        :param n_threads: number of threads used to read and write data
        :type n_threads: ``int``
        :rtype: :class:``Dataset``
        """

        if not self._permissions.can_write():
            raise ValueError("Cannot create dataset with read-only permissions.")
        if name in self:
            raise KeyError("Dataset %s is already existing." % name)
        path = os.path.join(self.path, name)
        return Dataset.create_dataset(path, shape, dtype,
                                      data, chunks, compression,
                                      fillvalue, n_threads,
                                      compression_options,
                                      self.is_zarr, self._internal_mode)

    def is_group(self, key):
        path = os.path.join(self.path, key)
        if self.is_zarr:
            return os.path.exists(os.path.join(path, '.zgroup'))
        else:
            meta_path = os.path.join(path, 'attributes.json')
            if not os.path.exists(meta_path):
                return True
            with open(meta_path, 'r') as f:
                # attributes for n5 file can be empty which cannot be parsed by json
                try:
                    attributes = json.load(f)
                except ValueError:
                    attributes = {}
            # The dimensions key is only present in a dataset
            return 'dimensions' not in attributes
