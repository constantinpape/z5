import os
from ._z5py import create_group, FileMode
from .dataset import Dataset
from .attribute_manager import AttributeManager
from .shape_utils import is_group


class Group(object):
    """
    Group in a N5 or zarr file.
    """

    # the python / h5py file modes and the corresponding internal types
    # as far as I can tell there is no difference between 'w-' and 'x',
    # so for now they get mapped to the same internal type
    file_modes = {'a': FileMode.a, 'r': FileMode.r,
                  'r+': FileMode.r_p, 'w': FileMode.w,
                  'w-': FileMode.w_m, 'x': FileMode.w_m}

    def __init__(self, path, is_zarr=True, mode='a'):
        if mode not in self.file_modes:
            raise ValueError("Invalid file mode: %s" % mode)

        self.mode = mode
        self._internal_mode = self.file_modes[mode]
        self._permissions = FileMode(self._internal_mode)
        self.path = path
        self.is_zarr = is_zarr
        self._attrs = AttributeManager(path, is_zarr)

    #
    # Magic Methods, Attributes, Keys, Contains
    #

    def __contains__(self, name):
        path = os.path.join(self.path, name)
        return os.path.exists(path) and os.path.isdir(path)

    def __getitem__(self, name):
        if name not in self:
            raise KeyError("Key %s does not exist" % name)
        path = os.path.join(self.path, name)
        if is_group(path, self.is_zarr):
            return Group.open_group(path, self.is_zarr, self.mode)
        else:
            return Dataset.open_dataset(path, self._internal_mode)

    @property
    def attrs(self):
        return self._attrs

    def keys(self):
        return [f for f in os.listdir(self.path)
                if os.path.isdir(os.path.join(self.path, f))]

    #
    # Group functionality
    #

    @classmethod
    def make_group(cls, path, is_zarr, mode):
        create_group(path, is_zarr, cls.file_modes[mode])
        return cls(path, is_zarr, mode)

    @classmethod
    def open_group(cls, path, is_zarr, mode):
        return cls(path, is_zarr, mode)

    def create_group(self, name):
        if name in self:
            raise KeyError("Group %s is already existing" % name)
        path = os.path.join(self.path, name)
        return Group.make_group(path, self.is_zarr, self.mode)

    def require_group(self, name):
        path = os.path.join(self.path, name)
        if os.path.exists(path):
            if not is_group(path, self.is_zarr):
                raise TypeError("Incompatible object (Dataset) already exists")
            return Group.open_group(path, self.is_zarr, self.mode)
        else:
            return self.create_group(name)

    #
    # Dataset functionality
    #

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

    def require_dataset(self, name, shape,
                        dtype=None, chunks=None,
                        n_threads=1, **kwargs):
        if not self._permissions.can_write():
            raise ValueError("Cannot create dataset with read-only permissions.")
        path = os.path.join(self.path, name)
        return Dataset.require_dataset(path, shape, dtype, chunks,
                                       n_threads, self.is_zarr, self.mode)
