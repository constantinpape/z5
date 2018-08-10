import os
from shutil import rmtree

try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping

from ._z5py import create_group, FileMode
from .dataset import Dataset
from .attribute_manager import AttributeManager
from .shape_utils import is_group


class Group(Mapping):
    """ Group inside of a z5py container.

    Corresponds to a directory on the filesystem.
    Supports python dict api.
    Should not be instantiated directly, but rather be created
    or opened via the `create_group`, `request_group` or `[]` operators
    of Group or File.
    """

    # the python / h5py file modes and the corresponding internal types
    # as far as I can tell there is no difference between 'w-' and 'x',
    # so for now they get mapped to the same internal type

    #: available modes for opening files. these correspond to the ``h5py`` file modes
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

    def __iter__(self):
        for name in os.listdir(self.path):
            if os.path.isdir(os.path.join(self.path, name)):
                yield name

    def __len__(self):
        counter = 0
        for _ in self:
            counter += 1
        return counter

    # TODO can use `util.removeDataset` here
    def __delitem__(self, name):
        if not self._permissions.can_write():
            raise ValueError("Cannot call delete on file not opened with write permissions.")
        path_ = os.path.join(self.path, name)
        if not os.path.exists(path_):
            raise KeyError("%s does not exist" % name)
        rmtree(path_)

    def __getitem__(self, name):
        """ Access group or dataset in the container.

        Fails if no dataset or group of the specified name exists.

        Args:
            name (str): name of group or dataset in container.

        Returns:
            ``Group`` or ``Dataset``.
        """
        path = os.path.join(self.path, name)
        if not os.path.isdir(path):
            raise KeyError("Key %s does not exist" % name)

        if is_group(path, self.is_zarr):
            return Group._open_group(path, self.is_zarr, self.mode)
        else:
            return Dataset._open_dataset(path, self._internal_mode)

    @property
    def attrs(self):
        """ Access additional attributes.

        Returns:
            ``AttributeManager``.
        """
        return self._attrs

    #
    # Group functionality
    #

    @classmethod
    def _create_group(cls, path, is_zarr, mode):
        create_group(path, is_zarr, cls.file_modes[mode])
        return cls(path, is_zarr, mode)

    @classmethod
    def _open_group(cls, path, is_zarr, mode):
        return cls(path, is_zarr, mode)

    def create_group(self, name):
        """ Create a new group.

        Create new (sub-)group of the group.
        Fails if a group of this name already exists.

        Args:
            name (str): name of the new group.

        Returns:
            ``Group``: group of the requested name.
        """
        if name in self:
            raise KeyError("Group %s is already existing" % name)
        path = os.path.join(self.path, name)
        return Group._create_group(path, self.is_zarr, self.mode)

    def require_group(self, name):
        """ Require group.

        Require that a group of the given name exists.
        The group will be created if it does not already exist.

        Args:
            name (str): name of the required group.

        Returns:
            ``Group``: group of the requested name.
        """
        path = os.path.join(self.path, name)
        if os.path.exists(path):
            if not is_group(path, self.is_zarr):
                raise TypeError("Incompatible object (Dataset) already exists")
            return Group._open_group(path, self.is_zarr, self.mode)
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
        """ Create a new dataset.

        Create a new dataset in the group. Syntax and behaviour similar to the
        corresponding ``h5py`` functionality.
        In contrast to ``h5py``, there is no option to store a dataset without chunking
        (if no chunks are given default values, suitable for the dimension of the dataset, will be used).
        Also, if a dataset is created with data and a dtype that is different
        from the data's is specified, the function throws a RuntimeError, instead
        of converting the data.

        Args:
            name (str): name of the new dataset.
            shape (tuple): shape of the new dataset. If no shape is given,
                the ``data`` argument must be given. (default: None).
            dtype (str or np.dtpye): datatype of the new dataset. If no dtype is given,
                the ``data`` argument must be given (default: None).
            data (np.ndarray): data used to infer shape, dtype and fill the dataset
                upon creation (default: None).
            chunks (tuple): chunk sizes of the new dataset. If no chunks are given,
                a suitable default value for the number of dimensions will be used (default: None).
            compression (str): name of the compression library used to compress chunks.
                If no compression is given, the default for the current format is used (default: None).
            fillvalue (float): fillvalue for empty chunks (only zarr) (default: 0).
            n_threads (int): number of threads used for chunk I/O (default: 1).
            **compression_options: options for the compression library.

        Returns:
            ``Dataset``: the new dataset.
        """

        if not self._permissions.can_create():
            raise ValueError("Cannot create dataset with read-only permissions.")
        if name in self:
            raise KeyError("Dataset %s is already existing." % name)
        path = os.path.join(self.path, name)
        return Dataset._create_dataset(path, shape, dtype,
                                       data, chunks, compression,
                                       fillvalue, n_threads,
                                       compression_options,
                                       self.is_zarr, self._internal_mode)

    def require_dataset(self, name, shape,
                        dtype=None, chunks=None,
                        n_threads=1, **kwargs):
        """ Require dataset.

        Require dataset in the group.
        Will create the dataset if it does not exist, otherwise returns
        existing dataset. If the dataset already exists, consistency with the
        arguments ``shape``, ``dtype`` (if given) and ``chunks`` (if given) is enforced.

        Args:
            name (str): name of the dataset.
            shape (tuple): shape of the dataset.
            dtype (str or np.dtpye): datatype of dataset (default: None).
            chunks (tuple): chunk sizes of the dataset (default: None).
            n_threads (int): number of threads used for chunk I/O (default: 1).
            **kwargs: additional arguments that will only be used for creation
                if the dataset does not exist.

        Returns:
            ``Dataset``: the required dataset.
        """
        if not self._permissions.can_create():
            raise ValueError("Cannot create dataset with read-only permissions.")
        path = os.path.join(self.path, name)
        return Dataset._require_dataset(path, shape, dtype, chunks,
                                        n_threads, self.is_zarr, self._internal_mode,
                                        **kwargs)
