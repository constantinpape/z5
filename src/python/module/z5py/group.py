from collections.abc import Mapping

from . import _z5py
from .dataset import Dataset
from .attribute_manager import AttributeManager


class Group(Mapping):
    """ Group inside of a z5py container.

    Corresponds to a directory on the filesystem or object in cloud storage.
    Supports python dict api.
    Should not be instantiated directly, but rather be created
    or opened via the `create_group`, `require_group` or `[]` operators
    of `z5py.Group` or `z5py.File`.
    """

    # the python / h5py file modes and the corresponding internal types
    # as far as I can tell there is no difference between 'w-' and 'x',
    # so for now they get mapped to the same internal type

    #: available modes for opening files. these correspond to the ``h5py`` file modes
    file_modes = {'a': _z5py.FileMode.a, 'r': _z5py.FileMode.r,
                  'r+': _z5py.FileMode.r_p, 'w': _z5py.FileMode.w,
                  'w-': _z5py.FileMode.w_m, 'x': _z5py.FileMode.w_m}

    def __init__(self, handle, handle_factory, parent, name):
        self._handle = handle
        self._handle_factory = handle_factory
        self._attrs = AttributeManager(self._handle)
        self._parent = parent
        self._name = name

    #
    # Magic Methods, Attributes, Keys, Contains
    #

    def __iter__(self):
        keys = self._handle.keys()
        for name in keys:
            yield name

    def __len__(self):
        return len(self._handle.keys())

    def __contains__(self, name):
        return self._handle.has(name.lstrip('/'))

    def __delitem__(self, name):
        if not self._handle.mode().can_write():
            raise ValueError("Cannot call delete on file not opened with write permissions.")
        if name not in self:
            raise KeyError("%s does not exist" % name)
        if self.is_sub_group(name.lstrip('/')):
            handle = self._handle_factory(self._handle, name.lstrip('/'))
            handle.remove()
        else:
            _z5py.remove_dataset(self[name]._impl, 1)

    def __getitem__(self, name):
        """ Access group or dataset in the container.

        Fails if no dataset or group of the specified name exists.

        Args:
            name (str): name of group or dataset in container.

        Returns:
            ``Group`` or ``Dataset``.
        """
        if name not in self:
            raise KeyError("Key %s does not exist" % name)

        if self.is_sub_group(name.lstrip('/')):
            handle = self._handle_factory(self._handle, name.lstrip('/'))
            return Group(handle, self._handle_factory, self, self._name + '/' + name)
        else:
            return Dataset._open_dataset(self, name.lstrip('/'))

    @property
    def attrs(self):
        """ Access additional attributes.

        Returns:
            ``AttributeManager``.
        """
        return self._attrs

    @property
    def is_zarr(self):
        return self._handle.is_zarr()

    @property
    def parent(self):
        return self._parent

    @property
    def name(self):
        return self._name

    @property
    def file(self):
        # we find the file by going up the parents till we find
        # the root (which is it's own parent)
        parent = self.parent
        while parent is not parent.parent:
            parent = parent.parent
        return parent

    #
    # Group functionality
    #

    def is_sub_group(self, name):
        return self._handle.is_sub_group(name)

    def create_group(self, name):
        """ Create a new group.

        Create new (sub-)group of the group.
        Fails if a group of this name already exists.

        Args:
            name (str): name of the new group.

        Returns:
            ``Group``: group of the requested name.
        """
        if not self._handle.mode().can_write():
            raise ValueError("Cannot create group with read-only permissions.")
        if name in self:
            raise KeyError("An object with name %s already exists" % name)
        handle = _z5py.create_group(self._handle, name)
        return Group(handle, self._handle_factory, self, self._name + '/' + name)

    def require_group(self, name):
        """ Require group.

        Require that a group of the given name exists.
        The group will be created if it does not already exist.

        Args:
            name (str): name of the required group.

        Returns:
            ``Group``: group of the requested name.
        """
        if name in self:
            if not self.is_sub_group(name):
                raise TypeError("Incompatible object (Dataset) already exists")
            handle = self._handle_factory(self._handle, name)
        else:
            if not self._handle.mode().can_write():
                raise ValueError("Cannot create group with read-only permissions.")
            handle = _z5py.create_group(self._handle, name)
        return Group(handle, self._handle_factory, self, self._name + '/' + name)

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

        if not self._handle.mode().can_write():
            raise ValueError("Cannot create dataset with read-only permissions.")
        if name in self:
            raise KeyError("Dataset %s is already existing." % name)
        return Dataset._create_dataset(self, name, shape, dtype,
                                       data, chunks, compression,
                                       fillvalue, n_threads,
                                       compression_options)

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
        if not self._handle.mode().can_write():
            raise ValueError("Cannot create dataset with read-only permissions.")
        return Dataset._require_dataset(self, name, shape, dtype, chunks,
                                        n_threads, **kwargs)

    def visititems(self, func, _root=None):
        """ Recursively visit names and objects in this group.

        You supply a callable (function, method or callable object); it
        will be called exactly once for each link in this group and every
        group below it. Your callable must conform to the signature:

            func(<member name>, <object>) => <None or return value>

        Returning None continues iteration, returning anything else stops
        and immediately returns that value from the visit method.  No
        particular order of iteration within groups is guaranteed.

        calls the function @param func with the found items, appended to @param path

        Example:

        # Get a list of all datasets in the file
        >>> mylist = []
        >>> def func(name, obj):
        ...     if isinstance(obj, Dataset):
        ...         mylist.append(name)
        ...
        >>> f = File('foo.n5')
        >>> f.visititems(func)
        """
        _root = self._handle if _root is None else _root
        for name, obj in self.items():
            # the name needs to be relative to the root object visititems was called on
            name = _root.relative_path(obj._handle)
            func_ret = func(name, obj)
            if func_ret is not None:
                return func_ret
            if isinstance(obj, Group):
                obj.visititems(func, _root=_root)
