import os
import numbers
import json


def slice_to_start_stop(s, size):
    """For a single dimension with a given size, normalize slice to size.
     Returns slice(None, 0) if slice is invalid."""
    if s.step not in (None, 1):
        raise ValueError('Nontrivial steps are not supported')

    if s.start is None:
        start = 0
    elif -size <= s.start < 0:
        start = size + s.start
    elif s.start < -size or s.start >= size:
        return slice(None, 0)
    else:
        start = s.start

    if s.stop is None or s.stop > size:
        stop = size
    elif s.stop < 0:
        stop = (size + s.stop)
    else:
        stop = s.stop

    if stop < 1:
        return slice(None, 0)

    return slice(start, stop)


def int_to_start_stop(i, size):
    """For a single dimension with a given size, turn an int into slice(start, stop)
    pair."""
    if -size < i < 0:
        start = i + size
    elif i >= size or i < -size:
        raise ValueError('Index ({}) out of range (0-{})'.format(i, size-1))
    else:
        start = i
    return slice(start, start + 1)


def sliding_window(arr, wsize):
    """Yield a wsize-length tuple of items from sequence arr as a sliding window"""
    if wsize > len(arr):
        return
    for idx in range(len(arr) - wsize + 1):
        yield tuple(arr[idx:idx+wsize])


def rectify_shape(arr, required_shape):
    """Reshape arr into the required shape while keeping neighbouring non-singleton dimensions together
    e.g. shape (1, 2, 1) -> (2, 1, 1, 1) is fine
    shape (1, 2, 1, 2, 1, 1, 1) -> (1, 2, 2, 1) is not
    """
    if arr.shape == required_shape:
        return arr

    important_shape = list(arr.shape)
    while len(important_shape) > 1:
        if important_shape[0] == 1:
            important_shape.pop(0)
        elif important_shape[-1] == 1:
            important_shape.pop()
        else:
            break

    important_shape = tuple(important_shape)

    msg = ('could not broadcast input array from shape {} into shape {}; '
           'complicated broacasting not supported').format(arr.shape, required_shape)

    if len(important_shape) > len(required_shape):
        raise ValueError(msg)

    is_leading = True
    for window in sliding_window(required_shape, len(important_shape)):
        if window == important_shape:
            if is_leading:
                is_leading = False
                continue
            else:
                break
        if is_leading:
            if window[0] != 1:
                break
        else:
            if window[-1] != 1:
                break
    else:
        return arr.reshape(required_shape)

    raise ValueError(msg)


# TODO we should adjust chunk sizes better if some dimensions
# are larger than the default chunks (e.g. shape (2, 2000, 2000)
# should have chunks (1, 512, 512) instead of (2, 64, 64))
def get_default_chunks(shape):
    # the default size is 64**3
    default_size = 262144
    default_dim = int(round(default_size ** (1. / len(shape))))
    return tuple(min(default_dim, sh) for sh in shape)


def is_group(path, is_zarr):
    if is_zarr:
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


def normalize_slices(slices, shape):
    """ Normalize slices to shape.

    Normalize input, which can be a slice or a tuple of slices / ellipsis to
    be of same length as shape and be in bounds of shape.

    Args:
        slices (int or slice or ellipsis or tuple[int or slice or ellipsis]): slices to be normalized

    Returns:
        tuple[slice]: normalized slices (start and stop are both non-None)
        tuple[int]: which singleton dimensions should be squeezed out
    """
    type_msg = 'Advanced selection inappropriate. ' \
               'Only numbers, slices (`:`), and ellipsis (`...`) are valid indices (or tuples thereof)'

    if isinstance(slices, tuple):
        slices_lst = list(slices)
    elif isinstance(slices, (numbers.Number, slice, type(Ellipsis))):
        slices_lst = [slices]
    else:
        raise TypeError(type_msg)

    ndim = len(shape)
    if len([item for item in slices_lst if item != Ellipsis]) > ndim:
        raise TypeError("Argument sequence too long")
    elif len(slices_lst) < ndim and Ellipsis not in slices_lst:
        slices_lst.append(Ellipsis)

    normalized = []
    found_ellipsis = False
    squeeze = []
    for item in slices_lst:
        d = len(normalized)
        if isinstance(item, slice):
            normalized.append(slice_to_start_stop(item, shape[d]))
        elif isinstance(item, numbers.Number):
            squeeze.append(d)
            normalized.append(int_to_start_stop(int(item), shape[d]))
        elif isinstance(item, type(Ellipsis)):
            if found_ellipsis:
                raise ValueError("Only one ellipsis may be used")
            found_ellipsis = True
            while len(normalized) + (len(slices_lst) - d - 1) < ndim:
                normalized.append(slice(0, shape[len(normalized)]))
        else:
            raise TypeError(type_msg)
    return tuple(normalized), tuple(squeeze)
