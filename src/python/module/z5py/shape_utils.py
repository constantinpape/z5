def slice_to_begin_shape(s, size):
    """For a single dimension with a given size, turn a slice object into a (start_idx, length)
     pair. Returns (None, 0) if slice is invalid."""
    if s.step not in (None, 1):
        raise ValueError('Nontrivial steps are not supported')

    if s.start is None:
        begin = 0
    elif -size <= s.start < 0:
        begin = size + s.start
    elif s.start < -size or s.start >= size:
        return None, 0
    else:
        begin = s.start

    if s.stop is None or s.stop > size:
        shape = size - begin
    elif s.stop < 0:
        shape = (size + s.stop) - begin
    else:
        shape = s.stop - begin

    if shape < 1:
        return None, 0

    return begin, shape


def int_to_begin_shape(i, size):
    """For a single dimension with a given size, turn an int into a (start_idx, length)
    pair."""
    if -size < i < 0:
        begin = i + size
    elif i >= size or i < -size:
        raise ValueError('Index ({}) out of range (0-{})'.format(i, size-1))
    else:
        begin = i

    return begin, 1


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
    ndim = len(shape)
    default_dim = default_size ** (1. / ndim)
    return tuple(min(default_dim, sh) for sh in shape)
