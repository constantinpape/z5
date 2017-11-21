import sys
import os
import time
import numpy as np
import h5py
import json

from volumina_viewer import volumina_n_layer

sys.path.append('../../../bld/python')
import z5py


save_folder = './tmp_files'
shape = (100, 1024, 1024)
bb = np.s_[:100, :1024, :1024]
chunks = [(1, 512, 512),
          (64, 64, 64)]


# FIXME zarr output is weirdly transposed
def single_read(data, chunk, compression):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression)
    save_path = os.path.join(save_folder,
                             'zarr_%s.zr' % key)
    t_read = time.time()
    f_out = z5py.File(save_path, use_zarr_format=True)
    ds = f_out['data']
    data_read = ds[:]
    t_read = time.time() - t_read
    volumina_n_layer([data.astype('float32'), data_read.astype('float32')])
    assert data.shape == data_read.shape
    assert np.allclose(data, data_read), "\n%i / \n%i" % (np.sum(np.isclose(data, data_read)), data.size)
    return key, t_read


def single_write_blosc(data, chunk, codec, shuffle):
    key = 'blosc_%s_%s_%i' % ('_'.join(str(cc) for cc in chunk),
                              codec,
                              shuffle)
    save_path = os.path.join(save_folder,
                             'zarr_%s.zr' % key)
    t_read = time.time()
    f_out = z5py.File(save_path, use_zarr_format=True)
    ds = f_out['data']
    data_read = ds[:]
    t_read = time.time() - t_read
    assert np.allclose(data, data_read)
    return key, t_read


def time_read_zarr(data):
    compressors_zarr = ['raw', 'zlib']
    blosc_codecs = ['lz4', 'zlib']
    times = {}
    for chunk in chunks:
        for compression in compressors_zarr:
            print("Read", chunk, compression)
            key, t_read = single_read(data, chunk, compression)
            times[key] = t_read

    # iterate over blosc codecs
    for chunk in chunks:
        for codec in blosc_codecs:
            for shuffle in (0, 1, 2):
                    print("Reading blosc", chunk, codec, shuffle)
                    key, t_read= single_read_blosc(data, chunk, codec, shuffle)
                    times[key] = t_write

    with open('./results/resread_zarr.json', 'w') as f:
        json.dump(times, f, indent=4, sort_keys=True)


if __name__ == '__main__':
    # path = '/home/consti/sampleA+_raw_automatically_realigned.h5'
    path = '/home/papec/Work/playground/z5_tests/sampleA+_raw_automatically_realigned.h5'

    with h5py.File(path, 'r') as f:
        data = f['data'][bb]

    time_read_zarr(data)
