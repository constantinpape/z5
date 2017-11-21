import sys
import os
import time
import numpy as np
import h5py
import json

sys.path.append('../../../bld/python')
import z5py

shape = (100, 1024, 1024)
bb = np.s_[:100, :1024, :1024]
chunks = [(1, 512, 512),
          (64, 64, 64)]


def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def single_write(data, chunk, compression):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression)
    save_path = os.path.join(save_folder,
                             'zarr_%s.zr' % key)
    f_out = z5py.File(save_path, use_zarr_format=True)
    t_write = time.time()
    ds = f_out.create_dataset('data',
                              shape=shape,
                              chunks=chunk,
                              dtype='uint8',
                              compressor=compression)
    ds[:] = data
    t_write = time.time() - t_write
    size = get_size(save_path)
    size /= 1e6  # divide by 1e6 to get mbytes
    return key, t_write, size


def single_write_blosc(data, chunk, codec, shuffle):
    key = 'blosc_%s_%s_%i' % ('_'.join(str(cc) for cc in chunk),
                              codec,
                              shuffle)
    save_path = os.path.join(save_folder,
                             'zarr_%s.zr' % key)
    f_out = z5py.File(save_path, use_zarr_format=True)
    t_write = time.time()
    ds = f_out.create_dataset('data',
                              shape=shape,
                              chunks=chunk,
                              dtype='uint8',
                              compressor='blosc',
                              codec=codec,
                              shuffle=shuffle)
    ds[:] = data
    t_write = time.time() - t_write
    size = get_size(save_path)
    size /= 1e6  # divide by 1e6 to get mbytes
    return key, t_write, size


def time_write_zarr(data):
    compressors_zarr = ['raw', 'zlib']
    blosc_codecs = ['lz4', 'zlib']
    times = {}
    # iterate over `normal` compressors
    for chunk in chunks:
        for compression in compressors_zarr:
            key, t_write, size = single_write(data, chunk, compression)
            times[key] = (t_write, size)

    # iterate over blosc codecs
    for chunk in chunks:
        for compression in compressors_zarr:
            for codec in blosc_codecs:
                for shuffle in (0, 1, 2):
                    key, t_write, size = single_write_blosc(data, chunk, codec, shuffle)
                    times[key] = (t_write, size)
    with open('./results/reswrite_zarr.json', 'w') as f:
        json.dump(times, f, indent=4, sort_keys=True)


if __name__ == '__main__':
    path = '/home/consti/sampleA+_raw_automatically_realigned.h5'

    save_folder = './tmp_files'
    if not os.path.exists(save_folder):
        os.mkdir(save_folder)
    if not os.path.exists('./results'):
        os.mkdir('./results')

    with h5py.File(path, 'r') as f:
        data = f['data'][bb]

    time_write_zarr(data)
