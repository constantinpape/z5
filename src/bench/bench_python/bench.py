import os
import time
import json
from shutil import rmtree

import numpy as np
import h5py
import z5py


save_folder = './tmp_files'
chunks = [(1, 512, 512),
          (64, 64, 64)]


def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def read_h5(data, chunk, compression):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression if compression is not None else 'raw')
    save_path = os.path.join(save_folder, 'h5_%s.h5' % key)
    t_read = time.time()
    with h5py.File(save_path, 'r') as f_out:
        data_read = f_out['data'][:]
    t_read = time.time() - t_read
    assert np.allclose(data, data_read)
    return key, t_read


def time_read_h5(data):
    compressors_h5 = [None, 'gzip']
    times = {}
    for chunk in chunks:
        for compression in compressors_h5:
            print("Read", chunk, compression)
            key, t_read = read_h5(data, chunk, compression)
            times[key] = t_read
    return times


def write_h5(data, chunk, compression):
    opts = 5 if compression == 'gzip' else None
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression if compression is not None else 'raw')
    save_path = os.path.join(save_folder, 'h5_%s.h5' % key)
    t_write = time.time()
    with h5py.File(save_path, 'w') as f_out:
        ds = f_out.create_dataset('data',
                                  shape=data.shape,
                                  chunks=chunk,
                                  compression=compression,
                                  dtype='uint8',
                                  compression_opts=opts)
        ds[:] = data
    t_write = time.time() - t_write
    size = os.path.getsize(save_path)
    size /= 1e6  # divide by 1e6 to get mbytes
    return key, t_write, size


def time_write_h5(data):
    compressors_h5 = [None, 'gzip']
    times = {}
    for chunk in chunks:
        for compression in compressors_h5:
            print("Write", chunk, compression)
            key, t_write, size = write_h5(data, chunk, compression)
            times[key] = (t_write, size)
    return times


def write_n5(data, chunk, compression):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression)
    save_path = os.path.join(save_folder, 'n5_%s.n5' % key)
    t_write = time.time()
    f_out = z5py.File(save_path, use_zarr_format=False)
    ds = f_out.create_dataset('data',
                              shape=data.shape,
                              chunks=chunk,
                              dtype='uint8',
                              compression=compression)
    ds[:] = data
    t_write = time.time() - t_write
    size = get_size(save_path)
    size /= 1e6  # divide by 1e6 to get mbytes
    return key, t_write, size


def time_write_n5(data):
    compressors_n5 = ['raw', 'gzip', 'xz', 'lz4', 'bzip2']
    times = {}
    for chunk in chunks:
        for compression in compressors_n5:
            print("Write", chunk, compression)
            key, t_write, size = write_n5(data, chunk, compression)
            times[key] = (t_write, size)
    return times


def read_n5(data, chunk, compression):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression)
    save_path = os.path.join(save_folder, 'n5_%s.n5' % key)
    t_read = time.time()
    f_out = z5py.File(save_path)
    ds = f_out['data']
    data_read = ds[:]
    t_read = time.time() - t_read
    assert np.allclose(data, data_read)
    return key, t_read


def time_read_n5(data):
    compressors_n5 = ['raw', 'gzip', 'xz', 'lz4', 'bzip2']
    times = {}
    for chunk in chunks:
        for compression in compressors_n5:
            print("Read", chunk, compression)
            key, t_read = read_n5(data, chunk, compression)
            times[key] = t_read
    return times


def write_zarr(data, chunk, compression, **compression_opts):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression if compression != 'zlib' else 'gzip')
    if compression_opts:
        key += '_'.join('%s=%s' % (str(kk), str(vv))
                        for kk, vv in compression_opts.items())
    save_path = os.path.join(save_folder, 'zarr_%s.zr' % key)
    f_out = z5py.File(save_path)
    t_write = time.time()
    ds = f_out.create_dataset('data',
                              shape=data.shape,
                              chunks=chunk,
                              dtype='uint8',
                              compression=compression,
                              **compression_opts)
    ds[:] = data
    t_write = time.time() - t_write
    size = get_size(save_path)
    size /= 1e6  # divide by 1e6 to get mbytes
    return key, t_write, size


def time_write_zarr(data):
    compressors_zarr = ['raw', 'zlib', 'blosc', 'bzip2']
    blosc_codecs = ['lz4', 'zlib']
    times = {}
    for chunk in chunks:
        for compression in compressors_zarr:
            print("Writing", chunk, compression)

            if compression == 'blosc':
                for codec in blosc_codecs:
                    for shuffle in (0, 1, 2):
                        key, t_write, size = write_zarr(data, chunk, compression,
                                                        codec=codec, shuffle=shuffle)
                        times[key] = (t_write, size)

            else:
                key, t_write, size = write_zarr(data, chunk, compression)
                times[key] = (t_write, size)
    return times


def read_zarr(data, chunk, compression, **compression_opts):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression if compression != 'zlib' else 'gzip')
    if compression_opts:
        key += '_'.join('%s=%s' % (str(kk), str(vv))
                        for kk, vv in compression_opts.items())
    save_path = os.path.join(save_folder, 'zarr_%s.zr' % key)
    t_read = time.time()
    f_out = z5py.File(save_path, use_zarr_format=True)
    ds = f_out['data']
    data_read = ds[:]
    t_read = time.time() - t_read
    assert data.shape == data_read.shape
    assert np.allclose(data, data_read), "\n%i / \n%i" % (np.sum(np.isclose(data, data_read)), data.size)
    return key, t_read


def time_read_zarr(data):
    compressors_zarr = ['raw', 'zlib', 'blosc', 'bzip2']
    blosc_codecs = ['lz4', 'zlib']
    times = {}
    for chunk in chunks:
        for compression in compressors_zarr:
            print("Read", chunk, compression)

            if compression == 'blosc':
                for codec in blosc_codecs:
                    for shuffle in (0, 1, 2):
                        key, t_read = read_zarr(data, chunk, compression,
                                                codec=codec, shuffle=shuffle)
                        times[key] = t_read

            else:
                key, t_read = read_zarr(data, chunk, compression)
                times[key] = t_read
    return times


if __name__ == '__main__':
    # TODO automatic download
    path = '/home/papec/Work/neurodata_hdd/cremi/sample_A_20160501.hdf'
    with h5py.File(path, 'r') as f:
        data = f['volumes/raw'][:]

    if os.path.exists(save_folder):
        rmtree(save_folder)
    os.mkdir(save_folder)

    print("Running hdf5 benchmarks ...")
    t_w_h5 = time_write_h5(data)
    t_r_h5 = time_read_h5(data)

    print("Running n5 benchmarks ...")
    t_w_n5 = time_write_n5(data)
    t_r_n5 = time_read_n5(data)

    print("Running zarr benchmarks ...")
    t_w_zarr = time_write_zarr(data)
    t_r_zarr = time_read_zarr(data)

    rmtree(save_folder)

    with open('bench_results_laptop.json', 'w') as f:
        json.dump({'h5-read': t_r_h5,
                   'h5-write': t_w_h5,
                   'n5-read': t_r_n5,
                   'n5-write': t_w_n5,
                   'zarr-read': t_r_zarr,
                   'zarr-write': t_w_zarr},
                  f, indent=4, sort_keys=True)
