import argparse
import json
import os
import time
from shutil import rmtree

import numpy as np
import h5py
import z5py


# (25, 125, 125) fits the default data without edge chunks
# chunks = [(1, 512, 512), (64, 64, 64), (25, 125, 125)]
chunks = [(25, 125, 125)]


def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


# TODO support copts
def _write_result(results, result, compression, chunk, **copts):
    comp_key = 'raw' if compression is None else compression
    ckey = '_'.join(map(str, chunk))
    if comp_key in results:
        results[comp_key][ckey] = result
    else:
        results[comp_key] = {ckey: result}
    return results


def read_h5(data, chunk, compression):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression if compression is not None else 'raw')
    save_path = os.path.join(save_folder, 'h5_%s.h5' % key)
    read_times = []
    for _ in range(iterations):
        t_read = time.time()
        with h5py.File(save_path, 'r') as f_out:
            data_read = f_out['data'][:]
        t_read = time.time() - t_read
        read_times.append(t_read)
    assert np.allclose(data, data_read)
    return read_times


def time_read_h5(data):
    compressors_h5 = [None, 'gzip']
    times = {}
    for chunk in chunks:
        for compression in compressors_h5:
            print("Read", chunk, compression)
            t_read = read_h5(data, chunk, compression)
            times = _write_result(times, t_read, compression, chunk)
    return times


def write_h5(data, chunk, compression):
    opts = 5 if compression == 'gzip' else None
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression if compression is not None else 'raw')
    save_path = os.path.join(save_folder, 'h5_%s.h5' % key)
    write_times = []
    for _ in range(iterations):
        if os.path.exists(save_path):
            os.remove(save_path)
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
        write_times.append(t_write)
    size = os.path.getsize(save_path)
    size /= 1e6  # divide by 1e6 to get mbytes
    return write_times, size


def time_write_h5(data):
    compressors_h5 = [None, 'gzip']
    times = {}
    sizes = {}
    for chunk in chunks:
        for compression in compressors_h5:
            print("Write", chunk, compression)
            t_write, size = write_h5(data, chunk, compression)
            print("Data size", size)
            times = _write_result(times, t_write, compression, chunk)
            sizes = _write_result(sizes, size, compression, chunk)
    return times, sizes


def write_n5(data, chunk, compression):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression)
    save_path = os.path.join(save_folder, 'n5_%s.n5' % key)
    write_times = []
    for _ in range(iterations):
        if os.path.exists(save_path):
            rmtree(save_path)
        t_write = time.time()
        f_out = z5py.File(save_path, use_zarr_format=False)
        ds = f_out.create_dataset('data',
                                  shape=data.shape,
                                  chunks=chunk,
                                  dtype='uint8',
                                  compression=compression)
        ds[:] = data
        t_write = time.time() - t_write
        write_times.append(t_write)
    size = get_size(save_path)
    size /= 1e6  # divide by 1e6 to get mbytes
    return write_times, size


def time_write_n5(data):
    compressors_n5 = z5py.Dataset.compressors_n5
    times = {}
    sizes = {}
    for chunk in chunks:
        for compression in compressors_n5:
            print("Write", chunk, compression)
            t_write, size = write_n5(data, chunk, compression)
            print("Data size", size)
            times = _write_result(times, t_write, compression, chunk)
            sizes = _write_result(sizes, size, compression, chunk)
    return times, sizes


def read_n5(data, chunk, compression):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression)
    save_path = os.path.join(save_folder, 'n5_%s.n5' % key)
    read_times = []
    for _ in range(iterations):
        t_read = time.time()
        f_out = z5py.File(save_path)
        ds = f_out['data']
        data_read = ds[:]
        t_read = time.time() - t_read
        read_times.append(t_read)
    assert np.allclose(data, data_read)
    return read_times


def time_read_n5(data):
    compressors_n5 = z5py.Dataset.compressors_n5
    times = {}
    for chunk in chunks:
        for compression in compressors_n5:
            print("Read", chunk, compression)
            t_read = read_n5(data, chunk, compression)
            times = _write_result(times, t_read, compression, chunk)
    return times


def write_zarr(data, chunk, compression, **compression_opts):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk), compression)
    if compression_opts:
        key += '_'.join('%s=%s' % (str(kk), str(vv))
                        for kk, vv in compression_opts.items())
    save_path = os.path.join(save_folder, 'zarr_%s.zr' % key)
    write_times = []
    for _ in range(iterations):
        if os.path.exists(save_path):
            rmtree(save_path)
        t_write = time.time()
        f_out = z5py.File(save_path)
        ds = f_out.create_dataset('data',
                                  shape=data.shape,
                                  chunks=chunk,
                                  dtype='uint8',
                                  compression=compression,
                                  **compression_opts)
        ds[:] = data
        t_write = time.time() - t_write
        write_times.append(t_write)
    size = get_size(save_path)
    size /= 1e6  # divide by 1e6 to get mbytes
    return write_times, size


def time_write_zarr(data, benchmark_blosc_options=False):
    compressors_zarr = z5py.Dataset.compressors_zarr
    blosc_codecs = ['lz4', 'zlib']
    times = {}
    sizes = {}
    for chunk in chunks:
        for compression in compressors_zarr:
            print("Writing", chunk, compression)

            if compression == 'blosc' and benchmark_blosc_options:
                raise NotImplementedError  # TODO implement this again
                for codec in blosc_codecs:
                    for shuffle in (0, 1, 2):
                        t_write, size = write_zarr(data, chunk, compression,
                                                   codec=codec, shuffle=shuffle)
                        times = _write_result(times, t_write, compression, chunk,
                                              codec=codec, shuffle=shuffle)
                        sizes = _write_result(sizes, size, compression, chunk,
                                              codec=codec, shuffle=shuffle)

            else:
                t_write, size = write_zarr(data, chunk, compression)
                times = _write_result(times, t_write, compression, chunk)
                sizes = _write_result(sizes, size, compression, chunk)
            print("Data size", size)
    return times, sizes


def read_zarr(data, chunk, compression, **compression_opts):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk), compression)
    if compression_opts:
        key += '_'.join('%s=%s' % (str(kk), str(vv))
                        for kk, vv in compression_opts.items())
    save_path = os.path.join(save_folder, 'zarr_%s.zr' % key)
    read_times = []
    for _ in range(iterations):
        t_read = time.time()
        f_out = z5py.File(save_path, use_zarr_format=True)
        ds = f_out['data']
        data_read = ds[:]
        t_read = time.time() - t_read
        read_times.append(t_read)
    assert data.shape == data_read.shape
    assert np.allclose(data, data_read), "\n%i / \n%i" % (np.sum(np.isclose(data, data_read)),
                                                          data.size)
    return read_times


def time_read_zarr(data, benchmark_blosc_options=False):
    compressors_zarr = z5py.Dataset.compressors_zarr
    blosc_codecs = ['lz4', 'zlib']
    times = {}
    for chunk in chunks:
        for compression in compressors_zarr:
            print("Read", chunk, compression)

            if compression == 'blosc' and benchmark_blosc_options:
                raise NotImplementedError  # TODO implement this again
                for codec in blosc_codecs:
                    for shuffle in (0, 1, 2):
                        t_read = read_zarr(data, chunk, compression,
                                           codec=codec, shuffle=shuffle)
                        times = _write_result(times, t_read, compression, chunk,
                                              codec=codec, shuffle=shuffle)

            else:
                t_read = read_zarr(data, chunk, compression)
                times = _write_result(times, t_read, compression, chunk)
    return times


if __name__ == '__main__':
    # default file:
    # https://cremi.org/static/data/sample_A_20160501.hdf
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', '-p', required=True)
    parser.add_argument('--name', '-n', default=None)
    parser.add_argument('--save_folder', '-s', default='./tmp_files')
    parser.add_argument('--iterations', '-i', default=5)

    args = parser.parse_args()
    path = args.path
    name = args.name
    save_folder = args.save_folder
    iterations = args.iterations

    with h5py.File(path, 'r') as f:
        data = f['volumes/raw'][:]

    if os.path.exists(save_folder):
        rmtree(save_folder)
    os.mkdir(save_folder)

    print("Running hdf5 benchmarks ...")
    t_w_h5, sizes_h5 = time_write_h5(data)
    t_r_h5 = time_read_h5(data)

    print("Running n5 benchmarks ...")
    t_w_n5, sizes_n5 = time_write_n5(data)
    t_r_n5 = time_read_n5(data)

    print("Running zarr benchmarks ...")
    t_w_zarr, sizes_zarr = time_write_zarr(data)
    t_r_zarr = time_read_zarr(data)

    rmtree(save_folder)

    res_path = 'bench_results.json' if name is None else f'bench_results_{name}.json'
    with open(res_path, 'w') as f:
        json.dump({'h5': {'read': t_r_h5, 'write': t_w_h5, "sizes": sizes_h5},
                   'n5': {'read': t_r_n5, 'write': t_w_n5, "sizes": sizes_n5},
                   'zarr': {'read': t_r_zarr, 'write': t_w_zarr, "sizes": sizes_zarr}},
                  f, indent=2, sort_keys=True)
