import sys
import os
import time
import numpy as np
import h5py
import json

sys.path.append('../../../bld/python')
import z5py

# small shape for debugging
#shape = (2, 1024, 1024)
#bb = np.s_[:shape[0], :shape[1], :shape[2]]

shape = (100, 1024, 1024)
bb = np.s_[:100, :1024, :1024]
chunks = [(1, 512, 512),
          (64, 64, 64)]

# result_folder = './results_laptop'
result_folder = './results'


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
    save_path = os.path.join(save_folder, 'n5_%s.n5' % key)
    t_write = time.time()
    f_out = z5py.File(save_path, use_zarr_format=False)
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


def time_write_n5(data):
    # compressors_n5 = ['raw', 'gzip', 'bzip2']
    compressors_n5 = ['raw',]
    times = {}
    for chunk in chunks:
        for compression in compressors_n5:
            print("Write", chunk, compression)
            key, t_write, size = single_write(data, chunk, compression)
            times[key] = (t_write, size)
    with open(os.path.join(result_folder, 'reswrite_n5.json'), 'w') as f:
        json.dump(times, f, indent=4, sort_keys=True)


if __name__ == '__main__':
    # path = '/home/consti/sampleA+_raw_automatically_realigned.h5'
    path = '/home/papec/Work/playground/z5_tests/sampleA+_raw_automatically_realigned.h5'

    save_folder = './tmp_files'
    if not os.path.exists(save_folder):
        os.mkdir(save_folder)
    if not os.path.exists(result_folder):
        os.mkdir(result_folder)

    with h5py.File(path, 'r') as f:
        data = f['data'][bb]

    time_write_n5(data)
