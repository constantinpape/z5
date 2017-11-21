import os
import time
import numpy as np
import h5py
import json


shape = (100, 1024, 1024)
bb = np.s_[:100, :1024, :1024]
chunks = [(1, 512, 512),
          (64, 64, 64)]


def single_write(data, chunk, compression):
    opts = 4 if compression == 'gzip' else None
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression)
    save_path = os.path.join(save_folder,
                             'h5_%s.h5' % key)
    t_write = time.time()
    with h5py.File(save_path, 'w') as f_out:
        ds = f_out.create_dataset('data',
                                  shape=shape,
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
    compressors_h5 = [None, 'gzip', 'lzf']
    times = {}
    for chunk in chunks:
        for compression in compressors_h5:
            key, t_write, size = single_write(data, chunk, compression)
            times[key] = (t_write, size)
    with open('./results/reswrite_h5.json', 'w') as f:
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

    time_write_h5(data)
