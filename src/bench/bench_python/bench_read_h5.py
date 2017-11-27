import os
import time
import numpy as np
import h5py
import json


save_folder = './tmp_files'
shape = (100, 1024, 1024)
bb = np.s_[:100, :1024, :1024]
chunks = [(1, 512, 512),
          (64, 64, 64)]

#result_folder = './results_laptop'
result_folder = './results'


def single_read(data, chunk, compression):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression)
    save_path = os.path.join(save_folder,
                             'h5_%s.h5' % key)
    t_read = time.time()
    with h5py.File(save_path, 'r') as f_out:
        ds = f_out['data']
        data_read = ds[:]
    t_read = time.time() - t_read
    assert np.allclose(data, data_read)
    return key, t_read


def time_read_h5(data):
    # compressors_h5 = [None, 'gzip', 'lzf']
    compressors_h5 = [None,]
    times = {}
    for chunk in chunks:
        for compression in compressors_h5:
            print("Read", chunk, compression)
            key, t_read = single_read(data, chunk, compression)
            times[key] = t_read
    with open(os.path.join(result_folder, 'resread_h5.json'), 'w') as f:
        json.dump(times, f, indent=4, sort_keys=True)


if __name__ == '__main__':
    # path = '/home/consti/sampleA+_raw_automatically_realigned.h5'
    path = '/home/papec/Work/playground/z5_tests/sampleA+_raw_automatically_realigned.h5'

    with h5py.File(path, 'r') as f:
        data = f['data'][bb]

    time_read_h5(data)
