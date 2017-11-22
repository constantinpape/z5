import sys
import os
import time
import numpy as np
import h5py
import json

sys.path.append('../../../bld/python')
import z5py


save_folder = './tmp_files'
shape = (100, 1024, 1024)
bb = np.s_[:100, :1024, :1024]
chunks = [(1, 512, 512),
          (64, 64, 64)]

result_folder = './results_laptop'


def single_read(data, chunk, compression):
    key = '%s_%s' % ('_'.join(str(cc) for cc in chunk),
                     compression)
    save_path = os.path.join(save_folder,
                             'n5_%s.n5' % key)
    t_read = time.time()
    f_out = z5py.File(save_path, use_zarr_format=False)
    ds = f_out['data']
    data_read = ds[:]
    t_read = time.time() - t_read
    # from volumina_viewer import volumina_n_layer
    # volumina_n_layer([data.astype('float32'), data_read.astype('float32')])
    # assert np.allclose(data, data_read)
    return key, t_read


def time_read_n5(data):
    compressors_n5 = ['raw', ]#, 'gzip', 'bzip2']
    times = {}
    for chunk in chunks:
        for compression in compressors_n5:
            print("Read", chunk, compression)
            key, t_read = single_read(data, chunk, compression)
            times[key] = t_read
    with open(os.path.join(result_folder, 'resread_n5.json'), 'w') as f:
        json.dump(times, f, indent=4, sort_keys=True)


if __name__ == '__main__':
    path = '/home/consti/sampleA+_raw_automatically_realigned.h5'
    # path = '/home/papec/Work/playground/z5_tests/sampleA+_raw_automatically_realigned.h5'

    with h5py.File(path, 'r') as f:
        data = f['data'][bb]

    time_read_n5(data)
