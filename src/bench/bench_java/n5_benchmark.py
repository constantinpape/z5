import os

from shutil import rmtree

import numpy as np
import z5py
from z5py.dataset import Dataset
import z5py.util

BENCH_DIR = 'bench_dir'


def set_up():
    """
    Make the tmp directory and load the test image
    """
    if os.path.exists(BENCH_DIR):
        rmtree(BENCH_DIR)
    os.mkdir(BENCH_DIR)

    im = z5py.util.fetch_test_data()
    return im[30:94, 100:164, 100:164]


def benchmark_writing_speed(data):
    n_blocks = 5

    shape = 3 * (n_blocks * 64,)
    chunks = 3 * (64,)

    compression_methods = Dataset.compressors_n5
    f = z5py.File(os.path.join(BENCH_DIR, 'f1.n5'))

    bdata = np.zeros(shape, dtype='uint8')
    for i in range(n_blocks):
        for j in range(n_blocks):
            for k in range(n_blocks):
                bdata[i*64:(i + 1)*64,
                      j*64:(j + 1)*64,
                      k*64:(k + 1)*64] = data

    for compression in compression_methods:
        with z5py.util.Timer() as t:
            ds = f.create_dataset('ds_%s' % compression, shape=shape,
                                  chunks=chunks, compression=compression, dtype='uint8')
            ds[:] = bdata
        print("%i : %s : %f s" % (1, compression, t.elapsed))


def benchmark_parallel_writing_speed(data):
    n_blocks = 5

    shape = 3 * (n_blocks * 64,)
    chunks = 3 * (64,)

    compression_methods = Dataset.compressors_n5
    f = z5py.File(os.path.join(BENCH_DIR, 'f2.n5'))

    bdata = np.zeros(shape, dtype='uint8')
    for i in range(n_blocks):
        for j in range(n_blocks):
            for k in range(n_blocks):
                bdata[i*64:(i + 1)*64,
                      j*64:(j + 1)*64,
                      k*64:(k + 1)*64] = data

    for n_threads in range(1, 9):
        n_threads *= 2
        print(n_threads, 'threads.')
        for compression in compression_methods:
            with z5py.util.Timer() as t:
                ds = f.create_dataset('ds_%s_%i' % (compression, n_threads), shape=shape,
                                      chunks=chunks, compression=compression, dtype='uint8',
                                      n_threads=n_threads)
                ds[:] = bdata

            print("%i : %s : %f s" % (n_threads, compression, t.elapsed))


def clean_up():
    try:
        rmtree(BENCH_DIR)
    except OSError:
        pass


if __name__ == '__main__':
    data = set_up()
    benchmark_writing_speed(data)
    benchmark_parallel_writing_speed(data)
    clean_up()
