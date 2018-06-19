import os
import zipfile
import time

from subprocess import call
from shutil import rmtree
from skimage import io

import numpy as np
import z5py
from z5py.dataset import Dataset

BENCH_DIR = 'bench_dir'
IM_URL = "https://imagej.nih.gov/ij/images/t1-head-raw.zip"


def set_up():
    """
    Make the tmp directory and load the test image
    """
    if os.path.exists(BENCH_DIR):
        rmtree(BENCH_DIR)
    os.mkdir(BENCH_DIR)

    im_file = os.path.join(BENCH_DIR, 'im.zip')
    call(['wget', '-O', im_file, IM_URL])

    with zipfile.ZipFile(im_file) as f:
        f.extract('JeffT1_le.tif', BENCH_DIR)

    im_file = os.path.join(BENCH_DIR, 'JeffT1_le.tif')
    im = np.array(io.imread(im_file))
    return im[30:94, 100:164, 100:164].astype('uint8')


def benchmark_writing_speed(data):
    n_blocks = 5
    n_reps = 1

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

    for n in range(n_reps):
        for compression in compression_methods:
            t0 = time.time()
            ds = f.create_dataset('ds_%s' % compression, shape=shape,
                                  chunks=chunks, compression='gzip', dtype='uint8')
            ds[:] = bdata
            t1 = time.time()
            t_diff = t1 - t0
            print("%i : %s : %f s" % (n, compression, t_diff))


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

            t0 = time.time()
            ds = f.create_dataset('ds_%s_%i' % (compression, n_threads), shape=shape,
                                  chunks=chunks, compression='gzip', dtype='uint8',
                                  n_threads=n_threads)
            ds[:] = bdata

            t1 = time.time()
            t_diff = t1 - t0
            print("%i : %s : %f s" % (n_threads, compression, t_diff))


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
