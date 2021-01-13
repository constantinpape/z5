# TODO remove before merging into master

import z5py
import numpy as np
import os
from shutil import copyfile, rmtree

filename = 'test_chunk_requests' + '.n5'
f = z5py.File(filename)

test_shape = (30, 10, 5, 20, 3, 32)
# test_shape = (30, 10, 5)
test_data = np.random.rand(*test_shape)
chunk_shape = (10, 1, 1, 12, 1, 6)
# chunk_shape = (10, 1, 1)


#remove dataset if it already exists before creating it
if os.path.isdir(filename + '/dim_test'): rmtree(filename + '/dim_test')

print(filename)
dset = f.create_dataset('dim_test',
                        shape=test_shape,
                        chunks=chunk_shape,
                        dtype=test_data.dtype)
dset[:] = test_data

read = [2, 8, 3, 4, 0, 1, 16, 19, 2, 3, 11, 19]
subset_test = test_data[read[0]:read[1],
                        read[2]:read[3],
                        read[4]:read[5],
                        read[6]:read[7],
                        read[8]:read[9],
                        read[10]:read[11]]


chunk_ids = dset.get_chunks_in_request(np.s_[read[0]:read[1],
                                             read[2]:read[3],
                                             read[4]:read[5],
                                             read[6]:read[7],
                                             read[8]:read[9],
                                             read[10]:read[11]])

compare_file = filename + '_compare' + '.n5'
if os.path.isdir(compare_file):  os.system('rm -r ' + compare_file)

# create a new empty data container
f = z5py.File(compare_file)
dset = f.create_dataset('dim_test',
                        shape=test_shape,
                        chunks=chunk_shape,
                        dtype=test_data.dtype)

# copy just those chunks into container
for ids in chunk_ids:
    ids_str = '/'.join(map(str, ids))
    # make the chunk dir if it's not there
    os.makedirs(os.path.dirname(compare_file + '/dim_test/' + ids_str), exist_ok=True)
    copyfile(filename + '/dim_test/' + ids_str, compare_file + '/dim_test/' + ids_str)

# performe the subset chunk read
compare_data = dset[read[0]:read[1],
                    read[2]:read[3],
                    read[4]:read[5],
                    read[6]:read[7],
                    read[8]:read[9],
                    read[10]:read[11]]

if not np.all(subset_test == compare_data):
    raise ValueError('The chunk id list did not produce an array that matched the source')
