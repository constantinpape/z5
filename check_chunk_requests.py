import numpy as np
import z5py

p = './data.n5'
f = z5py.File(p, 'a')

data = np.arange(100*100).reshape((100, 100))
ds = f.require_dataset('test', shape=data.shape, dtype=data.dtype, chunks=(10, 10))

chunk_list = ds.get_chunks_in_request(np.s_[5:15, 3:95])
print(len(chunk_list))

chunk_list, chunk_bbs = ds.get_chunks_in_request(np.s_[5:15, 3:95], True)

for chunk_id, chunk_bb in zip(chunk_list, chunk_bbs):
    print(chunk_id)
    print(chunk_bb)
    print()
