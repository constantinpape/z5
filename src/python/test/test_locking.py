import unittest
import sys
import multiprocessing as mp
import logging

import numpy as np
import os
from shutil import rmtree
from six import add_metaclass
from abc import ABCMeta

try:
    import fcntl
except ImportError:
    fcntl = False

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py

import z5py.util


logger = logging.getLogger(__name__)


SEED = 0
TIMEOUT = 10


class OtherLock(mp.Process):
    def __init__(self, path, timeout=TIMEOUT):
        super(OtherLock, self).__init__()
        self.path = path
        self.start_event = mp.Event()
        self.stop_event = mp.Event()
        self.timeout = timeout
        self.daemon = True

    def run(self):
        with open(self.path, 'a') as f:
            fd = f.fileno()
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.start_event.set()
            assert self.stop_event.wait(self.timeout), \
                "OtherLock timed out after {}s".format(self.timeout)
            fcntl.lockf(fd, fcntl.LOCK_UN)

    def __enter__(self):
        self.start()
        self.start_event.wait()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_event.set()
        self.join()


class WriterProcess(mp.Process):
    """
    Try to trigger a race condition by synchronising multiple processes writing
    to the same dataset
    """
    def __init__(self, filepath, ds_name, offset, data, ready_event, start_event):
        super(WriterProcess, self).__init__()
        self.filepath = filepath
        self.ds_name = ds_name
        self.offset = offset
        self.data = data
        self.ready_event = ready_event
        self.start_event = start_event
        self.daemon = True

    def run(self):
        dataset = z5py.File(self.filepath)[self.ds_name]
        slicing = tuple(slice(o, o + s) for o, s in zip(self.offset, self.data.shape))
        self.ready_event.set()
        self.start_event.wait()
        dataset[slicing] = self.data


class TestLockfManager(unittest.TestCase):
    """Check that the lockf utility defined here actually works"""
    def setUp(self):
        self.logger = logging.getLogger(self.id())
        self.fpath = "test.file"
        with open(self.fpath, 'w') as f:
            f.write("some text")

    @unittest.skipUnless(fcntl, "Locking is only supported on unix")
    def test_locks_respected(self):
        logger.debug("Getting first lock")
        with OtherLock(self.fpath):
            with self.assertRaises(IOError):
                logger.debug("Attempting to open file for writing")
                with open(self.fpath) as f:
                    f.write("some text which should not be written")


@add_metaclass(ABCMeta)
class LockingTestMixin(object):
    def setUp(self):
        self.shape = (100, 100, 100)
        self.fpath = 'array.' + self.data_format
        self.file = z5py.File(self.fpath, use_zarr_format=self.data_format == 'zarr')
        self.dtype = np.uint64
        self.ds_name = "ds"
        self.dataset = self.file.create_dataset(
            self.ds_name, data=np.ones(self.shape, dtype=self.dtype), chunks=self.shape
        )
        self.block_path = z5py.util.get_block_path(self.dataset, [0, 0, 0])
        np.random.seed(SEED)
        self.logger = logging.getLogger(self.id())

    def tearDown(self):
        try:
            rmtree('array.' + self.data_format)
        except OSError:
            pass

    @unittest.skipUnless(fcntl, "Locking only supported on unix")
    def test_lockf_respected(self):
        with self.assertRaises(Exception) as captured:
            with OtherLock(self.block_path):
                self.logger.debug("Attempting to write to block")
                self.dataset[:] = np.ones(self.dataset.shape, dtype=self.dtype) * 2
        print(captured.exception)

    @unittest.skipUnless(fcntl, "Locking only supported on unix")
    def test_race_condition(self):
        n_processes = 4
        slice_size = int(self.shape[0] / n_processes)
        final_data = np.random.randint(0, 100**3, self.shape, self.dtype)
        start_event = mp.Event()
        processes = []
        ready_events = []

        for idx in range(n_processes):
            ready_events.append(mp.Event())
            offset = [idx*slice_size, 0, 0]
            data = final_data[idx*slice_size:(idx + 1)*slice_size, :, :]
            proc = WriterProcess(self.fpath, self.ds_name, offset, data, ready_events[-1], start_event)
            logger.debug("Starting %s", proc.name)
            proc.start()
            processes.append(proc)

        for ready_event in ready_events:
            ready_event.wait()

        self.logger.debug("WriterProcesses ready, starting")
        start_event.set()

        for proc in processes:
            self.logger.debug("Waiting for %s to finish", proc.name)
            proc.join()

        self.assertTrue(np.allclose(final_data, self.dataset[:]))


class TestZarrLocking(LockingTestMixin, unittest.TestCase):
    data_format = 'zarr'


class TestN5Locking(LockingTestMixin, unittest.TestCase):
    data_format = 'n5'


if __name__ == '__main__':
    unittest.main()
