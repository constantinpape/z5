#include "test_helper.hxx"
#include "z5/io/io_zarr.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace io {

    TEST_F(IoTest, ReadFileZarr) {
        handle::Chunk chunkHandle(ds_zarr, chunk0Id, true);
        ChunkIoZarr<int> io;

        std::vector<int> tmpData;
        ASSERT_TRUE(io.read(chunkHandle, tmpData));
        ASSERT_EQ(tmpData.size(), SIZE);

        for(size_t i = 0; i < SIZE; ++ i) {
            ASSERT_EQ(data_[i], tmpData[i]);
        }
    }


    TEST_F(IoTest, WriteFileZarr) {
        handle::Chunk chunkHandle(ds_zarr, chunk1Id, true);
        ChunkIoZarr<int> io;

        std::vector<int> tmpData(SIZE, 0);
        io.write(chunkHandle, &tmpData[0], tmpData.size());
        ASSERT_TRUE(chunkHandle.exists());
    }


    TEST_F(IoTest, WriteReadFileZarr) {
        handle::Chunk chunkHandle(ds_zarr, chunk1Id, true);
        ChunkIoZarr<int> io;

        std::vector<int> tmpData1(SIZE);
        std::copy(data_, data_ + SIZE, tmpData1.begin());
        io.write(chunkHandle, &tmpData1[0], tmpData1.size());
        ASSERT_TRUE(chunkHandle.exists());

        std::vector<int> tmpData2;
        ASSERT_TRUE(io.read(chunkHandle, tmpData2));
        ASSERT_EQ(tmpData2.size(), SIZE);

        for(size_t i = 0; i < SIZE; ++ i) {
            ASSERT_EQ(data_[i], tmpData2[i]);
        }
    }

}
}
