#include "test_helper.hxx"
#include "z5/io/io_zarr.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace io {

    TEST_F(IoTest, ReadFileZarr) {
        handle::Chunk chunkHandle(ds_zarr, chunk0Id, true);
        ChunkIoZarr io;

        std::vector<char> tmpData;
        ASSERT_TRUE(io.read(chunkHandle, tmpData));

        // convert the tempory binary representation into integer vector
        std::vector<int> dataOut(tmpData.size() / sizeof(int));
        ASSERT_EQ(dataOut.size(), SIZE);
        memcpy(&dataOut[0], &tmpData[0], tmpData.size());

        for(size_t i = 0; i < SIZE; ++ i) {
            ASSERT_EQ(data_[i], dataOut[i]);
        }
    }


    TEST_F(IoTest, WriteFileZarr) {
        handle::Chunk chunkHandle(ds_zarr, chunk1Id, true);
        ChunkIoZarr io;

        std::vector<char> tmpData(SIZE, 1);
        io.write(chunkHandle, &tmpData[0], tmpData.size());
        ASSERT_TRUE(chunkHandle.exists());
    }


    TEST_F(IoTest, WriteReadFileZarr) {
        handle::Chunk chunkHandle(ds_zarr, chunk1Id, true);
        ChunkIoZarr io;

        std::vector<char> tmpData1(SIZE * sizeof(int));
        memcpy(&tmpData1[0], data_, SIZE * sizeof(int));
        io.write(chunkHandle, &tmpData1[0], tmpData1.size());
        ASSERT_TRUE(chunkHandle.exists());

        std::vector<char> tmpData2;
        ASSERT_TRUE(io.read(chunkHandle, tmpData2));
        ASSERT_EQ(tmpData2.size(), SIZE * sizeof(int));

        // convert the tempory binary representation into integer vector
        std::vector<int> dataOut(tmpData2.size() / sizeof(int));
        ASSERT_EQ(dataOut.size(), SIZE);
        memcpy(&dataOut[0], &tmpData2[0], tmpData2.size());

        for(size_t i = 0; i < SIZE; ++ i) {
            ASSERT_EQ(data_[i], dataOut[i]);
        }
    }

}
}
