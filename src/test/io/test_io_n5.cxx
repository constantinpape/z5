#include "test_helper.hxx"
#include "z5/io/io_n5.hxx"

namespace fs = boost::filesystem;

namespace z5 {
namespace io {

    // TODO TODO TODO
    // Tests to check for correct irregular chunk shapes
    // TODO TODO TODO

    TEST_F(IoTest, ReadFileN5) {
        handle::Chunk chunkHandle(ds_n5, chunk0Id, false);

        types::ShapeType shape({1000, 1000, 1000});
        ChunkIoN5<int> io(shape, chunkShape);

        std::vector<int> tmpData;
        ASSERT_TRUE(io.read(chunkHandle, tmpData));
        ASSERT_EQ(tmpData.size(), SIZE);

        for(size_t i = 0; i < SIZE; ++ i) {
            ASSERT_EQ(data_[i], tmpData[i]);
        }
    }


    TEST_F(IoTest, WriteFileN5) {
        handle::Chunk chunkHandle(ds_n5, chunk1Id, false);

        types::ShapeType shape({1000, 1000, 1000});
        ChunkIoN5<int> io(shape, chunkShape);

        std::vector<int> tmpData(SIZE, 0);
        io.write(chunkHandle, tmpData);
        ASSERT_TRUE(chunkHandle.exists());
    }


    TEST_F(IoTest, WriteReadFileN5) {
        handle::Chunk chunkHandle(ds_n5, chunk1Id, false);

        types::ShapeType shape({1000, 1000, 1000});
        ChunkIoN5<int> io(shape, chunkShape);

        std::vector<int> tmpData1(SIZE);
        std::copy(data_, data_ + SIZE, tmpData1.begin());
        io.write(chunkHandle, tmpData1);
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
