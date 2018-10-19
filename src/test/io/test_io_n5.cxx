#include "test_helper.hxx"
#include "z5/io/io_n5.hxx"
#include "z5/dataset_factory.hxx"


namespace fs = boost::filesystem;

namespace z5 {
namespace io {

    TEST_F(IoTest, IrregularChunksN5) {
        types::ShapeType shape({20, 20, 20});
        types::ShapeType chunks({13, 5, 9});

        auto ds = createDataset("array_irregular", "float32", shape, chunks, false, "raw");

        types::ShapeType chunkShape;
        types::ShapeType expectedShape;

        // check the chunk chapes
        // chunk 0, 0, 0 -> should be complete
        types::ShapeType chunk0({0, 0, 0});
        ds->getChunkShape(chunk0, chunkShape);
        ASSERT_EQ(chunkShape.size(), 3);
        for(int d = 0; d < 3; ++d) {
            ASSERT_EQ(chunkShape[d], chunks[d]);
        }

        // chunk 1, 0, 0 -> first axis should be 7
        expectedShape = chunks;
        expectedShape[0] = 7;
        types::ShapeType chunk1({1, 0, 0});
        ds->getChunkShape(chunk1, chunkShape);
        ASSERT_EQ(chunkShape.size(), 3);
        for(int d = 0; d < 3; ++d) {
            ASSERT_EQ(chunkShape[d], expectedShape[d]);
        }

        // chunk 1, 0, 1 -> first axis should be 7
        expectedShape = chunks;
        expectedShape[0] = 7;
        types::ShapeType chunk2({1, 0, 1});
        ds->getChunkShape(chunk2, chunkShape);
        ASSERT_EQ(chunkShape.size(), 3);
        for(int d = 0; d < 3; ++d) {
            ASSERT_EQ(chunkShape[d], expectedShape[d]);
        }

        // chunk 1, 0, 2 -> first axis should be 7, 3rd 2
        expectedShape = chunks;
        expectedShape[0] = 7;
        expectedShape[2] = 2;
        types::ShapeType chunk3({1, 0, 2});
        ds->getChunkShape(chunk3, chunkShape);
        ASSERT_EQ(chunkShape.size(), 3);
        for(int d = 0; d < 3; ++d) {
            ASSERT_EQ(chunkShape[d], expectedShape[d]);
        }

    }

    TEST_F(IoTest, ReadFileN5) {
        handle::Chunk chunkHandle(ds_n5, chunk0Id, false);

        types::ShapeType shape({1000, 1000, 1000});
        ChunkIoN5 io(shape, chunkShape);

        std::vector<char> tmpData;
        ASSERT_TRUE(io.read(chunkHandle, tmpData));

        // convert the tempory binary representation into integer vector
        std::vector<int> dataOut(tmpData.size() / sizeof(int));
        ASSERT_EQ(tmpData.size() / sizeof(int), SIZE);
        memcpy(&dataOut[0], &tmpData[0], tmpData.size());

        for(size_t i = 0; i < SIZE; ++ i) {
            ASSERT_EQ(data_[i], dataOut[i]);
        }
    }


    TEST_F(IoTest, WriteFileN5) {
        handle::Chunk chunkHandle(ds_n5, chunk1Id, false);

        types::ShapeType shape({1000, 1000, 1000});
        ChunkIoN5 io(shape, chunkShape);

        std::vector<char> tmpData(SIZE, 1);
        io.write(chunkHandle, &tmpData[0], tmpData.size());
        ASSERT_TRUE(chunkHandle.exists());
    }


    TEST_F(IoTest, WriteReadFileN5) {
        handle::Chunk chunkHandle(ds_n5, chunk1Id, false);

        types::ShapeType shape({1000, 1000, 1000});
        ChunkIoN5 io(shape, chunkShape);

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
