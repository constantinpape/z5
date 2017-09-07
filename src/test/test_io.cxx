#include <random>
#define BOOST_FILESYSTEM_NO_DEPERECATED
#include <boost/filesystem.hpp>

#include "gtest/gtest.h"

#include "zarr++/io/io.hxx"

namespace fs = boost::filesystem;

namespace zarr {
namespace io {

    // fixture for the chunk io
    class IoTest : public ::testing::Test {

    protected:
        IoTest() : size2_(100*100*100) {

        }

        virtual ~IoTest() {

        }

        virtual void SetUp() {
            // fill 'data_' with random values
            std::default_random_engine generator;
            std::uniform_int_distribution<int> distribution(0, 1000);
            auto draw = std::bind(distribution, generator);
            for(size_t i = 0; i < size_; ++i) {
                data_[i] = draw();
            }

            // make array
            fs::path arr("array.zr");
            fs::create_directory(arr);

            // write to file
            fs::fstream file("array.zr/0", std::ios::out | std::ios::binary);
            file.write((char*) data_, size_*sizeof(int));
        }

        virtual void TearDown() {
            fs::path file("array.zr");
            fs::remove_all(file);
        }

        const static size_t size_ = 100*100*100;
        int data_[size_];

        // FIXME for some reasons, I get linker errors when calling size_ from the outside,
        // so here we have size2_ ....
        size_t size2_;

    };


    TEST_F(IoTest, ReadFile) {
        handle::Chunk chunkHandle("array.zr/0");
        ChunkIo<int> io;

        std::vector<int> tmpData;
        ASSERT_TRUE(io.read(chunkHandle, tmpData));
        ASSERT_EQ(tmpData.size(), size2_);

        for(size_t i = 0; i < size2_; ++ i) {
            ASSERT_EQ(data_[i], tmpData[i]);
        }
    }


    TEST_F(IoTest, WriteFile) {
        handle::Chunk chunkHandle("array.zr/1");
        ChunkIo<int> io;

        std::vector<int> tmpData(size2_, 0);
        io.write(chunkHandle, tmpData);
        ASSERT_TRUE(chunkHandle.exists());
    }


    TEST_F(IoTest, WriteReadFile) {
        handle::Chunk chunkHandle("array.zr/1");
        ChunkIo<int> io;

        std::vector<int> tmpData1(size2_);
        std::copy(data_, data_ + size2_, tmpData1.begin());
        io.write(chunkHandle, tmpData1);
        ASSERT_TRUE(chunkHandle.exists());

        std::vector<int> tmpData2;
        ASSERT_TRUE(io.read(chunkHandle, tmpData2));
        ASSERT_EQ(tmpData2.size(), size2_);

        for(size_t i = 0; i < size2_; ++ i) {
            ASSERT_EQ(data_[i], tmpData2[i]);
        }
    }

}
}
