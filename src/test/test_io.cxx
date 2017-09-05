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
        IoTest() {

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
            // write to file
            fs::fstream file("chunk0.zr", std::ios::out | std::ios::binary);
            file.write((char*) data_, size_*sizeof(int));
        }

        virtual void TearDown() {
            fs::path chunk0("chunk0.zr");
            fs::remove(chunk0);

            fs::path chunk1("chunk1.zr");
            if(fs::exists(chunk1)) {
                fs::remove(chunk1);
            }
        }

        const static size_t size_ = 100*100*100;
        int data_[size_];

    };

    TEST_F(IoTest, ReadFile) {
        handle::Chunk chunkHandle("chunk0.zr");
        ChunkIo<int> io(size_, 0);

        // TODO try without proper size initialization
        int tmpData[size_];
        ASSERT_TRUE(io.read(chunkHandle, tmpData));

        for(size_t i = 0; i < size_; ++ i) {
            ASSERT_EQ(data_[i], tmpData[i]);
        }
    }

    TEST_F(IoTest, ReadEmptyFile) {
        handle::Chunk chunkHandle("chunk2.zr");

        std::vector<int> fillValues({-100, -1, 0, 1, 100});

        for(auto fillVal : fillValues) {
            ChunkIo<int> io(size_, fillVal);

            int tmpData[size_];
            ASSERT_FALSE(io.read(chunkHandle, tmpData));

            for(size_t i = 0; i < size_; ++ i) {
                ASSERT_EQ(tmpData[i], fillVal);
            }
        }
    }

    TEST_F(IoTest, WriteFile) {
        handle::Chunk chunkHandle("chunk1.zr");
        ChunkIo<int> io(size_, 0);

        io.write(chunkHandle, data_, size_ * sizeof(int));
        ASSERT_TRUE(chunkHandle.exists());
    }

    TEST_F(IoTest, WriteReadFile) {
        handle::Chunk chunkHandle("chunk1.zr");
        ChunkIo<int> io(size_, 0);

        io.write(chunkHandle, data_, size_ * sizeof(int));
        ASSERT_TRUE(chunkHandle.exists());

        int tmpData[size_];
        ASSERT_TRUE(io.read(chunkHandle, tmpData));

        for(size_t i = 0; i < size_; ++ i) {
            ASSERT_EQ(data_[i], tmpData[i]);
        }
    }

}
}
