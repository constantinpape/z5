#pragma once

#include <random>
#define BOOST_FILESYSTEM_NO_DEPERECATED
#include <boost/filesystem.hpp>

#include "gtest/gtest.h"
#include "z5/util/util.hxx"
#include "z5/handle/handle.hxx"

#define SIZE 100*100*100

namespace fs = boost::filesystem;

namespace z5 {
namespace io {

    // fixture for the chunk io
    class IoTest : public ::testing::Test {

    protected:

        IoTest() : chunkShape({100, 100, 100}), ds_zarr("array.zr"), ds_n5("array.n5"), chunk0Id({0, 0, 0}), chunk1Id({1, 1, 1}) {
        }

        virtual void SetUp() {
            // fill 'data_' with random values
            std::default_random_engine generator;
            std::uniform_int_distribution<int> distribution(0, 1000);
            auto draw = std::bind(distribution, generator);
            for(std::size_t i = 0; i < SIZE; ++i) {
                data_[i] = draw();
            }

            // make arrays
            fs::path arr("array.zr");
            fs::create_directory(arr);

            fs::path arr5("array.n5");
            fs::create_directory(arr5);

            // write to file
            fs::fstream file("array.zr/0.0.0", std::ios::out | std::ios::binary);
            file.write((char*) data_, SIZE*sizeof(int));

            // write to file
            fs::path n5_chunk("array.n5/0/0");
            fs::create_directories(n5_chunk);
            fs::fstream file1("array.n5/0/0/0", std::ios::out | std::ios::binary);
            // need to write header first
            uint16_t mode = 0; // TODO support the varlength mode as well
            util::reverseEndiannessInplace(mode);
            file1.write((char*) &mode, 2);
            //
            uint16_t nDims = 3;
            util::reverseEndiannessInplace(nDims);
            file1.write((char *) &nDims, 2);

            std::vector<uint32_t> tmp(chunkShape.begin(), chunkShape.end());
            util::reverseEndiannessInplace<uint32_t>(tmp.begin(), tmp.end());
            for(int d = 0; d < tmp.size(); ++d) {
                file1.write((char *) &tmp[d], 4);
            }

            // don't need to reverse the endianness here, because this
            // is only done after compression and not in the io class
            file1.write((char*) data_, SIZE*sizeof(int));
        }

        virtual void TearDown() {
            fs::path file_zr("array.zr");
            fs::remove_all(file_zr);
            fs::path file_n5("array.n5");
            fs::remove_all(file_n5);
            fs::path file_irr("array_irregular");
            if(fs::exists(file_irr)) {
                fs::remove_all(file_irr);
            }
        }

        int data_[SIZE];
        types::ShapeType chunkShape;
        handle::Dataset ds_zarr;
        handle::Dataset ds_n5;
        types::ShapeType chunk0Id;
        types::ShapeType chunk1Id;
    };

}
}
