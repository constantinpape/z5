#include "gtest/gtest.h"

#include <random>

#include "zarr++/compression/gzip_compressor.hxx"
#include "zarr++/metadata.hxx"

#include "test_helper.hxx"

namespace zarr {
namespace compression {


    TEST_F(CompressionTest, GzipCompressInt) {

        // Test compression with default values
        ArrayMetadata metadata;
        metadata.compressorId = "gzip";
        metadata.compressorName = "gzip";
        metadata.compressorLevel = 5;
        GzipCompressor<int> compressor(metadata);

        std::vector<int> dataOut;
        compressor.compress(dataInt_, dataOut, SIZE);

        ASSERT_TRUE(dataOut.size() < SIZE);
        std::cout << "Compression Int: " << dataOut.size() << " / " << SIZE << std::endl;

    }

}
}
