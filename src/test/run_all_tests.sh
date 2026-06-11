#!/bin/bash
# run every gtest binary in the current build's src/test directory;
# set -e so a single failing test suite fails the whole run
set -e

echo "Running Handle Test"
./test_handle

echo "Running Metadata Test"
./test_metadata

echo "Running Dataset Test"
./test_dataset

echo "Running Factories Test"
./test_factories

echo "Running Attributes Test"
./test_attributes

echo "Running Util Test"
./util/test_util

echo "Running Compression Tests"
./compression/test_raw
for codec in blosc bzip2 lz4 xz zlib zstd; do
    if [ -f "./compression/test_${codec}" ]; then
        "./compression/test_${codec}"
    fi
done

echo "Running Multiarray Tests"
./multiarray/test_array_util
./multiarray/test_broadcast
./multiarray/test_array
./multiarray/test_array_nd

# the s3 tests skip themselves unless Z5PY_S3_ENDPOINT points to a reachable
# endpoint (e.g. a local moto server)
if [ -f ./s3/test_handle_s3 ]; then
    echo "Running S3 Handle Test"
    ./s3/test_handle_s3
fi
if [ -f ./s3/test_factories_s3 ]; then
    echo "Running S3 Factories Test"
    ./s3/test_factories_s3
fi
