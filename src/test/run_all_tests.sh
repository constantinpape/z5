echo "Running Metadata Test"
./test_metadata

echo "Running Dataset Test"
./test_dataset

echo "Running Factories Test"
./test_factories

echo "Running Attributes Test"
./test_attributes

echo "Running Compression Tests"
./compression/test_raw
if [ -f ./compression/test_blosc ]; then
    ./compression/test_blosc
fi
if [ -f ./compression/test_bzip2 ]; then
    ./compression/test_bzip2
fi
if [ -f ./compression/test_lz4 ]; then
    ./compression/test_lz4
fi
if [ -f ./compression/test_xz ]; then
    ./compression/test_xz
fi
if [ -f ./compression/test_zlib ]; then
    ./compression/test_zlib
fi

echo "Running Multiarray Tests"
./multiarray/test_array_util
./multiarray/test_broadcast
./multiarray/test_array
./multiarray/test_array_nd

# don't run n5 tests in test runner, we can't run them in travis
# echo "Running N5 Tests"
# cd test_n5
# ./run_test.bash
# cd ..
