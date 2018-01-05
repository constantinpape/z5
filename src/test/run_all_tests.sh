echo "Running Metadata Test"
./test_metadata

echo "Running Dataset Test"
./test_dataset

echo "Running Factories Test"
./test_factories

echo "Running Attributes Test"
./test_attributes

echo "Running Compression Tests"
echo "Raw Compression"
./compression/test_raw
echo "Zlib Compression"
./compression/test_zlib
echo "Bzip2 Compression"
./compression/test_bzip2
echo "Blosc Compression"
./compression/test_blosc

echo "Running IO Tests"
echo "Io N5"
./io/test_io_n5
echo "Io Zarr"
./io/test_io_zarr

echo "Running Multiarray Tests"
if [ -f ./multiarray/test_marray ]; then
    echo "Marray Tests"
    ./multiarray/test_marray
fi

echo "XT Tests"
./multiarray/test_broadcast
./multiarray/test_xtensor
./multiarray/test_xtnd

echo "Running Zarr Tests"
cd test_zarr
./run_test.bash
cd ..

echo "Running N5 Tests"
cd test_n5
./test_n5/run_test.bash
cd ..
