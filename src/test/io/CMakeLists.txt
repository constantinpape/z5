# add zarr io test
add_executable(test_io_zarr test_io_zarr.cxx )
target_link_libraries(test_io_zarr ${TEST_LIBS})

# add n5 io test
add_executable(test_io_n5 test_io_n5.cxx )
target_link_libraries(test_io_n5 ${TEST_LIBS} ${COMPRESSION_LIBRARIES})
