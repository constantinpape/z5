# add tests that read / write from a zarr object created with the python lib
add_executable(test_python test_python.cxx)
target_link_libraries(test_python ${TEST_LIBS} ${BLOSC_LIBRARIES})
