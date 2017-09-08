# define test libraries
SET(TEST_LIBS
    ${Boost_FILESYSTEM_LIBRARY}    
    ${Boost_SYSTEM_LIBRARY}    
    gtest
    gtest_main
    pthread
)

# add metadata test
add_executable(test_metadata test_metadata.cxx)
target_link_libraries(test_metadata ${TEST_LIBS})

# add zarray test
add_executable(test_zarray test_zarray.cxx)
target_link_libraries(test_zarray ${TEST_LIBS} ${BLOSC_LIBRARIES})

# add factory test
add_executable(test_factories test_factories.cxx)
target_link_libraries(test_factories ${TEST_LIBS} ${BLOSC_LIBRARIES})

add_subdirectory(compression)
add_subdirectory(io)
add_subdirectory(test_zarr_python)
#add_subdirectory(test_n5)
