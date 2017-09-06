# define test libraries
SET(TEST_LIBS
    ${Boost_FILESYSTEM_LIBRARY}    
    ${Boost_SYSTEM_LIBRARY}    
    gtest
    gtest_main
    pthread
)
#message(STATUS ${TEST_LIBS})

# add io test
add_executable(test_io test_io.cxx )
target_link_libraries(test_io ${TEST_LIBS})

# add blosc test
add_executable(test_blosc test_blosc.cxx )
target_link_libraries(test_blosc ${TEST_LIBS} ${BLOSC_LIBRARIES})

# add metadata test
add_executable(test_metadata test_metadata.cxx )
target_link_libraries(test_metadata ${TEST_LIBS})
