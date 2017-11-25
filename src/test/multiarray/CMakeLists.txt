# add marray test
add_executable(test_marray test_marray.cxx)
target_link_libraries(test_marray ${TEST_LIBS} ${COMPRESSION_LIBRARIES})

# add xtensor test
add_executable(test_xtensor test_xtensor.cxx)
target_link_libraries(test_xtensor ${TEST_LIBS} ${COMPRESSION_LIBRARIES})

# add broadcast test
add_executable(test_broadcast test_broadcast.cxx)
target_link_libraries(test_broadcast ${TEST_LIBS} ${COMPRESSION_LIBRARIES})
