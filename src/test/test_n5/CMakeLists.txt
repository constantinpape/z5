# add tests that read / write from n5
add_executable(test_n5 test_n5.cxx)
target_link_libraries(test_n5 ${TEST_LIBS} ${BLOSC_LIBRARIES})
