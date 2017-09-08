# add blosc test
add_executable(test_blosc test_blosc.cxx )
target_link_libraries(test_blosc ${TEST_LIBS} ${BLOSC_LIBRARIES})
