# add handle test
add_executable(test_handle_s3 test_handle_s3.cxx)
target_link_libraries(test_handle_s3 ${TEST_LIBS} ${CLOUD_LIBRARIES})

# add factory test
add_executable(test_factories_s3 test_factories_s3.cxx)
target_link_libraries(test_factories_s3 ${TEST_LIBS} ${COMPRESSION_LIBRARIES} ${CLOUD_LIBRARIES})
