# add tests that read / write from n5
add_executable(test_n5 test_n5.cxx)
target_link_libraries(test_n5 ${TEST_LIBS} ${COMPRESSION_LIBRARIES})

add_custom_command(
        TARGET test_n5 POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_SOURCE_DIR}/src/test/test_n5/MakeTestData.java
                ${CMAKE_CURRENT_BINARY_DIR}
)

#add_custom_command(
#        TARGET test_python POST_BUILD
#        COMMAND ${CMAKE_COMMAND} -E copy
#                ${CMAKE_SOURCE_DIR}/src/test/test_zarr_python/test_read_python.py
#                ${CMAKE_CURRENT_BINARY_DIR}
#)

add_custom_command(
        TARGET test_n5 POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_SOURCE_DIR}/src/test/test_n5/run_test.bash
                ${CMAKE_CURRENT_BINARY_DIR}
)
