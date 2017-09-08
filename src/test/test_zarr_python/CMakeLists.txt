# add tests that read / write from a zarr object created with the python lib
add_executable(test_python test_python.cxx)
target_link_libraries(test_python ${TEST_LIBS} ${COMPRESSION_LIBRARIES})

add_custom_command(
        TARGET test_python POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_SOURCE_DIR}/src/test/test_zarr_python/make_data.py
                ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_command(
        TARGET test_python POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_SOURCE_DIR}/src/test/test_zarr_python/test_read_python.py
                ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_command(
        TARGET test_python POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_SOURCE_DIR}/src/test/test_zarr_python/run_test.bash
                ${CMAKE_CURRENT_BINARY_DIR}
)
