# define test libraries
SET(TEST_LIBS
    gtest
    gtest_main
    ${CMAKE_THREAD_LIBS_INIT}
    ${FILESYSTEM_LIBRARIES}
)

# add metadata test
add_executable(test_metadata test_metadata.cxx)
target_link_libraries(test_metadata ${TEST_LIBS})

# add zarray test
add_executable(test_dataset test_dataset.cxx)
target_link_libraries(test_dataset ${TEST_LIBS} ${COMPRESSION_LIBRARIES})

# add factory test
add_executable(test_factories test_factories.cxx)
target_link_libraries(test_factories ${TEST_LIBS} ${COMPRESSION_LIBRARIES})

# add attributes test
add_executable(test_attributes test_attributes.cxx)
target_link_libraries(test_attributes ${TEST_LIBS} ${COMPRESSION_LIBRARIES})

add_subdirectory(compression)
add_subdirectory(io)
add_subdirectory(multiarray)
add_subdirectory(test_n5)
add_subdirectory(util)

add_custom_command(
        TARGET test_attributes POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_SOURCE_DIR}/src/test/run_all_tests.sh
                ${CMAKE_CURRENT_BINARY_DIR}
)
