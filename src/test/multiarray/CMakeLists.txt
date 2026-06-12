# add array IO test
add_executable(test_array test_array.cxx)
target_link_libraries(test_array ${TEST_LIBS} ${COMPRESSION_LIBRARIES})

# add broadcast test
add_executable(test_broadcast test_broadcast.cxx)
target_link_libraries(test_broadcast ${TEST_LIBS} ${COMPRESSION_LIBRARIES})

# add nd tests
add_executable(test_array_nd test_array_nd.cxx)
target_link_libraries(test_array_nd ${TEST_LIBS} ${COMPRESSION_LIBRARIES})

# add array util test (copyView / fillView / subview)
add_executable(test_array_util test_array_util.cxx)
target_link_libraries(test_array_util ${TEST_LIBS})
