# add blosc test
if(WITH_BLOSC)
    add_executable(test_blosc test_blosc.cxx )
    target_link_libraries(test_blosc ${TEST_LIBS} ${BLOSC_LIBRARIES})
endif()

# add gzip tests
if(WITH_GZIP)
    add_executable(test_gzip test_gzip.cxx)
    target_link_libraries(test_gzip ${TEST_LIBS} ${GZIP_LIBRARIES})
endif()
