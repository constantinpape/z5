# add raw test
add_executable(test_raw test_raw.cxx)
target_link_libraries(test_raw ${TEST_LIBS})

# add blosc test
if(WITH_BLOSC)
    add_executable(test_blosc test_blosc.cxx )
    target_link_libraries(test_blosc ${TEST_LIBS} ${BLOSC_LIBRARIES})
endif()

# add gzip tests
if(WITH_ZLIB)
    add_executable(test_zlib test_zlib.cxx)
    target_link_libraries(test_zlib ${TEST_LIBS} ${ZLIB_LIBRARIES})
endif()

# add bzip tests
if(WITH_BZIP2)
    add_executable(test_bzip2 test_bzip2.cxx)
    target_link_libraries(test_bzip2 ${TEST_LIBS} ${BZIP2_LIBRARIES})
endif()

# add xz tests
if(WITH_XZ)
    add_executable(test_xz test_xz.cxx)
    target_link_libraries(test_xz ${TEST_LIBS} ${LIBLZMA_LIBRARIES})
endif()

# add lz4 tests
if(WITH_LZ4)
    add_executable(test_lz4 test_lz4.cxx)
    target_link_libraries(test_lz4 ${TEST_LIBS} ${LZ4_LIBRARY})
endif()
