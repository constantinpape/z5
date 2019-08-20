addPythonModule(
    NESTED_NAME
        z5py
    SOURCES
        z5py.cxx
        dataset.cxx
        handles.cxx
        factory.cxx
        # util.cxx
    LIBRRARIES
        ${COMPRESSION_LIBRARIES} 
        ${FILESYSTEM_LIBRARIES}
        ${CLOUD_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
)
