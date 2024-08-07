###########################################################################
# create a custom target which bundles all python building and copying
###########################################################################
add_custom_target(python-module)


###########################################################################
# copy all python files from:
#    src/python/module
#    src/python/test
# recursively into
#   /blddir/python/
#   /blddir/python/test
###########################################################################
file(GLOB_RECURSE  ${MODULE_NAME}_PYTHON_FILES
    "${CMAKE_SOURCE_DIR}/src/python/module/z5py/*.py"
)
FOREACH(PYTHON_FILENAME   ${${MODULE_NAME}_PYTHON_FILES})
    string(REPLACE "${CMAKE_SOURCE_DIR}/src/python/module/" ""
        RELATIVE_FILENAME
        ${PYTHON_FILENAME})
    string(REPLACE "/" "_"
        RELATIVE_FILENAME_TARGET_NAME
        ${RELATIVE_FILENAME})
    get_filename_component(FILE_DIR  ${CMAKE_BINARY_DIR}/${RELATIVE_FILENAME} DIRECTORY)
    file(MAKE_DIRECTORY ${FILE_DIR})

    add_custom_target(copy_${RELATIVE_FILENAME_TARGET_NAME} ALL
        COMMAND cmake -E copy_if_different
        ${PYTHON_FILENAME}
        ${CMAKE_BINARY_DIR}/python/${RELATIVE_FILENAME})
    add_dependencies(python-module copy_${RELATIVE_FILENAME_TARGET_NAME})
ENDFOREACH()

file(GLOB_RECURSE  ${MODULE_NAME}_PYTHON_FILES
    "${CMAKE_SOURCE_DIR}/src/python/test/*.py"
)
FOREACH(PYTHON_FILENAME   ${${MODULE_NAME}_PYTHON_FILES})
    string(REPLACE "${CMAKE_SOURCE_DIR}/src/python/test/" ""
        RELATIVE_FILENAME
        ${PYTHON_FILENAME})
    string(REPLACE "/" "_"
        RELATIVE_FILENAME_TARGET_NAME
        ${RELATIVE_FILENAME})
    get_filename_component(FILE_DIR  ${CMAKE_BINARY_DIR}/${RELATIVE_FILENAME} DIRECTORY)
    file(MAKE_DIRECTORY ${FILE_DIR})

    add_custom_target(copy_${RELATIVE_FILENAME_TARGET_NAME} ALL
        COMMAND cmake -E copy_if_different
        ${PYTHON_FILENAME}
        ${CMAKE_BINARY_DIR}/python/test/${RELATIVE_FILENAME})
    add_dependencies(python-module copy_${RELATIVE_FILENAME_TARGET_NAME})
ENDFOREACH()


###########################################################################
# macro to simplify the adding of a submodule
###########################################################################
macro(addPythonModule)

    set(options "")
    set(oneValueArgs NESTED_NAME)
    set(multiValueArgs SOURCES LIBRARIES)
    cmake_parse_arguments(ADD_PY_MOD "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN} )

    # get name of the module
    string(REPLACE "/" ";" MOD_NESTING_LIST "${ADD_PY_MOD_NESTED_NAME}")
    list(LENGTH MOD_NESTING_LIST nesting_len)
    LIST(GET MOD_NESTING_LIST ${LENGTH}-1 MODULE_NAME)


    #Create the binding library
    pybind11_add_module(_${MODULE_NAME}
        ${ADD_PY_MOD_SOURCES}
    )
    
    # TODO Dunno if this is important, it produces a very ugly warning...
    #if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    #  target_compile_options(_${MODULE_NAME} PRIVATE "-fvisibility=default")
    #endif()

    set_target_properties(_${MODULE_NAME}  PROPERTIES PREFIX "")

    # link additional libraries
    target_link_libraries(_${MODULE_NAME}
        PUBLIC
        ${ADD_PY_MOD_LIBRARIES}
    )

    IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        set_target_properties(_${MODULE_NAME} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
    ELSEIF(${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
        target_link_libraries(_${MODULE_NAME}
            PUBLIC
            ${PYTHON_LIBRARIES}
        )
    ENDIF()

    # create module dir
    file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/python/${ADD_PY_MOD_NESTED_NAME})

    # copy the module file
    ADD_CUSTOM_COMMAND(
        TARGET _${MODULE_NAME}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} ARGS -E copy_if_different $<TARGET_FILE:_${MODULE_NAME}>
        ${CMAKE_BINARY_DIR}/python/${ADD_PY_MOD_NESTED_NAME}/
        COMMENT "Copying target ${MODULE_NAME} to temporary module directory")

    add_dependencies(python-module _${MODULE_NAME})
endmacro()

add_subdirectory(lib)


###########################
# INSTALL THE PYTHON MODULE
###########################

# Find the python install dir
IF(NOT DEFINED PYTHON_MODULE_INSTALL_DIR OR PYTHON_MODULE_INSTALL_DIR MATCHES "^$")
    
    execute_process(
      COMMAND "${PYTHON_EXECUTABLE}" -c 
       "from __future__ import print_function; import sysconfig; print(sysconfig.get_path('platlib'))"
      OUTPUT_VARIABLE PYTHON_SITE
      OUTPUT_STRIP_TRAILING_WHITESPACE)

    SET(PYTHON_MODULE_INSTALL_DIR ${PYTHON_SITE})

ENDIF()


SET(PYTHON_MODULE_INSTALL_DIR ${PYTHON_MODULE_INSTALL_DIR}
    CACHE PATH "where to install the z5py package" FORCE)

# this is the install path relative to CMAKE_INSTALL_PREFIX,
# use this in INSTALL() commands to get packaging right
FILE(RELATIVE_PATH PYTHON_MODULE_INSTALL_DIR ${CMAKE_INSTALL_PREFIX} ${PYTHON_MODULE_INSTALL_DIR})


install(DIRECTORY ${CMAKE_BINARY_DIR}/python/z5py
    DESTINATION ${PYTHON_MODULE_INSTALL_DIR})
