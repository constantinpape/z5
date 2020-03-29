# z5 cmake module
# This module sets the following variables in your project:
#
#   z5_FOUND - true if z5 found on the system
#   z5_INCLUDE_DIRS - the directory containing z5 headers
#   z5_LIBRARY - empty

@PACKAGE_INIT@

include(CMakeFindDependencyMacro)
find_dependency(nlohmann_json)

if(NOT TARGET @PROJECT_NAME@)
    include("${CMAKE_CURRENT_LIST_DIR}/@PROJECT_NAME@-targets.cmake")
    get_target_property(@PROJECT_NAME@_INCLUDE_DIRS @PROJECT_NAME@ INTERFACE_INCLUDE_DIRECTORIES)
endif()

# TODO check if we can use this for compression libraries
# if(Z5_USE_ZLIB)
#     find_dependency(zlib)
# endif()
