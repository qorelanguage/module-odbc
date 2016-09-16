cmake_minimum_required(VERSION 2.8.3)

project(qore-odbc-module)

set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "release")
endif (NOT CMAKE_BUILD_TYPE)
string(TOLOWER ${CMAKE_BUILD_TYPE} QORE_BUILD_TYPE_LWR)
if (${QORE_BUILD_TYPE_LWR} MATCHES "debug")
    add_definitions(-DDEBUG)
else ()
    add_definitions(-DNDEBUG)
endif ()

add_definitions(-DUNICODE)

FIND_PACKAGE (Qore REQUIRED)
FIND_PACKAGE (ODBC REQUIRED)

# Check for C++11.
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if(COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
endif()


set(CPP_SRC
    src/odbc-module.cpp
    src/ODBCConnection.cpp
    src/ODBCStatement.cpp
    src/ODBCPreparedStatement.cpp
)

set(module_name "odbc")

add_library(${module_name} SHARED ${CPP_SRC} ${QPP_SOURCES})

target_include_directories(${module_name} PUBLIC ${ODBC_INCLUDE_DIR})

target_link_libraries(${module_name} ${ODBC_LIBRARIES})

qore_binary_module(${module_name} "0.0.1")

qore_dist("${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")

qore_config_info()

