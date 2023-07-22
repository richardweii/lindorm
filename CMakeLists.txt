cmake_minimum_required(VERSION 2.8.12)
project(lindorm-tsdb-contest-cpp)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# We use g++-12 to compile, so that we recommend you to use the same compiler to test for better compatibility.

message("Build Type: ${CMAKE_BUILD_TYPE} ${CMAKE_CXX_FLAGS}")
if( CMAKE_BUILD_TYPE STREQUAL "Debug" )
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Wno-unused-variable")
else( CMAKE_BUILD_TYPE STREQUAL "Release" )
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
endif( CMAKE_BUILD_TYPE STREQUAL "Debug" )

message("Your OS: ${CMAKE_HOST_SYSTEM_NAME}")
if (CMAKE_HOST_SYSTEM_NAME MATCHES "Darwin")
    set(CMAKE_C_COMPILER "gcc-12")
    set(CMAKE_CXX_COMPILER "g++-12")
elseif (MAKE_HOST_SYSTEM_NAME MATCHES "Windows")
    # Add w64devkit\bin to PATH.
else ()
    set(CMAKE_C_COMPILER "gcc")
    set(CMAKE_CXX_COMPILER "g++")
endif ()

include_directories(${PROJECT_SOURCE_DIR}/include)
add_subdirectory(example)

file(GLOB_RECURSE sources_c "${PROJECT_SOURCE_DIR}/source/*.c")
file(GLOB_RECURSE sources_cpp "${PROJECT_SOURCE_DIR}/source/*.cpp")

add_library(yourDbLib ${sources_c} ${sources_cpp})
