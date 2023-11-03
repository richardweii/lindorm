set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
cmake_minimum_required(VERSION 2.8.12)
project(lindorm-tsdb-contest-cpp)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# We use g++-12 to compile, so that we recommend you to use the same compiler to test for better compatibility.
set(CMAKE_CXX_STANDARD 17)
message("Build Type: ${CMAKE_BUILD_TYPE} ${CMAKE_CXX_FLAGS}")
if( CMAKE_BUILD_TYPE STREQUAL "Debug" )
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Wno-unused-variable -avx512")
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

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/source/third_party)

add_subdirectory(test)