cmake_minimum_required(VERSION 2.8.12)

set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_INSTALL_RPATH ${PROJECT_SOURCE_DIR}/example/lib)
set(CMAKE_MACOSX_RPATH 1)
if( CMAKE_BUILD_TYPE STREQUAL "Debug" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -fPIC -g -ggdb -gdwarf-4 -gstrict-dwarf -Wall -Wno-unused-variable")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -g -ggdb -gdwarf-4 -gstrict-dwarf -Wall -Wno-unused-variable")
else( CMAKE_BUILD_TYPE STREQUAL "Release" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -fPIC -O3")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -O3")
endif( CMAKE_BUILD_TYPE STREQUAL "Debug" )

add_library(testLib ${PROJECT_SOURCE_DIR}/source/struct/ColumnValue.cpp
        ${PROJECT_SOURCE_DIR}/source/struct/Row.cpp
        ${PROJECT_SOURCE_DIR}/source/struct/Schema.cpp
        ${PROJECT_SOURCE_DIR}/source/struct/Vin.cpp
        ${PROJECT_SOURCE_DIR}/example/TSDBEngineSample.cpp)

install (TARGETS testLib DESTINATION ${PROJECT_SOURCE_DIR}/example/lib)
add_executable(testMain ${PROJECT_SOURCE_DIR}/example/EvaluationSample.cpp)
add_executable(testColumnValue ${PROJECT_SOURCE_DIR}/example/ColumnValueUsageSample.cpp)
target_link_libraries(testMain testLib)
target_link_libraries(testColumnValue testLib)