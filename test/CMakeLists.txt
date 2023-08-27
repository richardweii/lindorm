file(GLOB_RECURSE sources_c "${PROJECT_SOURCE_DIR}/source/*.c")
file(GLOB_RECURSE sources_cpp "${PROJECT_SOURCE_DIR}/source/*.cpp")

add_library(lindorm ${sources_c} ${sources_cpp})

include_directories(${PROJECT_SOURCE_DIR}/test)
file(GLOB_RECURSE test_cpp "${PROJECT_SOURCE_DIR}/test/*.cpp")

add_executable(my_test test.cpp)
target_link_libraries(my_test lindorm)

add_executable(hard_test hard_test.cpp)
target_link_libraries(hard_test lindorm)

add_executable(playground playground.cpp)
target_link_libraries(playground lindorm)

add_executable(column_usage ColumnValueUsageSample.cpp)
target_link_libraries(column_usage lindorm)
