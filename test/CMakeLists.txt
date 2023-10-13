file(GLOB_RECURSE sources_c "${PROJECT_SOURCE_DIR}/source/*.c")
file(GLOB_RECURSE sources_cpp "${PROJECT_SOURCE_DIR}/source/*.cpp")

add_library(lindorm ${sources_c} ${sources_cpp})

include_directories(${PROJECT_SOURCE_DIR}/test)

function(add TEST_FILE)
  get_filename_component(TEST_NAME ${TEST_FILE} NAME_WE)
  add_executable(${TEST_NAME} ${TEST_FILE})
  target_link_libraries(${TEST_NAME} PRIVATE 
    lindorm
  )
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  message(STATUS "ADD TEST : ${TEST_NAME}")
endfunction()

FILE(GLOB_RECURSE  test_files *_test.cpp)

foreach(test_file ${test_files})
  add(${test_file})
endforeach(test_file)
