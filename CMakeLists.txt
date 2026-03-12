cmake_minimum_required(VERSION 3.16)
project(dag_task_scheduler LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(dag_scheduler src/main.cpp)
target_include_directories(dag_scheduler PRIVATE include)

add_executable(dag_scheduler_tests tests/test_scheduler.cpp)
target_include_directories(dag_scheduler_tests PRIVATE include)

enable_testing()
add_test(NAME dag_scheduler_tests COMMAND dag_scheduler_tests)

if (MSVC)
    target_compile_options(dag_scheduler PRIVATE /W4)
    target_compile_options(dag_scheduler_tests PRIVATE /W4)
else()
    target_compile_options(dag_scheduler PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(dag_scheduler_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()
