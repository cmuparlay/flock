cmake_minimum_required (VERSION 3.9)

#### USER DEFINED ##############################################################

project(utils-tests)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_EXTENSIONS OFF)

#### BASIC SETTINGS ############################################################

include_directories(${PROJECT_SOURCE_DIR})

set (CMAKE_CXX_FLAGS "-march=native -Wall -Wextra -O3 -g")

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

#### FIND PACKAGES #############################################################

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

#### MAKE TESTS ################################################################

add_executable( c_buffer_test src/test_circular_buffer.cpp)

add_executable( concurrent_c_buffer_test src/test_concurrent_circular_buffer.cpp)
target_link_libraries(concurrent_c_buffer_test PRIVATE Threads::Threads)

add_executable( mpsc_buffer_test src/test_many_producer_single_consumer_buffer.cpp)
target_link_libraries(mpsc_buffer_test PRIVATE Threads::Threads)

add_executable( rec_test src/test_reclamation_strategies.cpp)
target_link_libraries(rec_test PRIVATE Threads::Threads)

add_executable( slinked_list_test src/test_singly_linked_list.cpp)
target_link_libraries(slinked_list_test PRIVATE Threads::Threads)

add_executable( protected_list_test src/test_protected_list.cpp)
target_link_libraries(protected_list_test PRIVATE Threads::Threads)
