cmake_minimum_required(VERSION 3.0.0)
project(lock-free-search-tree VERSION 0.1.0)

include(CTest)
enable_testing()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-deprecated-declarations")

# Include thread library
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

# Add mrlock source files here
set(MRLOCK_SOURCE_DIR ${PROJECT_SOURCE_DIR}/lib/mrlock/src)
set(
    MRLOCK_SOURCE
)

# Add main source files here
set(
    SOURCE
    ${PROJECT_SOURCE_DIR}/lfca.cpp
    ${PROJECT_SOURCE_DIR}/mrlocktree.cpp
    ${PROJECT_SOURCE_DIR}/treap.cpp
)

# Add test source files here
set(
    TEST_SOURCE
    ${PROJECT_SOURCE_DIR}/test/test_treap.cpp
    ${PROJECT_SOURCE_DIR}/test/test_lfcatree.cpp
)

set(MAIN ${PROJECT_SOURCE_DIR}/main.cpp)

add_executable(lfca ${SOURCE} ${MRLOCK_SOURCE} ${MAIN})
add_executable(TEST ${SOURCE} ${MRLOCK_SOURCE} ${TEST_SOURCE})

target_link_libraries(lfca Threads::Threads)
target_link_libraries(TEST Threads::Threads)

target_include_directories(lfca PRIVATE ${MRLOCK_SOURCE_DIR})
target_include_directories(TEST PRIVATE ${MRLOCK_SOURCE_DIR})

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)

# Download and unpack googletest at configure time
configure_file(CMakeLists-gtest.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                 ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL)

# The gtest/gtest_main targets carry header search path
# dependencies automatically when using CMake 2.8.11 or
# later. Otherwise we have to add them here ourselves.
if (CMAKE_VERSION VERSION_LESS 2.8.11)
    include_directories("${gtest_SOURCE_DIR}/include")
endif()

# Link test library
target_link_libraries(TEST gtest_main)
