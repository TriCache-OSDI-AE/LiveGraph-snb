cmake_minimum_required(VERSION 3.1)
project(LiveGraph-snb CXX)

set (CMAKE_CXX_STANDARD 14)
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)
find_package(OpenMP)
if (OPENMP_FOUND)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")
find_package(TBB REQUIRED)
include_directories(${TBB_INCLUDE_DIRS})

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
else()
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no march native support.")
endif()

find_package(PkgConfig)
pkg_check_modules(THRIFT REQUIRED thrift)
include_directories(${THRIFT_INCLUDE_DIRS})
link_directories(${THRIFT_LIBRARY_DIRS})
add_definitions(${THRIFT_CFLAGS_OTHER})

set(THRIFT_GEN_PATH ${CMAKE_BINARY_DIR}/gen-cpp)
set(THRIFT_CODE_PATH ${CMAKE_SOURCE_DIR}/thrift)
add_custom_command(
OUTPUT
    ${THRIFT_GEN_PATH}/interactive_constants.cpp
    ${THRIFT_GEN_PATH}/interactive_constants.h
    ${THRIFT_GEN_PATH}/Interactive.cpp
    ${THRIFT_GEN_PATH}/Interactive.h
    ${THRIFT_GEN_PATH}/interactive_types.cpp
    ${THRIFT_GEN_PATH}/interactive_types.h
COMMAND
    thrift --gen cpp ${THRIFT_CODE_PATH}/interactive.thrift
DEPENDS
    ${THRIFT_CODE_PATH}/interactive.thrift
WORKING_DIRECTORY
    ${CMAKE_BINARY_DIR}
)
include_directories(${THRIFT_GEN_PATH})

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DNDEBUG")
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING
      "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif(NOT CMAKE_BUILD_TYPE)

set(PROJECT_DEPS_DIR ${PROJECT_SOURCE_DIR}/deps)
include_directories(${PROJECT_SOURCE_DIR} ${PROJECT_DEPS_DIR})

add_executable(snb_server src/server.cpp ${THRIFT_GEN_PATH}/interactive_constants.cpp ${THRIFT_GEN_PATH}/Interactive.cpp ${THRIFT_GEN_PATH}/interactive_types.cpp)

target_link_libraries(snb_server Threads::Threads ${TBB_LIBRARIES} ${THRIFT_LIBRARIES})
