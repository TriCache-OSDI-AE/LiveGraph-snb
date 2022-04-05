cmake_minimum_required(VERSION 3.1)
project(LiveGraph-snb CXX)

set (CMAKE_CXX_STANDARD 17)
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)

find_package(OpenMP REQUIRED)

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
pkg_check_modules(THRIFT REQUIRED thrift>=0.13)
include_directories(${THRIFT_INCLUDE_DIRS})
link_directories(${THRIFT_LIBRARY_DIRS})
add_definitions(${THRIFT_CFLAGS_OTHER})

set(THRIFT_GEN_PATH ${CMAKE_BINARY_DIR}/gen-cpp)
set(THRIFT_CODE_PATH ${CMAKE_SOURCE_DIR}/thrift)
set(ROCKSDB_CODE_PATH ${CMAKE_SOURCE_DIR}/deps/rocksdb)
set(LIVEGRAPH_CODE_PATH ${CMAKE_SOURCE_DIR}/deps/LiveGraph)
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

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING
      "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif(NOT CMAKE_BUILD_TYPE)

include(ExternalProject)
ExternalProject_Add(
    rocksdblib
    SOURCE_DIR ${ROCKSDB_CODE_PATH}
    INSTALL_DIR ${PROJECT_BINARY_DIR}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=<INSTALL_DIR>
               -DWITH_TESTS=OFF
               -DWITH_BENCHMARK_TOOLS=OFF
               -DWITH_TOOLS=OFF
               -DWITH_GFLAGS=OFF
               -DWITH_SNAPPY=OFF
               -DWITH_ZSTD=OFF
               -DFAIL_ON_WARNINGS=OFF
               -DROCKSDB_BUILD_SHARED=ON
               -DCMAKE_BUILD_TYPE=Release
)

include_directories(${PROJECT_BINARY_DIR}/include)
link_directories(${PROJECT_BINARY_DIR}/lib)
set(PROJECT_DEPS_DIR ${PROJECT_SOURCE_DIR}/deps)
include_directories(${PROJECT_SOURCE_DIR} ${PROJECT_DEPS_DIR})

set(USE_SHARED_LIVEGRAPH OFF CACHE BOOL "Use Shared LiveGraph.")
if(USE_SHARED_LIVEGRAPH)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_SHARED_LIVEGRAPH")
    set(BUILD_SHARED_LIVEGRAPH ON)
endif()
add_subdirectory(${PROJECT_DEPS_DIR}/LiveGraph)

aux_source_directory(src SOURCES)
add_executable(snb_server ${SOURCES} ${THRIFT_GEN_PATH}/interactive_constants.cpp ${THRIFT_GEN_PATH}/Interactive.cpp ${THRIFT_GEN_PATH}/interactive_types.cpp)

#remove deprecated warning
target_compile_definitions(snb_server PRIVATE -DHAS_UNCAUGHT_EXCEPTIONS=1)

if(USE_SHARED_LIVEGRAPH)
    target_link_libraries(snb_server Threads::Threads OpenMP::OpenMP_CXX ${TBB_LIBRARIES} rocksdb thrift livegraph)
else()
    target_link_libraries(snb_server Threads::Threads OpenMP::OpenMP_CXX ${TBB_LIBRARIES} rocksdb thrift corelib)
endif()
add_dependencies(snb_server rocksdblib)

option(WITH_TRICACHE "build with TriCache" OFF)
set(TRICACHE_DIR $ENV{TRICACHE_ROOT}/build CACHE STRING "build directory of TriCache")

if(WITH_TRICACHE)
    if(NOT ${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang")
        message(FATAL_ERROR "TriCache requires Clang 13.0.1.")
    endif()
    if(USE_SHARED_LIVEGRAPH)
        message(FATAL_ERROR "TriCache requires corelib.")
    endif()
    message(STATUS "Enabling TriCache.")
    set(CMAKE_AR llvm-ar-13)
    set(CMAKE_RANLIB llvm-ranlib-13)

    target_compile_options(snb_server PUBLIC -flto=thin -fpass-plugin=${TRICACHE_DIR}/llvm-plugin/TriCacheInstrument/TriCacheInstrument.so -fpass-plugin=${TRICACHE_DIR}/llvm-plugin/TriCacheAlloc/TriCacheAlloc.so)
    target_compile_options(corelib PUBLIC -flto=thin -fpass-plugin=${TRICACHE_DIR}/llvm-plugin/TriCacheInstrument/TriCacheInstrument.so -fpass-plugin=${TRICACHE_DIR}/llvm-plugin/TriCacheAlloc/TriCacheAlloc.so)
    target_link_directories(snb_server PUBLIC ${TRICACHE_DIR})

    target_link_libraries(snb_server -flto=thin cache)

    find_package(PkgConfig)
    pkg_check_modules(SPDK REQUIRED spdk_nvme spdk_env_dpdk spdk_syslibs)
    link_directories(${SPDK_LIBRARY_DIRS})
    message("(${SPDK_INCLUDE_DIRS})")
    message("(${SPDK_LIBRARIES_DIRS})")
    message("(${SPDK_LIBRARIES})")
    target_link_libraries(snb_server -Wl,--whole-archive ${SPDK_LIBRARIES} -Wl,--no-whole-archive)
    target_link_libraries(snb_server boost_fiber boost_context boost_thread boost_filesystem boost_atomic numa dl)

endif()
