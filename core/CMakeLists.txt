cmake_minimum_required(VERSION 2.8)
project(cs8803_workshop6)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

#set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build)
#set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build)
#set(CMAKE_BINARY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/build)
#set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModules/")


set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost REQUIRED COMPONENTS log thread system filesystem program_options)
find_package(Casablanca REQUIRED)
find_package(AzureStorageCpp REQUIRED)
find_package(SSL REQUIRED)
find_package(glog REQUIRED)

#find_package(Threads REQUIRED)
#find_package(Zookeeper REQUIRED)
#find_package(Check REQUIRED)
#find_package(Conservator REQUIRED)
find_package(Protobuf REQUIRED)


include_directories(${CHECK_INCLUDE_DIRS})
#include_directories(${ZOOKEEPER_INCLUDE_DIR}/zookeeper)
#include_directories(${CONSERVATOR_INCLUDE_DIR}/conservator)
include_directories(/usr/local/include)
include_directories(rpc_generated)

link_directories(/usr/local/lib)
link_libraries(grpc++_unsecure grpc gpr ${PROTOBUF_LIBRARY})

file(GLOB MY_SERVER_SRC worker-server.cpp split_file.cpp "rpc_generated/*.cc")
file(GLOB MY_CLIENT_SRC master-client.cpp split_file.cpp "rpc_generated/*.cc")
add_executable(worker ${MY_SERVER_SRC})
add_executable(master ${MY_CLIENT_SRC})
target_link_libraries(worker glog::glog ${Boost_LIBRARIES} ${SSL_LIBRARIES} ${CASABLANCA_LIBRARY} ${AZURE_STORAGE_LIBRARY})
target_link_libraries(master glog::glog ${Boost_LIBRARIES} ${SSL_LIBRARIES} ${CASABLANCA_LIBRARY} ${AZURE_STORAGE_LIBRARY})
