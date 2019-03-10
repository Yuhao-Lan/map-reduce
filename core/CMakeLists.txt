cmake_minimum_required(VERSION 2.8)
project(cs8803_workshop6)


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -lazurestorage -lcpprest -lboost_system -lcrypto -lssl")
#add_definitions("-std=c++11 -lazurestorage -lcpprest -lboost_system -lcrypto -lssl")
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
#set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build)
#set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build)
#set(CMAKE_BINARY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/build)
#set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMakeModules/")

#find_package(Threads REQUIRED)
#find_package(Zookeeper REQUIRED)
#find_package(Check REQUIRED)
#find_package(Conservator REQUIRED)
find_package(Boost 1.66 REQUIRED COMPONENTS system)
find_package(Protobuf REQUIRED)
find_package(glog REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
find_package(Boost REQUIRED COMPONENTS log thread system filesystem program_options)
find_package(Casablanca REQUIRED)
find_package(AzureStorageCpp REQUIRED)
find_package(SSL REQUIRED)
add_definitions(-std=c++11 -lazurestorage -lcpprest -lboost_system -lcrypto -lssl)

include_directories(${CHECK_INCLUDE_DIRS})
#include_directories(${ZOOKEEPER_INCLUDE_DIR}/zookeeper)
#include_directories(${CONSERVATOR_INCLUDE_DIR}/conservator)
include_directories(/usr/local/include)
include_directories(rpc_generated)

link_directories(/usr/local/lib)
link_libraries(grpc++_unsecure grpc gpr ${PROTOBUF_LIBRARY})

file(GLOB MY_SERVER_SRC worker-server.cpp "rpc_generated/*.cc" "split_file.cpp")
file(GLOB MY_CLIENT_SRC master-client.cpp "rpc_generated/*.cc" "split_file.cpp")
add_executable(workerserver ${MY_SERVER_SRC})
add_executable(masterclient ${MY_CLIENT_SRC})
target_link_libraries(workerserver glog::glog)
target_link_libraries(masterclient glog::glog)


#target_link_libraries(master ${ZOOKEEPER_LIBRARIES} ${CONSERVATOR_LIBRARIES})