cmake_minimum_required(VERSION 3.10)

project(KVStoreProject)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(include)

add_executable(kvstore_server 
    src/main.cpp 
    src/KVStore.cpp
    src/Server.cpp
)

if(UNIX)
    target_link_libraries(kvstore_server pthread)
endif()