cmake_minimum_required(VERSION 3.0.2)
#SET(CMAKE_CXX_COMPILER "/usr/bin/g++-11")
project(MyIndex)
# set(CMAKE_CXX_FLAGS "-g -O0") # -DENABLE_HUGEPAGE
set(CMAKE_CXX_FLAGS "-O3") # -DENABLE_HUGEPAGE
set(CMAKE_CXX_STANDARD 20)

include_directories(
  ${catkin_INCLUDE_DIRS}
  "./include"
  "/usr/include"
)

link_directories(
  ${catkin_LIB_DIRS}
  "/usr/lib/x86_64-linux-gnu"
)

AUX_SOURCE_DIRECTORY ("src" src)

set(libs "-lpthread -lm -ldl -libverbs -lpthread -lconfig++" )

#add_executable(ser_cli test/ser_cli.cc ${src})
#target_link_libraries(ser_cli PRIVATE ${libs})

add_executable(skiplist test/rdma_skiplist_test.cc ${src})
target_link_libraries(skiplist PRIVATE ${libs})
