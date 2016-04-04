cmake_minimum_required(VERSION 3.3)
project(assign2)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    buffer_mgr.h
    buffer_mgr_stat.c
    buffer_mgr_stat.h
    dberror.c
    dberror.h
    dt.h
    storage_mgr.h
    test_assign2_1.c
    test_helper.h record_mgr.old.c.old aux.c record_mgr.c)

add_executable(assign2 ${SOURCE_FILES})