cmake_minimum_required(VERSION 2.8.12 FATAL_ERROR)
set(CMAKE_LEGACY_CYGWIN_WIN32 0)

project("ffindex")
include_directories(src)
include_directories(src/ext)
add_subdirectory(src)

