cmake_minimum_required(VERSION 2.8 FATAL_ERROR)
set(CMAKE_LEGACY_CYGWIN_WIN32 0)
SET(HAVE_MPI 1 CACHE BOOL "Have MPI")

project("ffindex")
include_directories(src)
include_directories(src/ext)
add_subdirectory(src)

