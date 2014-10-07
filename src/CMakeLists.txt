set(CMAKE_C_FLAGS "-std=c99")

add_library (ffindex ffindex.c ffutil.c)
target_include_directories (ffindex PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_library (ffindex_shared SHARED ffindex.c ffutil.c)

add_executable(ffindex_apply
	ffindex_apply.c
)
target_link_libraries (ffindex_apply ffindex)


find_package(MPI QUIET)
IF(MPI_FOUND)
  add_executable(ffindex_apply_mpi
    ffindex_apply_mpi.c
  )
  set_property(TARGET ffindex_apply_mpi PROPERTY COMPILE_FLAGS ${MPI_LINK_FLAGS})
  target_include_directories(ffindex_apply_mpi PUBLIC ${MPI_INCLUDE_PATH})
  target_link_libraries (ffindex_apply_mpi ${MPI_LIBRARIES} ffindex)
	
	INSTALL(PROGRAMS 
  	${CMAKE_CURRENT_BINARY_DIR}/ffindex_apply_mpi
  	DESTINATION bin
	)
ENDIF()


add_executable(ffindex_build
  ffindex_build.c
)
target_link_libraries (ffindex_build ffindex)


add_executable(ffindex_from_fasta
  ffindex_from_fasta.c
)
target_link_libraries (ffindex_from_fasta ffindex)


add_executable(ffindex_get
  ffindex_get.c
)
target_link_libraries (ffindex_get ffindex)


add_executable(ffindex_modify
  ffindex_modify.c
)
target_link_libraries (ffindex_modify ffindex)


add_executable(ffindex_unpack
  ffindex_unpack.c
)
target_link_libraries (ffindex_unpack ffindex)


#add_executable(ffindex_apply_mpi
#  ffindex_apply_mpi.c
#)
#target_link_libraries (ffindex_apply_mpi ffindex)


install(PROGRAMS 
	ffindex.h 
	ffutil.h
	DESTINATION include
)

install(TARGETS ffindex
  LIBRARY DESTINATION lib64 COMPONENT libraries
	ARCHIVE DESTINATION lib64 COMPONENT libraries
)

#ugly workaround
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/libffindex_shared.so
	DESTINATION lib64 RENAME libffindex.so
)

INSTALL(PROGRAMS 
  ${CMAKE_CURRENT_BINARY_DIR}/ffindex_apply
  ${CMAKE_CURRENT_BINARY_DIR}/ffindex_build
  ${CMAKE_CURRENT_BINARY_DIR}/ffindex_from_fasta
  ${CMAKE_CURRENT_BINARY_DIR}/ffindex_get
  ${CMAKE_CURRENT_BINARY_DIR}/ffindex_modify
  ${CMAKE_CURRENT_BINARY_DIR}/ffindex_unpack
  DESTINATION bin
)
