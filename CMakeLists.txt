cmake_minimum_required (VERSION 3.15)
project (qtl)

set(src ${src})
set(src ${src} include/qtl_odbc.hpp)

add_subdirectory(cpptest-2.0.0)

include_directories(cpptest-2.0.0/src)
 
include_directories(include)
add_executable(TestOdbc test/TestOdbc.cpp test/TestOdbc.h ${src} test/md5.c test/md5.h)

add_executable(example_1 example/example_1.cpp ${src})


#//////////////////////////
#link with libraries
#lib_dep contains a cascade definition of all the libraries needed to link
#//////////////////////////

set(lib_dep ${lib_dep})
set(lib_dep ${lib_dep} cpptest)

#//////////////////////////
#ODBC Windows
#//////////////////////////

if (MSVC)
  set(lib_dep ${lib_dep} odbc32.lib odbccp32.lib)
endif()


target_link_libraries (TestOdbc ${lib_dep})
target_link_libraries (example_1 ${lib_dep})

