cmake_minimum_required (VERSION 3.15)
project (qtl)

set(src ${src})
set(src ${src} include/qtl_odbc.hpp)
 
include_directories(include)
add_executable(TestOdbc test/TestOdbc.cpp ${src})


#//////////////////////////
#link with libraries
#lib_dep contains a cascade definition of all the libraries needed to link
#//////////////////////////

set(lib_dep ${lib_dep})

#//////////////////////////
#ODBC Windows
#//////////////////////////

if (MSVC)
  set(lib_dep ${lib_dep} odbc32.lib odbccp32.lib)
endif()


target_link_libraries (TestOdbc ${lib_dep})

