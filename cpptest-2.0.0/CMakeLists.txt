cmake_minimum_required (VERSION 3.15)
project (cpptest)

if (MSVC)
  include_directories(win)
endif()

set(cpptest_src ${cpptest_src})
set(cpptest_src ${cpptest_src} src/source.cpp src/suite.cpp src/time.cpp src/utils.cpp src/missing.cpp src/textoutput.cpp)

add_library(cpptest STATIC ${cpptest_src} )

