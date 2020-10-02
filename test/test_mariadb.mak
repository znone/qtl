TARGET=test_mariadb
CC=g++
PCH_HEADER=stdafx.h
PCH=stdafx.h.gch
OBJ=TestMariaDB.o
CFLAGS=-g -D_DEBUG -O2 -I/usr/include -I/usr/include/mariadb -I/usr/local/include -I/usr/local/include/mariadb 
CXXFLAGS=-I../include -std=c++11
LDFLAGS= -L/usr/local/lib -L/usr/local/mariadb/lib -lmariadb

all : $(TARGET)

$(PCH) : $(PCH_HEADER)
	$(CC) $(CFLAGS) $(CXXFLAGS) -x c++-header -o $@ $<

TestMariaDB.o : TestMariaDB.cpp $(PCH)
	$(CC) -c $(CFLAGS) $(CXXFLAGS) -o $@ $< 
	
$(TARGET) : $(OBJ)
	libtool --tag=CXX --mode=link $(CC) $(LDFLAGS) -o $@ $^

clean:
	rm $(TARGET) $(PCH) $(OBJ) -f
