TARGET=test_odbc
CC=g++
PCH_HEADER=stdafx.h
PCH=stdafx.h.gch
OBJ=TestOdbc.o md5.o
CFLAGS=-g -D_DEBUG -O2 -I/usr/local/include
CXXFLAGS=-I../include -std=c++11
LDFLAGS= -L/usr/local/lib -lcpptest -lodbc

all : $(TARGET)

$(PCH) : $(PCH_HEADER)
	$(CC) $(CFLAGS) $(CXXFLAGS) -x c++-header -o $@ $<

TestOdbc.o : TestOdbc.cpp TestOdbc.h $(PCH)
	$(CC) -c $(CFLAGS) $(CXXFLAGS) -o $@ $< 
	
md5.o : md5.c md5.h
	gcc -c $(CFLAGS) -o $@ $<

$(TARGET) : $(OBJ)
	libtool --tag=CXX --mode=link $(CC) $(LDFLAGS) -o $@ $^

clean:
	rm $(TARGET) $(PCH) $(OBJ) -f
