TARGET=test_mysql
CC=g++
PCH_HEADER=stdafx.h
PCH=stdafx.h.gch
OBJ=TestMysql.o md5.o
CFLAGS=-g -D_DEBUG -O2 -I../include -I/usr/include -I/usr/local/include $(shell mysql_config --cflags) 
CXXFLAGS=-std=c++11
LDFLAGS= -L/usr/local/lib -lcpptest $(shell mysql_config --libs)

all : $(TARGET)

$(PCH) : $(PCH_HEADER)
	$(CC) $(CFLAGS) $(CXXFLAGS) -x c++-header -o $@ $<

TestMysql.o : TestMysql.cpp TestMysql.h $(PCH)
	$(CC) -c $(CFLAGS) $(CXXFLAGS) -o $@ $< 
	
md5.o : md5.c md5.h
	gcc -c $(CFLAGS) -o $@ $<

$(TARGET) : $(OBJ)
	libtool --tag=CXX --mode=link $(CC) $(LDFLAGS) -o $@ $^

clean:
	rm $(TARGET) $(PCH) $(OBJ) -f
