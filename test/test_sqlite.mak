TARGET=test_sqlite
CC=g++
PCH_HEADER=stdafx.h
PCH=stdafx.h.gch
OBJ=TestSqlite.o sqlite3.o md5.o
CFLAGS=-g -D_DEBUG -O2 -I. -I../include -I/usr/local/include -std=c++11
LDFLAGS= -L/usr/local/lib -ldl -lcpptest -lpthread

all : $(TARGET)

$(PCH) : $(PCH_HEADER)
	$(CC) $(CFLAGS) -x c++-header -o $@ $<

TestSqlite.o : $(PCH) TestSqlite.cpp TestSqlite.h
	$(CC) -c $(CFLAGS) -o $@ TestSqlite.cpp 
	
sqlite3.o : sqlite3.c
	gcc -c -g -O2 -I../include -o $@ $^
	
md5.o : md5.c md5.h
	gcc -c $(CFLAGS) -o $@ $<
	
$(TARGET) : $(OBJ)
	libtool --tag=CXX --mode=link $(CC) $(LDFLAGS) -o $@ $^

clean:
	rm $(TARGET) $(PCH) $(OBJ) -f
