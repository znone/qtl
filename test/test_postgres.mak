TARGET=test_postgres async_postgres
CC=g++
PCH_HEADER=stdafx.h
PCH=stdafx.h.gch
OBJ=TestPostgres.o AsyncPostgres.o md5.o
CFLAGS=-g -D_DEBUG -O2 -I/usr/include -I/usr/local/include -I$(shell pg_config --includedir) -I$(shell pg_config --includedir-server )
CXXFLAGS= -I../include -std=c++11
LDFLAGS= -L$(shell pg_config --libdir) -pthread -lcpptest -lpq -lpgtypes

all : $(TARGET)

$(PCH) : $(PCH_HEADER)
	$(CC) $(CFLAGS) $(CXXFLAGS) -x c++-header -o $@ $<

TestPostgres.o : TestPostgres.cpp TestPostgres.h $(PCH)
	$(CC) -c $(CFLAGS) $(CXXFLAGS) -o $@ $< 

AsyncPostgres.o : AsyncPostgres.cpp $(PCH)
	$(CC) -c $(CFLAGS) $(CXXFLAGS) -o $@ $< 
	
md5.o : md5.c md5.h
	gcc -c $(CFLAGS) -o $@ $<

test_postgres : TestPostgres.o md5.o
	libtool --tag=CXX --mode=link $(CC) $(LDFLAGS) -o $@ $^
	
async_postgres : AsyncPostgres.o md5.o
	libtool --tag=CXX --mode=link $(CC) $(LDFLAGS) -o $@ $^

clean:
	rm $(TARGET) $(PCH) $(OBJ) -f
