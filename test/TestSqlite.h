#ifndef _TEST_SQLITE_H_
#define _TEST_SQLITE_H_

#include "TestSuite.h"

namespace qtl
{
	namespace sqlite
	{
		class database;
	}
}

class TestSqlite : public TestSuite
{
public:
	TestSqlite();

private:
	void test_dual();
	void test_clear();
	void test_insert();
	void test_query();
	void test_update();
	void test_insert2();
	void test_iterator();
	void test_insert_blob();
	void test_select_blob();
	void test_any();

private:
	int64_t id;
	qtl::sqlite::database connect();
	void get_md5(std::string& str, unsigned char* result);
	void copy_stream(std::istream& is, std::ostream& os);
	static void print_hex(const unsigned char* data, size_t n);
};

#endif //_TEST_SQLITE_H_
