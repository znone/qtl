#ifndef _TEST_MYSQL_H_
#define _TEST_MYSQL_H_

#include "TestSuite.h"

namespace qtl
{
	namespace postgres
	{
		class database;
	}
}

class TestPostgres : public TestSuite
{
public:
	TestPostgres();

private:
	void test_dual();
	void test_clear();
	void test_insert();
	void test_select();
	void test_update();
	void test_insert2();
	void test_iterator();
	void test_insert_blob();
	void test_select_blob();
	void test_any();

private:
	int32_t id;
	void connect(qtl::postgres::database& db);
	void get_md5(std::istream& is, unsigned char* result);
};

#endif //_TEST_MYSQL_H_