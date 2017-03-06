#ifndef _TEST_ODBC_H_
#define _TEST_ODBC_H_

#include "TestSuite.h"
#include "../include/qtl_odbc.hpp"

namespace qtl
{
	namespace odbc
	{
		class database;
	}
}

class TestOdbc : public TestSuite
{
public:
	TestOdbc();

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

private:
	uint64_t id;
	qtl::odbc::environment m_env;
	qtl::odbc::database m_db;
	void get_md5(std::istream& is, unsigned char* result);
	static void print_hex(const unsigned char* data, size_t n);
};

#endif //_TEST_ODBC_H_
