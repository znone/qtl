#include "stdafx.h"
#include "TestPostgres.h"
#include <iostream>
#include <fstream>
#include <array>
#include <iomanip>
#include "md5.h"
#include "../include/qtl_postgres.hpp"

using namespace std;

struct TestpostgresRecord
{
	int32_t id;
	char name[33];
	qtl::postgres::timestamp create_time;

	TestpostgresRecord()
	{
		memset(this, 0, sizeof(TestpostgresRecord));
	}

	void print() const
	{
		printf("ID=\"%d\", Name=\"%s\"\n",
			id, name);
	}
};

namespace qtl
{
	template<>
	inline void bind_record<qtl::postgres::statement, TestpostgresRecord>(qtl::postgres::statement& command, TestpostgresRecord&& v)
	{
		qtl::bind_fields(command, v.id, v.name, v.create_time);
	}
}

TestPostgres::TestPostgres()
{
	this->id = 0;
	TEST_ADD(TestPostgres::test_dual)
		TEST_ADD(TestPostgres::test_clear)
		TEST_ADD(TestPostgres::test_insert)
		TEST_ADD(TestPostgres::test_select)
		TEST_ADD(TestPostgres::test_update)
		TEST_ADD(TestPostgres::test_insert2)
		TEST_ADD(TestPostgres::test_iterator)
		TEST_ADD(TestPostgres::test_any)
}

inline void TestPostgres::connect(qtl::postgres::database& db)
{
	TEST_ASSERT_MSG(db.open("localhost", "postgres", "111111", 5432U, "test") == true, "Cannot connect to database");
}

void TestPostgres::test_dual()
{
	qtl::postgres::database db;
	connect(db);

	try
	{
		db.query("select 0, 'hello world';",
			[](int32_t i, const std::string& str) {
			printf("0=\"%d\", 'hello world'=\"%s\"\n",
				i, str.data());
		});
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestPostgres::test_select()
{
	qtl::postgres::database db;
	connect(db);

	try
	{
		db.query("select * from test where id=$1", 0, id,
			[](const qtl::indicator<int32_t>& id, const std::string& name, const qtl::postgres::timestamp& create_time) {
			printf("ID=\"%d\", Name=\"%s\"\n",
				id.data, name.data());
		});

		db.query("select * from test where id=$1", 0, id,
			[](const TestpostgresRecord& record) {
			printf("ID=\"%d\", Name=\"%s\"\n",
				record.id, record.name);
		});

		db.query("select * from test where id=$1", 0, id,
			&TestpostgresRecord::print);
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestPostgres::test_insert()
{
	qtl::postgres::database db;
	connect(db);

	try
	{
		db.query_first("insert into test(Name, CreateTime) values($1, LOCALTIMESTAMP) returning ID",
			"test_user", id);
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	TEST_ASSERT_MSG(id > 0, "insert failture.");
}

void TestPostgres::test_insert2()
{
	qtl::postgres::database db;
	connect(db);

	try
	{
		uint64_t affected = 0;
		qtl::postgres::statement stmt = db.open_command("insert into test(Name, CreateTime) values($1, LOCALTIMESTAMP)");
		qtl::execute(stmt, &affected, "second_user", "third_user");
		TEST_ASSERT_MSG(affected == 2, "Cannot insert 2 records to table test.");
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestPostgres::test_update()
{
	qtl::postgres::database db;
	connect(db);

	try
	{
		db.execute_direct("update test set Name=$1 WHERE ID=$2", NULL,
			"other_user", id);
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	TEST_ASSERT_MSG(id > 0, "insert failture.");
}

void TestPostgres::test_clear()
{
	qtl::postgres::database db;
	connect(db);

	try
	{
		db.simple_execute("delete from test");
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestPostgres::test_iterator()
{
	qtl::postgres::database db;
	connect(db);

	try
	{
		cout << "after insert all:" << endl;
		for (auto& record : db.result<TestpostgresRecord>("select * from test"))
		{
			printf("ID=\"%d\", Name=\"%s\"\n",
				record.id, record.name);
		}
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestPostgres::test_any()
{
#ifdef _QTL_ENABLE_CPP17

	qtl::postgres::database db;
	connect(db);

	try
	{
		db.query("select 0, 'hello world', LOCALTIMESTAMP",
			[](const std::any& i, const std::any& str, const std::any& now) {
			const qtl::postgres::time& time = std::any_cast<const qtl::postgres::time&>(now);
			struct tm tm;
			time.as_tm(tm);
			cout << "0=\"" << std::any_cast<int32_t>(i) << "\", 'hello world'=\"" <<
				std::any_cast<const std::string&>(str) << "\", now=\"" <<
				std::put_time(&tm, "%c") << "\" \n";
		});
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	catch (std::bad_cast& e)
	{
		ASSERT_EXCEPTION(e);
	}
#endif
}

int main(int argc, char* argv[])
{
	Test::TextOutput output(Test::TextOutput::Verbose);

	cout << endl << "Testing postgres:" << endl;
	TestPostgres test_postgres;
	test_postgres.run(output);
	return 0;
}

