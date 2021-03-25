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
	TEST_ADD(TestPostgres::test_insert_blob)
	TEST_ADD(TestPostgres::test_select_blob)
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
	catch (std::exception& e)
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
			[](const qtl::indicator<int32_t>& id, const std::string& name, const qtl::postgres::timestamp& create_time, const std::vector<int32_t>& v, const std::tuple<int, int>& pt) {
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
	catch (std::exception& e)
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
		int32_t va[] = { 200, 300, 400 };
		db.query_first("insert into test(Name, CreateTime, va, percent) values($1, LOCALTIMESTAMP, $2, $3) returning ID",
			std::forward_as_tuple("test_user", va, std::make_pair(11, 22)), id);
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	catch (std::exception& e)
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
	catch (std::exception& e)
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
	catch (std::exception& e)
	{
		ASSERT_EXCEPTION(e);
	}
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
	catch (std::exception& e)
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
	catch (std::exception& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void hex_string(char* dest, const unsigned char* bytes, size_t count)
{
	for (size_t i = 0; i != count; i++)
	{
		sprintf(&dest[i * 2], "%02X", bytes[i] & 0xFF);
	}
}

void TestPostgres::test_insert_blob()
{
	qtl::postgres::database db;
	connect(db);

	try
	{
#ifdef _WIN32
		const char filename[] = "C:\\windows\\explorer.exe";
#else
		const char filename[] = "/bin/sh";
#endif //_WIN32
		qtl::postgres::transaction trans(db);
		qtl::postgres::large_object content = qtl::postgres::large_object::load(db.handle(), filename);
		TEST_ASSERT_MSG(content.oid()>0, "Cannot open test file.");
		fstream fs(filename, ios::binary | ios::in);
		unsigned char md5[16] = { 0 };
		char md5_hex[33] = { 0 };
		get_md5(fs, md5);
		hex_string(md5_hex, md5, sizeof(md5));
		printf("MD5 of file %s: %s.\n", filename, md5_hex);

		db.simple_execute("DELETE FROM test_blob");

		fs.clear();
		fs.seekg(0, ios::beg);
		db.query_first("INSERT INTO test_blob (filename, content, md5) values($1, $2, $3) returning id",
			forward_as_tuple(filename, content, qtl::const_blob_data(md5, sizeof(md5))), id);
		content.close();
		trans.commit();
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	catch (std::bad_cast& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestPostgres::test_select_blob()
{
	qtl::postgres::database db;
	connect(db);

	try
	{
		const char dest_file[] = "explorer.exe";

		unsigned char md5[16] = { 0 };
		std::string source_file;
		qtl::postgres::transaction trans(db);
		qtl::postgres::large_object content;
		qtl::const_blob_data md5_value;
		db.query_first("SELECT filename, content, md5 FROM test_blob WHERE id=$1", make_tuple(id),
			forward_as_tuple(source_file, content, qtl::blob_data(md5, sizeof(md5))));
		if (content.is_open())
		{
			content.save(dest_file);
			ifstream fs(dest_file, ios::binary | ios::in);
			TEST_ASSERT_MSG(fs, "Cannot open test file.");
			char md5_hex[33] = { 0 };
			hex_string(md5_hex, md5, sizeof(md5));
			printf("MD5 of file %s stored in database: %s.\n", source_file.data(), md5_hex);
			fs.clear();
			fs.seekg(0, ios::beg);
			get_md5(fs, md5);
			hex_string(md5_hex, md5, sizeof(md5));
			printf("MD5 of file %s: %s.\n", dest_file, md5_hex);
			content.close();
		}
	}
	catch (qtl::postgres::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	catch (std::bad_cast& e)
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

void TestPostgres::get_md5(std::istream& is, unsigned char* result)
{
	std::array<char, 64 * 1024> buffer;
	MD5_CTX context;
	MD5Init(&context);
	while (!is.eof())
	{
		is.read(&buffer.front(), buffer.size());
		MD5Update(&context, (unsigned char*)buffer.data(), (unsigned int)is.gcount());
	}
	MD5Final(result, &context);
}

int main(int argc, char* argv[])
{
	Test::TextOutput output(Test::TextOutput::Verbose);

	cout << endl << "Testing postgres:" << endl;
	TestPostgres test_postgres;
	test_postgres.run(output);
	return 0;
}

