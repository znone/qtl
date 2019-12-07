#include "stdafx.h"
#include <time.h>
#include <fstream>
#include <sstream>
#include <array>
#include "md5.h"
#include "TestSqlite.h"
#include "../include/qtl_sqlite.hpp"

using namespace std;

struct TestSqliteRecord
{
	int id;
	char name[33];
	int64_t create_time;

	TestSqliteRecord()
	{
		memset(this, 0, sizeof(TestSqliteRecord));
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
	inline void bind_record<qtl::sqlite::statement, TestSqliteRecord>(qtl::sqlite::statement& command, TestSqliteRecord&& v)
	{
		qtl::bind_fields(command, v.id, v.name, v.create_time);
	}
}

TestSqlite::TestSqlite()
{
	id=0;
	TEST_ADD(TestSqlite::test_dual)
	TEST_ADD(TestSqlite::test_clear)
	TEST_ADD(TestSqlite::test_insert)
	TEST_ADD(TestSqlite::test_query)
	TEST_ADD(TestSqlite::test_update)
	TEST_ADD(TestSqlite::test_insert2)
	TEST_ADD(TestSqlite::test_iterator)
	TEST_ADD(TestSqlite::test_insert_blob)
	TEST_ADD(TestSqlite::test_select_blob)
	TEST_ADD(TestSqlite::test_any)
}

inline qtl::sqlite::database TestSqlite::connect()
{
	qtl::sqlite::database db;
	try
	{
		db.open("test.db");
	}
	catch (qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	return db;
}

void TestSqlite::test_dual()
{
	qtl::sqlite::database db = connect();

	try
	{
		db.query("select 0, 'hello world'", 
			[](int32_t i, const std::string& str) {
				printf("0=\"%d\", 'hello world'=\"%s\"\n",
					i, str.data());
		});
	}
	catch(qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestSqlite::test_clear()
{
	qtl::sqlite::database db = connect();

	try
	{
		db.simple_execute("delete from test");
	}
	catch(qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestSqlite::test_insert()
{
	qtl::sqlite::database db = connect();

	try
	{
		id=db.insert("insert into test(Name, CreateTime) values(?, datetime('now'))",
			"test_user");
	}
	catch(qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	TEST_ASSERT_MSG(id>0, "insert failture.");
}

void TestSqlite::test_insert2()
{
	qtl::sqlite::database db = connect();

	try
	{
		uint64_t affected=0;
		qtl::sqlite::statement stmt=db.open_command("insert into test(Name, CreateTime) values(?, datetime('now'))");
		qtl::execute(stmt, &affected, "second_user", "third_user");
		TEST_ASSERT_MSG(affected==2, "Cannot insert 2 records to table test.");
	}
	catch(qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestSqlite::test_update()
{
	qtl::sqlite::database db = connect();

	try
	{
		db.execute_direct("update test set Name=? WHERE ID=?", NULL,
			"other_user", id);
	}
	catch(qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	TEST_ASSERT_MSG(id>0, "insert failture.");
}

void TestSqlite::test_query()
{
	qtl::sqlite::database db = connect();

	try
	{
		db.query("select * from test where id=?", id, 
			[](int id, const std::string& name, const std::string& create_time) {
				printf("ID=\"%d\", Name=\"%s\", CrateTime=\"%s\"\n",
					id, name.data(), create_time.data());
		});

		db.query("select ID, Name, strftime('%s', CreateTime) from test where id=?", id, 
			[](const TestSqliteRecord& record) {
				time_t t=record.create_time;
				printf("ID=\"%d\", Name=\"%s\", CrateTime=\"%s\"\n",
					record.id, record.name, asctime(localtime(&t)));
		});

		db.query("select * from test where id=?", id,
			&TestSqliteRecord::print);
	}
	catch(qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestSqlite::test_iterator()
{
	qtl::sqlite::database db = connect();

	try
	{
		cout<<"after insert all:"<<endl;
		for(auto& record : db.result<TestSqliteRecord>("select ID, Name, strftime('%s', CreateTime) from test"))
		{
			printf("ID=\"%d\", Name=\"%s\"\n",
				record.id, record.name);
		}
	}
	catch(qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestSqlite::test_insert_blob()
{
	qtl::sqlite::database db = connect();

	try
	{
#ifdef _WIN32
		const char filename[]="C:\\windows\\explorer.exe";
#else
		const char filename[]="/bin/sh";
#endif //_WIN32
		fstream fs(filename, ios::binary|ios::in);
		stringstream ss(ios::binary|ios::out);
		TEST_ASSERT_MSG(fs, "Cannot open test file.");
		unsigned char md5[16]={0};
		copy_stream(fs, ss);

		db.simple_execute("DELETE FROM test_blob");

		string str=ss.str();
		get_md5(str, md5);
		cout<<"MD5 of file "<<filename<<": ";
		print_hex(md5, sizeof(md5));
		cout<<endl;
		id=db.insert("INSERT INTO test_blob (Filename, Content, MD5) values(?, ?, ?)",
			forward_as_tuple(filename, qtl::const_blob_data(str.data(), str.size()), qtl::const_blob_data(md5, sizeof(md5))));
	}
	catch(qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestSqlite::test_select_blob()
{
	qtl::sqlite::database db = connect();

	try
	{
		const char dest_file[]="explorer.exe";
		fstream fs(dest_file, ios::binary|ios::in|ios::out|ios::trunc);
		std::string source_file;
		unsigned char md5[16]={0};
		TEST_ASSERT_MSG(fs, "Cannot open test file.");

		fs.seekg(ios::beg);
		db.query_first("SELECT Filename, Content, MD5 FROM test_blob WHERE id=?", make_tuple(id),
			forward_as_tuple(source_file, fs, qtl::blob_data(md5, 16)));
		fs.flush();
		cout<<"MD5 of file "<<source_file<<" stored in database: ";
		print_hex((const unsigned char*)md5, sizeof(md5));
		cout<<endl;
	}
	catch(qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestSqlite::test_any()
{
#ifdef _QTL_ENABLE_CPP17
	qtl::sqlite::database db = connect();

	try
	{
		db.query("select 0, 'hello world'",
			[](const std::any& i, const std::any& str) {
				cout << "0=\"" << std::any_cast<int64_t>(i) << "\", 'hello world'=\"" <<
					std::any_cast<const std::string_view&>(str).data() << "\" \n";
		});
	}
	catch (qtl::sqlite::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	catch (std::bad_cast& e)
	{
		ASSERT_EXCEPTION(e);
	}
#endif // C++17
}

void TestSqlite::get_md5(std::string& str, unsigned char* result)
{
	MD5_CTX context;
	MD5Init(&context);
	MD5Update(&context, (unsigned char*)str.data(), (unsigned int)str.size());
	MD5Final(result, &context);
}

void TestSqlite::copy_stream(istream& is, ostream& os)
{
	array<char, qtl::blob_buffer_size> buffer;
	while(!is.eof())
	{
		is.read(buffer.data(), buffer.size());
		os.write(buffer.data(), is.gcount());
	}
}

void TestSqlite::print_hex(const unsigned char* data, size_t n)
{
	cout<<hex;
	for(size_t i=0; i!=n; i++)
		cout<<(data[i]&0xFF);
}

int main(int argc, char* argv[])
{
	Test::TextOutput output(Test::TextOutput::Verbose);

	cout<<endl<<"Testing Sqlite:"<<endl;
	TestSqlite test_sqlite; 
	test_sqlite.run(output);
	return 0;
}
