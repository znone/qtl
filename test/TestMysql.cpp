#include "stdafx.h"
#include "TestMysql.h"
#include <fstream>
#include <array>
#include <iomanip>
#include "md5.h"
#include "../include/qtl_mysql.hpp"

using namespace std;

struct TestMysqlRecord
{
	uint32_t id;
	char name[33];
	qtl::mysql::time create_time;

	TestMysqlRecord()
	{
		memset(this, 0, sizeof(TestMysqlRecord));
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
	inline void bind_record<qtl::mysql::statement, TestMysqlRecord>(qtl::mysql::statement& command, TestMysqlRecord&& v)
	{
		qtl::bind_fields(command, v.id, v.name, v.create_time);
	}
}

TestMysql::TestMysql()
{
	id=0;
	TEST_ADD(TestMysql::test_dual)
	TEST_ADD(TestMysql::test_clear)
	TEST_ADD(TestMysql::test_insert)
	TEST_ADD(TestMysql::test_select)
	TEST_ADD(TestMysql::test_update)
	TEST_ADD(TestMysql::test_insert2)
	TEST_ADD(TestMysql::test_iterator)
	TEST_ADD(TestMysql::test_insert_blob)
	TEST_ADD(TestMysql::test_select_blob)
	TEST_ADD(TestMysql::test_any)
		//TEST_ADD(TestMysql::test_insert_stream)
	//TEST_ADD(TestMysql::test_fetch_stream)
}

inline void TestMysql::connect(qtl::mysql::database& db)
{
	TEST_ASSERT_MSG(db.open("localhost", "root", "", "test")==true, "Cannot connect to database");
}

void TestMysql::test_dual()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
		db.query("select 0, 'hello world' from dual",
			[](uint32_t i, const std::string& str) {
				printf("0=\"%d\", 'hello world'=\"%s\"\n",
					i, str.data());
		});
	}
	catch(qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestMysql::test_select()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
		db.query("select * from test where id=?", 0, id, 
			[](const qtl::indicator<uint32_t>& id, const std::string& name, const qtl::mysql::time& create_time) {
				printf("ID=\"%d\", Name=\"%s\"\n",
					id.data, name.data());
		});

		db.query("select * from test where id=?", 0, id,
			[](const TestMysqlRecord& record) {
				printf("ID=\"%d\", Name=\"%s\"\n",
					record.id, record.name);
		});

		db.query("select * from test where id=?", 0, id, 
			&TestMysqlRecord::print);
	}
	catch(qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestMysql::test_insert()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
		id=db.insert("insert into test(Name, CreateTime) values(?, now())",
			"test_user");
	}
	catch(qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	TEST_ASSERT_MSG(id>0, "insert failture.");
}

void TestMysql::test_insert2()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
		uint64_t affected=0;
		qtl::mysql::statement stmt=db.open_command("insert into test(Name, CreateTime) values(?, now())");
		qtl::execute(stmt, &affected, "second_user", "third_user");
		TEST_ASSERT_MSG(affected==2, "Cannot insert 2 records to table test.");
	}
	catch(qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestMysql::test_update()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
		db.execute_direct("update test set Name=? WHERE ID=?", NULL,
			"other_user", id);
	}
	catch(qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	TEST_ASSERT_MSG(id>0, "insert failture.");
}

void TestMysql::test_clear()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
		db.simple_execute("delete from test");
	}
	catch(qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestMysql::test_iterator()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
		cout<<"after insert all:"<<endl;
		for(auto& record : db.result<TestMysqlRecord>("select * from test"))
		{
			printf("ID=\"%d\", Name=\"%s\"\n",
				record.id, record.name);
		}
	}
	catch(qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestMysql::test_insert_blob()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
#ifdef _WIN32
		const char filename[]="C:\\windows\\explorer.exe";
#else
		const char filename[]="/bin/sh";
#endif //_WIN32
		fstream fs(filename, ios::binary|ios::in);
		TEST_ASSERT_MSG(fs, "Cannot open test file.");
		unsigned char md5[16]={0};
		char md5_hex[33]={0};
		get_md5(fs, md5);
		mysql_hex_string(md5_hex, (char*)md5, sizeof(md5));
		printf("MD5 of file %s: %s.\n", filename, md5_hex);

		db.simple_execute("DELETE FROM test_blob");

		fs.clear();
		fs.seekg(0, ios::beg);
		id=db.insert("INSERT INTO test_blob (Filename, Content, MD5) values(?, ?, ?)",
			forward_as_tuple(filename, (istream&)fs, qtl::const_blob_data(md5, sizeof(md5))));
	}
	catch(qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestMysql::test_select_blob()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
		const char dest_file[]="explorer.exe";
		fstream fs(dest_file, ios::binary|ios::in|ios::out|ios::trunc);
		TEST_ASSERT_MSG(fs, "Cannot open test file.");
		unsigned char md5[16]={0};
		char md5_hex[33]={0};

		fs.seekg(ios::beg);
		std::string source_file;
		db.query_first("SELECT Filename, Content, MD5 FROM test_blob WHERE id=?", make_tuple(id),
			forward_as_tuple(source_file, (ostream&)fs, qtl::blob_data(md5, sizeof(md5))));
		fs.flush();
		mysql_hex_string(md5_hex, (char*)md5, sizeof(md5));
		printf("MD5 of file %s stored in database: %s.\n", source_file.data(), md5_hex);
		fs.clear();
		fs.seekg(0, ios::beg);
		get_md5(fs, md5);
		mysql_hex_string(md5_hex, (char*)md5, sizeof(md5));
		printf("MD5 of file %s: %s.\n", dest_file, md5_hex);
	}
	catch(qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestMysql::test_insert_stream()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
		qtl::blob_writer writer = [](std::ostream& s) {
			for (size_t i = 0; i != 100; i++)
			{
				s << i << ": ";
				for (size_t j = 0; j <= i; j++)
					s << char('a' + j % 26);
				s << endl;
				for (size_t j = 0; j <= i; j++)
					s << '-';
				s << endl;
			}
		};
		id = db.insert("INSERT INTO test_stream (Data) values(?)",
				writer);
	}
	catch (qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestMysql::test_fetch_stream()
{
	qtl::mysql::database db;
	connect(db);

	try
	{
		db.query("SELECT Data from test_stream", [](qtl::mysql::blobbuf&& buf) {
			istream s(&buf);
			string str;
			while (!s.eof())
			{
				getline(s, str);
				cout << str << endl;
			}
			s.clear(ios_base::goodbit | ios_base::eofbit);
			s.seekg(0, ios::beg);
			if (s.good())
			{
				cout << "again:" << endl;
				while (!s.eof())
				{
					getline(s, str);
					cout << str << endl;
				}
			}
		});
	}
	catch (qtl::mysql::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestMysql::get_md5(std::istream& is, unsigned char* result)
{
	std::array<char, 64*1024> buffer;
	MD5_CTX context;
	MD5Init(&context);
	while(!is.eof())
	{
		is.read(&buffer.front(), buffer.size());
		MD5Update(&context, (unsigned char*)buffer.data(), (unsigned int)is.gcount());
	}
	MD5Final(result, &context);
}

void TestMysql::test_any()
{
#ifdef _QTL_ENABLE_CPP17

	qtl::mysql::database db;
	connect(db);

	try
	{
		db.query("select 0, 'hello world', now() from dual",
			[](const std::any& i, const std::any& str, const std::any& now) {
				const qtl::mysql::time& time = std::any_cast<const qtl::mysql::time&>(now);
				struct tm tm;
				time.as_tm(tm);
				cout << "0=\"" << std::any_cast<int32_t>(i) << "\", 'hello world'=\"" <<
					std::any_cast<const std::string&>(str) << "\", now=\"" <<
					std::put_time(&tm, "%c") << "\" \n";
		});
	}
	catch (qtl::mysql::error& e)
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

	cout<<endl<<"Testing MYSQL:"<<endl;
	TestMysql test_mysql; 
	test_mysql.run(output);
	return 0;
}

