#include "stdafx.h"
#include "TestOdbc.h"
#include <fstream>
#include <array>
#include "md5.h"

using namespace std;

struct TestOdbcRecord
{
	uint32_t id;
	char name[33];
	qtl::odbc::timestamp create_time;

	TestOdbcRecord()
	{
		memset(this, 0, sizeof(TestOdbcRecord));
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
	inline void bind_record<qtl::odbc::statement, TestOdbcRecord>(qtl::odbc::statement& command, TestOdbcRecord&& v)
	{
		qtl::bind_field(command, 0, v.id);
		qtl::bind_field(command, 1, v.name);
		qtl::bind_field(command, 2, v.create_time);
	}
}

TestOdbc::TestOdbc() : m_db(m_env)
{
	m_db.open("DRIVER={SQL Server};SERVER=(local);UID=;PWD=;Trusted_Connection=yes;DATABASE=test");
	cout<<"DBMS: "<<m_db.dbms_name()<<endl;
	cout<<"SERVER: "<<m_db.server_name()<<endl;
	cout<<"USER: "<<m_db.user_name()<<endl;
	cout<<"DATABASE: "<<m_db.db_name()<<endl;

	id=0;
	TEST_ADD(TestOdbc::test_dual)
	TEST_ADD(TestOdbc::test_clear)
	TEST_ADD(TestOdbc::test_insert)
	TEST_ADD(TestOdbc::test_select)
	TEST_ADD(TestOdbc::test_update)
	TEST_ADD(TestOdbc::test_insert2)
	TEST_ADD(TestOdbc::test_iterator)
	TEST_ADD(TestOdbc::test_insert_blob)
	TEST_ADD(TestOdbc::test_select_blob)
}

void TestOdbc::test_dual()
{
	try
	{
		m_db.query("select 0, 'hello world';",
			[](uint32_t i, const std::string& str) {
				printf("0=\"%d\", 'hello world'=\"%s\"\n",
					i, str.data());
		});
	}
	catch(qtl::odbc::error& e)
	{
		if(!e) ASSERT_EXCEPTION(e);
	}
}

void TestOdbc::test_select()
{
	try
	{
		m_db.query("select * from test where id=?", id, 
			[](const qtl::indicator<uint32_t>& id, const std::string& name, const qtl::odbc::timestamp& create_time) {
				printf("ID=\"%d\", Name=\"%s\"\n",
					id.data, name.data());
		});

		m_db.query("select * from test where id=?", id,
			[](const TestOdbcRecord& record) {
				printf("ID=\"%d\", Name=\"%s\"\n",
					record.id, record.name);
		});

		m_db.query("select * from test where id=?", id, 
			&TestOdbcRecord::print);
	}
	catch(qtl::odbc::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestOdbc::test_insert()
{
	try
	{
		m_db.execute("insert into test(Name, CreateTime) values(?, CURRENT_TIMESTAMP)",
			"test_user");
		m_db.query_first("SELECT @@IDENTITY", id);
	}
	catch(qtl::odbc::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	TEST_ASSERT_MSG(id>0, "insert failture.");
}

void TestOdbc::test_insert2()
{
	try
	{
		uint64_t affected=0;
		qtl::odbc::statement stmt=m_db.open_command("insert into test(Name, CreateTime) values(?, CURRENT_TIMESTAMP)");
		qtl::execute(stmt, &affected, "second_user", "third_user");
		TEST_ASSERT_MSG(affected==2, "Cannot insert 2 records to table test.");
	}
	catch(qtl::odbc::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestOdbc::test_update()
{
	try
	{
		m_db.execute_direct("update test set Name=? WHERE ID=?", NULL,
			"other_user", id);
	}
	catch(qtl::odbc::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
	TEST_ASSERT_MSG(id>0, "insert failture.");
}

void TestOdbc::test_clear()
{
	try
	{
		m_db.simple_execute("delete from test");
	}
	catch(qtl::odbc::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestOdbc::test_iterator()
{
	try
	{
		cout<<"after insert all:"<<endl;
		for(auto& record : m_db.result<TestOdbcRecord>("select ID, Name, CreateTime from test"))
		{
			printf("ID=\"%d\", Name=\"%s\"\n",
				record.id, record.name);
		}
	}
	catch(qtl::odbc::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestOdbc::test_insert_blob()
{
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
		cout<<"MD5 of file "<<filename<<": ";
		print_hex(md5, sizeof(md5));
		cout<<endl;

		m_db.simple_execute("DELETE FROM test_blob");

		fs.clear();
		fs.seekg(0, ios::beg);
		m_db.execute("INSERT INTO test_blob (Filename, [Content], MD5) values(?, ?, ?)",
			make_tuple(filename, ref((istream&)fs), qtl::const_blob_data(md5, sizeof(md5))));
		m_db.query_first("SELECT @@IDENTITY", id);
	}
	catch(qtl::odbc::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestOdbc::test_select_blob()
{
	try
	{
		const char dest_file[]="explorer.exe";
		fstream fs(dest_file, ios::binary|ios::in|ios::out|ios::trunc);
		TEST_ASSERT_MSG(fs, "Cannot open test file.");
		unsigned char md5[16]={0};
		char md5_hex[33]={0};

		fs.seekg(ios::beg);
		std::string source_file;
		m_db.query_first("SELECT Filename, MD5 , [Content]FROM test_blob WHERE id=?", make_tuple(id),
			make_tuple(ref(source_file), qtl::blob_data(md5, sizeof(md5)), ref((ostream&)fs)));
		fs.flush();
		cout<<"MD5 of file "<<source_file<<" stored in database: ";
		print_hex((const unsigned char*)md5, sizeof(md5));
		fs.clear();
		fs.seekg(0, ios::beg);
		get_md5(fs, md5);
		cout<<endl<<"MD5 of file "<<dest_file<<": ";
		print_hex(md5, sizeof(md5));
		cout<<endl;
	}
	catch(qtl::odbc::error& e)
	{
		ASSERT_EXCEPTION(e);
	}
}

void TestOdbc::get_md5(std::istream& is, unsigned char* result)
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

void TestOdbc::print_hex(const unsigned char* data, size_t n)
{
	cout<<hex;
	for(size_t i=0; i!=n; i++)
		cout<<(data[i]&0xFF);
}

int main(int argc, char* argv[])
{
	Test::TextOutput output(Test::TextOutput::Verbose);

	cout<<endl<<"Testing MSSQL(ODBC):"<<endl;

	try
	{
		TestOdbc test_odbc; 
		test_odbc.run(output);
	}
	catch(qtl::odbc::error& e)
	{
		cerr<<e.what()<<endl;
	}
	return 0;
}

