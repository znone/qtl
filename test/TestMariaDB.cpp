#include "stdafx.h"
#include "../include/qtl_mysql.hpp"
#include <vector>
#include <thread>
#include <system_error>
#include <time.h>
#include <limits.h>
#include "../include/qtl_mysql_pool.hpp"
#include "../include/qtl_asio.hpp"

#if MARIADB_VERSION_ID < 0100000
#error "The program need mariadb version > 10.0"
#endif 

using namespace qtl::mysql;

void LogError(const error& e)
{
	fprintf(stderr, "MySQL Error(%d): %s\n", e.code(), e.what());
}

const char mysql_server[]="localhost";
const char mysql_user[]="root";
const char mysql_password[]="123456";
const char mysql_database[]="test";

class MariaDBTest
{
public:
	MariaDBTest()
	{
		Open();
		_service.run();
	}

private:
	void Open();
	void Close();
	void SimpleQuery();
	void Execute();
	void Query();
	void MultiQuery();

private:
	async_connection _connection;
	qtl::asio::service _service;

};

void MariaDBTest::Open()
{
	_connection.open(_service, [this](const error& e) {
		if (e)
		{
			LogError(e);
			_service.stop();
		}
		else
		{
			printf("Connect to mysql ok.\n");
			SimpleQuery();
		}
	}, mysql_server, mysql_user, mysql_password, mysql_database);
}

void MariaDBTest::Close()
{
	_connection.close([this]() {
		printf("Connection is closed.\n");
		_service.stop();
	});
}

void MariaDBTest::SimpleQuery()
{
	_connection.simple_query("select * from test", 0, [](MYSQL_ROW row, int field_count) {
		for (int i = 0; i != field_count; i++)
			printf("%s\t", row[i]);
		printf("\n");
		return true;
	}, [this](const error& e, size_t row_count) {
		if (e)
		{
			LogError(e);
			Close();
		}
		else
		{
			printf("Total %lu rows.\n", row_count);
			Execute();
		}
	});
}

void MariaDBTest::Execute()
{
	_connection.execute([this](const error& e, uint64_t affected) mutable {
		if (e)
		{
			LogError(e);
			Close();
		}
		else
		{
			printf("Insert %llu records ok.\n", affected);
			Query();
		}
	}, "insert into test(name, createtime, company) values(?, now(), ?)", 0, std::make_tuple("test name", "test company"));
}

void MariaDBTest::Query()
{
	_connection.query("select id, name, CreateTime, Company from test", 0, 
		[](int64_t id, const std::string& name, const qtl::mysql::time& create_time, const std::string& company) {
			char szTime[128] = { 0 };
			if (create_time.year != 0)
			{
				struct tm tm;
				create_time.as_tm(tm);
				strftime(szTime, 128, "%c", &tm);
			}
			printf("%lld\t%s\t%s\t%s\n", id, name.data(), szTime, company.data());
	}, [this](const error& e) {
		printf("query has completed.\n");
		if (e)
		{
			LogError(e);
			Close();
		}
		else
		{
			MultiQuery();
		}
	});
}

void MariaDBTest::MultiQuery()
{
	_connection.query_multi("call test_proc",
		[this](const error& e) {
			if (e)
				LogError(e);
			Close();
			
	}, [](uint32_t i, const std::string& str) {
		printf("0=\"%d\", 'hello world'=\"%s\"\n", i, str.data());
	}, [](const qtl::mysql::time& time) {
		struct tm tm;
		time.as_tm(tm);
		printf("current time is: %s\n", asctime(&tm));
	});
}

int main(int argc, char* argv[])
{
	MariaDBTest test;
	return 0;
}
