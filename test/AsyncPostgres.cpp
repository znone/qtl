#include "stdafx.h"
#include "../include/qtl_postgres.hpp"
#include <vector>
#include <thread>
#include <system_error>
#include <time.h>
#include <limits.h>
#include "../include/qtl_postgres_pool.hpp"
#include "../include/qtl_asio.hpp"

using namespace qtl::postgres;

void LogError(const error& e)
{
	fprintf(stderr, "PostgreSQL Error: %s\n", e.what());
}

const char postgres_server[] = "localhost";
const char postgres_user[] = "postgres";
const char postgres_password[] = "111111";
const char postgres_database[] = "test";

qtl::asio::service service;

void SimpleTest()
{
	async_connection connection;
	service.reset();
	std::map<std::string, std::string> params;
	params["host"] = postgres_server;
	params["dbname"] = postgres_database;
	params["user"] = postgres_user;
	params["password"] = postgres_password;
	connection.open(service, [&connection](const error& e) {
		if (e)
		{
			LogError(e);
			service.stop();
		}
		else
		{
			printf("Connect to PostgreSQL ok.\n");
			connection.simple_query("select id, name from test", [](const std::string& id, const std::string& name) {
				printf("%s\t%s\n", id.data(), name.data());
				return true;
			}, [&connection](const error& e, size_t row_count) {
				if (e)
					LogError(e);
				else
					printf("Total %lu rows.\n", row_count);

				connection.close();
			});
		}
	}, params);

	service.run();
}

void insert(async_connection& connection, int next)
{
	qtl::asio::async_execute(connection, [&connection, next](const error& e, uint64_t affected) mutable {
		if (e)
		{
			LogError(e);
			service.stop();
		}
		else
		{
			if (next)
			{
				--next;
				asio::post(service.context(), [&connection, next]() {
					insert(connection, next);
				});
			}
			else
			{
				service.stop();
			}
		}
	}, "insert into test(name, createtime) values($1, LOCALTIMESTAMP)", 0, "test name");
}

void ExecuteTest()
{
	async_connection connection;
	service.reset();
	std::map<std::string, std::string> params;
	params["host"] = postgres_server;
	params["dbname"] = postgres_database;
	params["user"] = postgres_user;
	params["password"] = postgres_password;
	qtl::asio::async_open(service, connection, [&connection](const error& e) {
		if (e)
		{
			LogError(e);
			service.stop();
		}
		else
		{
			printf("Connect to PostgreSQL ok.\n");
			insert(connection, 10);
		}
	}, params);

	service.run();
};

void QueryTest()
{
	async_connection connection;
	service.reset();
	std::map<std::string, std::string> params;
	params["host"] = postgres_server;
	params["dbname"] = postgres_database;
	params["user"] = postgres_user;
	params["password"] = postgres_password;
	connection.open(service, [&connection](const error& e) {
		if (e)
		{
			LogError(e);
			service.stop();
		}
		else
		{
			printf("Connect to PostgreSQL ok.\n");
			connection.query("select id, name, CreateTime from test",
				[](int32_t id, const std::string& name, const qtl::postgres::timestamp& create_time) {
				printf("%d\t%s\t%s\n", id, name.data(), create_time.to_string().data());
			}, [&connection](const error& e) {
				printf("query has completed.\n");
				if (e)
					LogError(e);

				connection.close();
			});
		}
	}, params);

	service.run();
}

int main(int argc, char* argv[])
{
	ExecuteTest();
	SimpleTest();
	QueryTest();
	return 0;
}
