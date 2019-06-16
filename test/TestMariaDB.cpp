#include "stdafx.h"
#include "../include/qtl_mysql.hpp"
#include <vector>
#include <thread>
#include <system_error>
#include <time.h>
#include <limits.h>
#include "../include/qtl_mysql_pool.hpp"

#if MARIADB_VERSION_ID < 0100000
#error "The program need mariadb version > 10.0"
#endif 

class simple_event_loop
{
public:
	simple_event_loop()
	{
		m_expired = 0;
		m_maxfd = 0;
		m_stoped = false;
		FD_ZERO(&m_readset);
		FD_ZERO(&m_writeset);
		FD_ZERO(&m_exceptset);
	}

	void reset()
	{
		m_stoped = false;
	}

	void run()
	{
		while (!m_stoped)
			do_once(nullptr);
	}

	void run_for(long seconds)
	{
		time_t expired, now;
		time(&now);
		expired = now + seconds;
		timeval tv = { seconds };
		do
		{
			do_once(&tv);
			time(&now);
			tv.tv_sec = expired - now;
			tv.tv_usec = 0;
		} while (!m_stoped && now < expired);
	}

	void stop()
	{
		m_stoped = true;
	}

private:
	class event_item : public qtl::event
	{
	public:
		event_item(simple_event_loop* loop, qtl::socket_type fd)
			: m_loop(loop), m_fd(fd), m_expired(LONG_MAX)
		{
		}

		virtual void set_io_handler(int flags, long timeout, std::function<void(int)>&& handler) NOEXCEPT override
		{
			if (timeout > 0)
			{
				time_t now;
				time(&now);
				if (m_expired > now + timeout)
					m_expired = now + timeout;
				m_loop->set(this, flags, m_expired);
			}
			else
			{
				m_expired = LONG_MAX;
				m_loop->set(this, flags, 0);
			}
			m_io_handler = std::forward<std::function<void(int)>>(handler);
		}
		virtual void remove() override
		{
			m_loop->remove(this);
		}
		virtual bool is_busying() override
		{
			return m_io_handler!=nullptr;
		}

		simple_event_loop* m_loop;
		qtl::socket_type m_fd;
		time_t m_expired;
		std::function<void(int)> m_io_handler;
	};
	std::vector<std::unique_ptr<event_item>> m_events;

	//implement for QTL
public:
	template<typename Connection>
	event_item* add(Connection* connection)
	{
		qtl::socket_type fd = connection->socket();
		event_item* result = new event_item(this, fd);
		std::unique_ptr<event_item> item(result);
		m_events.emplace_back(std::move(item));
		return result;
	}

	template<typename Handler>
	event_item* set_timeout(const timeval& timeout, Handler&& handler)
	{
		event_item* result = new event_item(this, 0);
		std::unique_ptr<event_item> item(result);
		::time(&result->m_expired);
		result->m_expired+=timeout.tv_sec;
		if(m_expired>result->m_expired)
			m_expired=result->m_expired;
		m_events.emplace_back(std::move(item));
		return result;
	}

public:
	void set(event_item* item, int flags, time_t expired)
	{
		if (item->m_fd + 1 > m_maxfd)
			m_maxfd = item->m_fd + 1;
		if (flags&qtl::event::ef_read)
			FD_SET(item->m_fd, &m_readset);
		if (flags&qtl::event::ef_write)
			FD_SET(item->m_fd, &m_writeset);
		if (flags&qtl::event::ef_exception)
			FD_SET(item->m_fd, &m_exceptset);
		if (expired > 0 && m_expired > expired)
			m_expired = expired;
	}

	void remove(event_item* item)
	{
		auto it = m_events.begin();
		while (it != m_events.end())
		{
			if (it->get() == item)
			{
				m_events.erase(it);
				return;
			}
			++it;
		}
	}

private:
	fd_set m_readset, m_writeset, m_exceptset;
	qtl::socket_type m_maxfd;
	int do_once(timeval* timeout)
	{
		fd_set rs = m_readset, ws = m_writeset, es = m_exceptset;
		timeval tv = { INT_MAX, 0 };
		time_t now;
		time(&now);
		if (timeout)
		{
			tv = *timeout;
			timeout = &tv;
		}

		long d = m_expired - now;
		if (d > 0 && tv.tv_sec > d)
		{
			tv.tv_sec = d;
			timeout = &tv;
		}

		qtl::socket_type maxfd = m_maxfd;
		FD_ZERO(&m_readset);
		FD_ZERO(&m_writeset);
		FD_ZERO(&m_exceptset);
		m_maxfd = 0;

		if (maxfd == 0)
		{
			if (timeout)
			{
				std::this_thread::sleep_for(std::chrono::microseconds(timeout->tv_sec*std::micro::den + timeout->tv_usec));
				check_timeout();
			}
			else
			{
				std::this_thread::sleep_for(std::chrono::seconds(1));
			}
			return 0;
		}
		else
		{
			int ret = select(maxfd, &rs, &ws, &es, timeout);
			if (ret > 0)
			{
				for (auto& item : m_events)
				{
					int flags = 0;
					if (FD_ISSET(item->m_fd, &rs))
						flags |= qtl::event::ef_read;
					if (FD_ISSET(item->m_fd, &ws))
						flags |= qtl::event::ef_write;
					if (FD_ISSET(item->m_fd, &es))
						flags |= qtl::event::ef_exception;
					if (flags && item->m_io_handler)
					{
						auto handler = std::move(item->m_io_handler);
						handler(flags);
					}
				}
			}
			else if (ret == 0)
			{
				time(&now);
				for (auto& item : m_events)
				{
					if (item->m_expired > 0 && item->m_io_handler && item->m_expired < now)
					{
						item->m_io_handler(qtl::event::ef_timeout);
						item->m_expired = 0;
						item->m_io_handler = nullptr;
					}
				}
			}
			else if (ret < 0)
			{
				int errc;
#ifdef _WIN32
				errc = WSAGetLastError();
#else
				errc = errno;
#endif
				throw std::system_error(errc, std::system_category());
			}
			return ret;
		}
	}

	void check_timeout()
	{
		time_t now;
		time(&now);
		for (auto& item : m_events)
		{
			if (item->m_expired > 0 && item->m_io_handler && item->m_expired < now)
			{
				item->m_io_handler(qtl::event::ef_timeout);
				item->m_expired = 0;
				item->m_io_handler = nullptr;
			}
		}
	}

	time_t m_expired;
	bool m_stoped;
};

simple_event_loop ev;

using namespace qtl::mysql;

void LogError(const error& e)
{
	fprintf(stderr, "MySQL Error(%d): %s\n", e.code(), e.what());
}

const char mysql_server[]="localhost";
const char mysql_user[]="root";
const char mysql_password[]="";
const char mysql_database[]="test";

void SimpleTest()
{
	async_connection connection;
	ev.reset();
	connection.open(ev, [&connection](const error& e) {
		if (e)
		{
			LogError(e);
			ev.stop();
		}
		else
		{
			printf("Connect to mysql ok.\n");
			connection.simple_query("select * from test", 0, [](MYSQL_ROW row, int field_count) {
				for (int i = 0; i != field_count; i++)
					printf("%s\t", row[i]);
				printf("\n");
				return true;
			}, [&connection](const error& e, size_t row_count) {
				if (e)
					LogError(e);
				else
					printf("Total %lu rows.\n", row_count);

				connection.close([]() {
					printf("Close connection ok.\n");
					ev.stop();
				});
			});
		}
	}, mysql_server, mysql_user, mysql_password, mysql_database);

	ev.run();
}

void ExecuteTest()
{
	async_connection connection;
	ev.reset();
	connection.open(ev, [&connection](const error& e) {
		if (e)
		{
			LogError(e);
			ev.stop();
		}
		else
		{
			printf("Connect to mysql ok.\n");
			connection.execute([&connection](const error& e, uint64_t affected) mutable {
					if (e)
						LogError(e);
					else
						printf("Insert %llu records ok.\n", affected);
					connection.close([]() {
						printf("Close connection ok.\n");
						ev.stop();
					});
			}, "insert into test(name, createtime, company) values(?, now(), ?)", 0, std::make_tuple("test name", "test company"));
		}
	}, mysql_server, mysql_user, mysql_password, mysql_database);

	ev.run();
};

void QueryTest()
{
	async_connection connection;
	ev.reset();
	connection.open(ev, [&connection](const error& e) {
		if (e)
		{
			LogError(e);
			ev.stop();
		}
		else
		{
			printf("Connect to mysql ok.\n");
			connection.query("select id, name, CreateTime, Company from test", 0, 
				[](int64_t id, const std::string& name, const qtl::mysql::time& create_time, const std::string& company) {
				char szTime[128] = { 0 };
				if (create_time.year != 0)
				{
					struct tm tm;
					create_time.as_tm(tm);
					strftime(szTime, 128, "%c", &tm);
				}
				printf("%lld\t%s\t%s\t%s\n", id, name.data(), szTime, company.data());
			}, [&connection](const error& e) {
				printf("query has completed.\n");
				if (e)
					LogError(e);

				connection.close([]() {
					printf("Close connection ok.\n");
					ev.stop();
				});
			});
		}
	}, mysql_server, mysql_user, mysql_password, mysql_database);

	ev.run();
}

void MultiQueryTest()
{
	async_connection connection;
	ev.reset();
	connection.open(ev, [&connection](const error& e) {
		if (e)
		{
			LogError(e);
			ev.stop();
		}
		else
		{
			printf("Connect to mysql ok.\n");
			connection.query_multi("call test_proc",
				[&connection](const error& e) {
				if (e)
					LogError(e);

				connection.close([]() {
					printf("Close connection ok.\n");
					ev.stop();
				});
			}, [](uint32_t i, const std::string& str) {
				printf("0=\"%d\", 'hello world'=\"%s\"\n", i, str.data());
			}, [](const qtl::mysql::time& time) {
				struct tm tm;
				time.as_tm(tm);
				printf("current time is: %s\n", asctime(&tm));
			});
		}
	}, mysql_server, mysql_user, mysql_password, mysql_database);

	ev.run();
}

int main(int argc, char* argv[])
{
	ExecuteTest();
	SimpleTest();
	QueryTest();
	MultiQueryTest();
	return 0;
}
