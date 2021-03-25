#ifndef _QTL_MYSQL_POOL_H_
#define _QTL_MYSQL_POOL_H_

#include "qtl_database_pool.hpp"
#include "qtl_postgres.hpp"

namespace qtl
{

namespace postgres
{

class database_pool : public qtl::database_pool<database>
{
public:
	database_pool() : m_port(0) { }
	virtual ~database_pool() { }
	virtual database* new_database() throw() override
	{
		database* db=new database;
		if(!db->open(m_host.data(), m_user.data(), m_password.data(), m_port, m_database.data()))
		{
			delete db;
			db=NULL;
		}
		else
		{
			PQsetClientEncoding(db->handle(), "UTF8");
		}
		return db;
	}

protected:
	std::string m_host;
	unsigned short m_port;
	std::string m_database;
	std::string m_user;
	std::string m_password;
};

template<typename EventLoop>
class async_pool : public qtl::async_pool<async_pool<EventLoop>, EventLoop, async_connection>
{
	typedef qtl::async_pool<async_pool<EventLoop>, EventLoop, async_connection> base_class;
public:
	async_pool(EventLoop& ev) : base_class(ev) { }
	virtual ~async_pool() { }

	template<typename Handler>
	void new_connection(EventLoop& ev, Handler&& handler) throw()
	{
		async_connection* db = new async_connection;
		db->open(ev, [this, handler, db](const postgres::error& e) mutable {
			if (e)
			{
				delete db;
				db = nullptr;
			}
			handler(e, db);
		}, m_params);
	}

protected:
	std::map<std::string, std::string> m_params;
};

}

}

#endif //_QTL_MYSQL_POOL_H_
