#ifndef _QTL_ODBC_POOL_HPP_
#define _QTL_ODBC_POOL_HPP_

#include "qtl_database_pool.hpp"
#include "qtl_odbc.hpp"

namespace qtl
{

namespace odbc
{

class database_pool : public qtl::database_pool<database>
{
public:
	database_pool() { }
	virtual ~database_pool() { }
	virtual database* new_database() throw() override
	{
		database* db=NULL;
		try
		{
			db=new database(m_env);
			db->open(m_connection);
		}
		catch(error& e)
		{
			if(db)
			{
				delete db;
				db=NULL;
			}
		}
		return db;
	}

protected:
	std::string m_connection;
	environment m_env;
};

#ifdef QTL_ODBC_ENABLE_ASYNC_MODE

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
		async_connection* db = new async_connection(env);
		db->open(ev, [this, handler, db](const mysql::error& e) mutable {
			if (e)
			{
				delete db;
				db = nullptr;
			}
			handler(e, db);
		}, m_connection);
	}

protected:
	std::string m_connection;
	environment m_env;
};

#endif //QTL_ODBC_ENABLE_ASYNC_MODE

}

}

#endif //_QTL_ODBC_POOL_HPP_
