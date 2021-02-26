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

}

}

#endif //_QTL_MYSQL_POOL_H_
