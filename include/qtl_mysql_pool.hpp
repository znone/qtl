#ifndef _QTL_MYSQL_POOL_H_
#define _QTL_MYSQL_POOL_H_

#include "qtl_database_pool.hpp"
#include "qtl_mysql.hpp"

namespace qtl
{

namespace mysql
{

class database_pool : public qtl::database_pool<database>
{
public:
	database_pool() : m_port(0) { }
	virtual ~database_pool() { }
	virtual database* new_database() throw() override
	{
		database* db=new database;
		db->charset_name("utf8");
		if(!db->open(m_host.data(), m_user.data(), m_password.data(), m_database.data(), 0, m_port))
		{
			delete db;
			db=NULL;
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
