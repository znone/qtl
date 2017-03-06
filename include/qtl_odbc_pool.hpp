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

}

}

#endif //_QTL_ODBC_POOL_HPP_
