#ifndef _QTL_SQLITE_POOL_H_
#define _QTL_SQLITE_POOL_H_

#include "qtl_sqlite.hpp"
#include "qtl_database_pool.hpp"

namespace qtl
{

namespace sqlite
{

class database_pool : public qtl::database_pool<database>
{
public:
	database_pool() : m_flags(SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE) { }
	virtual database* new_database() throw() override
	{
		database* db=NULL;
		try
		{
			db=new database;
			db->open(m_filename.data(), m_flags);
		}
		catch (error& e)
		{
			delete db;
			db=NULL;
		}
		return db;
	}

protected:
	std::string m_filename;
	int m_flags;
};

}

}

#endif //_QTL_SQLITE_POOL_H_

