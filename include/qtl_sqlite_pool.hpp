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
	virtual bool open_database(database& db) override
	{
		try
		{
			db.open(m_filename.data(), m_flags);
			return true;
		}
		catch (error& e)
		{
			return false;
		}
	}

protected:
	std::string m_filename;
	int m_flags;
};

}

}

#endif //_QTL_SQLITE_POOL_H_

