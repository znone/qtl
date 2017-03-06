#ifndef _QTL_DATABASE_POOL_H_
#define _QTL_DATABASE_POOL_H_

#include <memory>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <chrono>
#include <algorithm>

namespace qtl
{

template<typename Database>
class database_pool
{
public:
	typedef Database value_type;
	typedef std::shared_ptr<Database> pointer;

	database_pool()
		: m_trying_connection(false), m_stop_thread(false)
	{
	}

	virtual ~database_pool()
	{
		if(m_background_thread.joinable())
		{
			m_stop_thread=true;
			m_background_thread.join();
		}
		clear();
	}

	pointer get()
	{
		Database* db=popup();
		if(db==NULL && m_trying_connection==false)
			db=create_database();
		return pointer(db, [this](Database* db) {
			recovery(db);
		});
	}

	bool test_alive()
	{
		if(m_databases.empty()) 
			return false;
		std::unique_lock<std::mutex> lock(m_pool_mutex);
		auto it=m_databases.begin();
		while(it!=m_databases.end())
		{
			Database* db=*it;
			if(!db->is_alive())
			{
				delete db;
				it=m_databases.erase(it);
			}
			else
			{
				++it;
			}
		}
		if(m_databases.empty())
		{
			lock.unlock();
			try_connect();
			return false;
		}
		else return true;
	}

private:
	std::vector<Database*> m_databases;
	std::mutex m_pool_mutex;
	std::atomic<bool> m_trying_connection;
	std::thread m_background_thread;
	bool m_stop_thread;

	virtual Database* new_database() throw()=0;
	void recovery(Database* db)
	{
		if(db==NULL) return;
		if(db->is_alive())
		{
			std::lock_guard<std::mutex> lock(m_pool_mutex);
			m_databases.push_back(db);
		}
		else
		{
			delete db;
			{
				std::lock_guard<std::mutex> lock(m_pool_mutex);
				clear();
			}
			try_connect();
		}
	}

	Database* create_database()
	{
		Database* db=new_database();
		if(db) return db;

		{
			std::lock_guard<std::mutex> lock(m_pool_mutex);
			clear();
		}
		try_connect();
		return NULL;
	}

	Database* popup()
	{
		Database* db=NULL;
		std::lock_guard<std::mutex> lock(m_pool_mutex);
		if(!m_databases.empty())
		{
			db=m_databases.back();
			m_databases.pop_back();
		}
		return db;
	}

	void try_connect()
	{
		if(m_trying_connection)
			return;

		m_trying_connection=true;
		try
		{
			m_background_thread=std::thread(&database_pool<Database>::background_connect, this);
		}
		catch (std::system_error&)
		{
			m_trying_connection=false;
		}
	}

	void background_connect()
	{
		Database* db=NULL;
		int interval=1;
		while(db==NULL && m_stop_thread==false)
		{
			db=create_database();
			if(db==NULL)
			{
				std::this_thread::sleep_for(std::chrono::seconds(interval));
				if(interval<60) interval<<=1;
			}
		}
		if(db)
		{
			recovery(db);
		}
		m_background_thread.detach();
		m_trying_connection=false;
	}

	void clear()
	{
		std::for_each(m_databases.begin(), m_databases.end(), std::default_delete<Database>());
		m_databases.clear();
	}
};

}

#endif //_QTL_DATABASE_POOL_H_
