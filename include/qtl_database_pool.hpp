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
			try
			{
				m_background_thread.join();
			}
			catch (std::system_error&)
			{
				//igore the error
			}
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

template<typename T, typename EventLoop, typename Connection>
class async_pool
{
public:
	typedef Connection value_type;
	typedef std::shared_ptr<Connection> pointer;

	async_pool(EventLoop& ev)
		: m_ev(ev), m_trying_connecting(false)
	{
	}

	virtual ~async_pool()
	{
		clear();
	}

	/*
		Handler defines as:
		void handler(const pointer& ptr);
	*/
	template<typename Handler>
	void get(Handler&& handler, EventLoop* ev=nullptr)
	{
		Connection* db = popup();
		if(ev==nullptr) ev=&m_ev;
		
		if(db)
		{
			db->bind(*ev);
			handler(typename Connection::exception_type(), wrap(db));
		}
		else if (m_trying_connecting == false)
		{
			create_connection(ev, [this, handler](const typename Connection::exception_type& e,  Connection* db) {
				handler(e, wrap(db));
			});
		}
		else
		{
			handler(typename Connection::exception_type(), nullptr);
		}
	}

	void test_alive()
	{
		if (m_connections.empty())
			return;
		std::unique_lock<std::mutex> lock(m_pool_mutex);
		auto it = m_connections.begin();
		while (it != m_connections.end())
		{
			Connection* db = *it;
			db->is_alive([this, db](const typename Connection::exception_type& e) {
				if (e)
				{
					std::unique_lock<std::mutex> lock(m_pool_mutex);
					auto it = std::find(m_connections.begin(), m_connections.end(), db);
					delete db;
					m_connections.erase(it);
					if (m_connections.empty())
						try_connect();
				}
			});
			++it;
		}
	}

private:
	EventLoop& m_ev;
	std::vector<Connection*> m_connections;
	std::recursive_mutex m_pool_mutex;
	std::atomic<bool> m_trying_connecting;

	void recovery(Connection* db)
	{
		if (db == NULL) return;
		db->is_alive([this, db](const typename Connection::exception_type& e) {
			if (e)
			{
				{
					std::lock_guard<std::recursive_mutex> lock(m_pool_mutex);
					clear();
				}
				try_connect();
			}
			else
			{
				if(!db->unbind())
					throw std::runtime_error("destroy a busysing connection.");
				std::lock_guard<std::recursive_mutex> lock(m_pool_mutex);
				m_connections.push_back(db);
			}
		});
	}

	template<typename Handler>
	void create_connection(EventLoop* ev, Handler&& handler)
	{
		T* pThis = static_cast<T*>(this);
		pThis->new_connection(*ev, [this, handler](const typename Connection::exception_type& e, Connection* db) {
			handler(e, db);
			if (!db)
			{
				std::lock_guard<std::recursive_mutex> lock(m_pool_mutex);
				clear();

				timeval tv = { 1,  0};
				m_ev.set_timeout(tv, [this]() {
					try_connect();
				});
			}
		});
	}

	Connection* popup()
	{
		Connection* db = nullptr;
		std::lock_guard<std::recursive_mutex> lock(m_pool_mutex);
		if (!m_connections.empty())
		{
			db = m_connections.back();
			m_connections.pop_back();
		}
		return db;
	}

	void try_connect()
	{
		if (m_trying_connecting)
			return;

		m_trying_connecting = true;
		create_connection(&m_ev, [this](const typename Connection::exception_type& e, Connection* db) {
			if (db)
			{
				std::lock_guard<std::recursive_mutex> lock(m_pool_mutex);
				m_connections.push_back(db);
			}
			else
			{
				m_trying_connecting = false;
			}
		});
	}

	void clear()
	{
		std::for_each(m_connections.begin(), m_connections.end(), std::default_delete<Connection>());
		m_connections.clear();
	}

	pointer wrap(Connection* db)
	{
		if (db)
		{
			return pointer(db, [this](Connection* db) {
				recovery(db);
			});
		}
		else return nullptr;
	}
};

}

#endif //_QTL_DATABASE_POOL_H_
