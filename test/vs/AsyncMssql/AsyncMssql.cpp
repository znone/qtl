#include "pch.h"
#include <iostream>
#include <array>
#include "../../../include/qtl_odbc.hpp"

class SimpleEventLoop
{
public:
	SimpleEventLoop()
	{
		m_objs.reserve(MAXIMUM_WAIT_OBJECTS);
		m_nExpired = INFINITE;
		m_hStopEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
		m_objs.push_back(m_hStopEvent);
	}

	qtl::event* add(HANDLE hEvent)
	{
		if (m_objs.size() < m_objs.capacity())
		{
			m_objs.push_back(hEvent);

			std::unique_ptr<event_item> item(new event_item(*this, hEvent));
			event_item* result = item.get();
			m_items.push_back(std::move(item));
			return result;
		}
		return nullptr;
	}

	void run()
	{
		while (m_objs.size()>1)
		{
			DWORD dwSize = m_objs.size();
			DWORD dwTimeout = m_nExpired - GetTickCount();
			DWORD dwObject = WaitForMultipleObjects(dwSize, m_objs.data(), FALSE, dwTimeout);
			if (dwObject >= WAIT_OBJECT_0 && WAIT_OBJECT_0 + dwSize)
			{
				DWORD index = dwObject - WAIT_OBJECT_0;
				HANDLE hEvent = m_objs[index];
				if (hEvent == m_hStopEvent)
					break;

				m_objs.erase(m_objs.begin() + index);
				fire(hEvent, qtl::event::ef_read | qtl::event::ef_write);
			}
			else if (dwObject == WAIT_TIMEOUT)
			{
				fire_timeout();
			}
		}
	}

	void stop()
	{
		SetEvent(m_hStopEvent);
	}

private:
	class event_item : public qtl::event
	{
	public:
		event_item(SimpleEventLoop& ev, HANDLE hEvent) : m_ev(&ev), m_hEvent(hEvent), m_busying(false)
		{
		}
		virtual void set_io_handler(int flags, long timeout, std::function<void(int)>&& handler) override
		{
			m_handler = handler;
			m_busying = true;
			m_nExpired = GetTickCount() + timeout*1000;
			m_ev->m_nExpired = std::min<DWORD>(m_ev->m_nExpired, m_nExpired);
		}
		virtual void remove() override
		{
			if(!m_busying)
				m_ev->remove(this);
		}
		virtual bool is_busying() override
		{
			return m_busying;
		}

		HANDLE m_hEvent;
		std::function<void(int)> m_handler;
		SimpleEventLoop* m_ev;
		DWORD m_nExpired;
		bool m_busying;
	};

	HANDLE m_hStopEvent;
	std::vector<HANDLE> m_objs;
	std::vector<std::unique_ptr<event_item>> m_items;
	DWORD m_nExpired;

	void remove_object(event_item* item)
	{
		auto it = std::find_if(m_objs.begin(), m_objs.end(), [item](HANDLE hEvent) {
			return item->m_hEvent == hEvent;
		});
		if (it != m_objs.end())
		{
			m_objs.erase(it);
		}
	}

	void remove(event_item* item)
	{
		remove_object(item);
		{
			auto it = std::find_if(m_items.begin(), m_items.end(), [item](const std::unique_ptr<event_item>& v) {
				return item == v.get();
			});
			if (it != m_items.end())
			{
				m_items.erase(it);
			}
		}
	}

	void fire(HANDLE hEvent, int flags)
	{
		auto it = std::find_if(m_items.begin(), m_items.end(), [hEvent](const std::unique_ptr<event_item>& v) {
			return hEvent == v->m_hEvent;
		});
		if (it != m_items.end())
		{
			(*it)->m_handler(flags);
		}
	}

	void fire_timeout()
	{
		for (auto& item : m_items)
		{
			if (item->m_nExpired < m_nExpired)
			{
				item->m_handler(qtl::event::ef_timeout);
				remove_object(item.get());
			}
			else
			{
				m_nExpired = std::min<DWORD>(item->m_nExpired, m_nExpired);
			}
		}
	}
};

int main()
{
	SimpleEventLoop ev;
	ev.run();
	return 0;
}
