#ifndef _QTL_ASYNC_H_
#define _QTL_ASYNC_H_

#include <tuple>
#include <memory>
#include <chrono>
#include <functional>

namespace qtl
{

#ifdef _WIN32
	typedef SOCKET socket_type;
#else
	typedef int socket_type;
#endif 

namespace detail 
{

template<typename Values, typename RowHandler, typename FinishHandler>
struct async_fetch_helper : public std::enable_shared_from_this<async_fetch_helper<Values, RowHandler, FinishHandler>>
{
	async_fetch_helper(const Values& values, const RowHandler& row_handler, const FinishHandler& finish_handler)
		: m_values(values), m_row_handler(row_handler), m_finish_handler(finish_handler), m_auto_close_command(true)
	{
	}

	template<typename Command, typename Exception>
	void start(const std::shared_ptr<Command>& command)
	{
		auto self = this->shared_from_this();
		command->fetch(std::forward<Values>(m_values), [this, command]() {
			return qtl::detail::apply<RowHandler, Values>(std::forward<RowHandler>(m_row_handler), std::forward<Values>(m_values));
		}, [self, command](const Exception& e) {
			if (e || self->m_auto_close_command)
			{
				command->close([self, command, e](const Exception& new_e) {
					self->m_finish_handler(e ? e : new_e);
				});
			}
			else 
			{
				self->m_finish_handler(e);
			}
		});
	}
	
	void auto_close_command(bool auto_close)
	{
		m_auto_close_command = auto_close;
	}

private:
	Values m_values;
	RowHandler m_row_handler;
	FinishHandler m_finish_handler;
	bool m_auto_close_command;
};

template<typename Values, typename RowHandler, typename FinishHandler>
inline std::shared_ptr<async_fetch_helper<Values, RowHandler, FinishHandler>> make_fetch_helper(const Values& values, const RowHandler& row_handler, const FinishHandler& cpmplete_handler)
{
	return std::make_shared<async_fetch_helper<Values, RowHandler, FinishHandler>>(values, row_handler, cpmplete_handler);
}


template<typename Exception, typename Command, typename RowHandler, typename FinishHandler>
inline void async_fetch_command(const std::shared_ptr<Command>& command, FinishHandler&& finish_handler, RowHandler&& row_handler)
{
	auto values=make_values(row_handler);
	typedef decltype(values) values_type;
	auto helper = make_fetch_helper(std::forward<values_type>(values), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	helper->auto_close_command(false);
	helper->template start<Command, Exception>(command);
}

template<typename Exception, typename Command, typename RowHandler, typename FinishHandler, typename... OtherHandler>
inline void async_fetch_command(const std::shared_ptr<Command>& command, FinishHandler&& finish_handler, RowHandler&& row_handler, OtherHandler&&... other)
{
	async_fetch_command<Exception>(command, [command, finish_handler, other...](const Exception& e) mutable {
		if (e)
		{
			finish_handler(e);
		}
		else
		{
			command->next_result([=](const Exception& e) mutable {
				if (e)
					finish_handler(e);
				else
					async_fetch_command<Exception>(command, std::forward<FinishHandler>(finish_handler), std::forward<OtherHandler>(other)...);
			});
		}
	}, std::forward<RowHandler>(row_handler));
}

}

struct event
{
	enum io_flags
	{
		ef_read = 0x1,
		ef_write = 0x2,
		ef_exception = 0x4,
		ef_timeout =0x8,
		ev_all = ef_read | ef_write | ef_exception
	};

	virtual ~event() { }
	virtual void set_io_handler(int flags, long timeout, std::function<void(int)>&&) = 0;
	virtual void remove() = 0;
	virtual bool is_busying() = 0;
};

template<typename T, class Command>
class async_connection
{
public:
	template<typename EventLoop>
	bool bind(EventLoop& ev)
	{
		T* pThis = static_cast<T*>(this);
		if(m_event_handler) 
		{
			if(m_event_handler->is_busying())
				return false;
			unbind();
		}
		m_event_handler = ev.add(pThis);
		return m_event_handler!=nullptr;
	}

	qtl::event* event() const
	{
		return m_event_handler;
	}

	bool unbind()
	{
		if(m_event_handler)
		{
			if(m_event_handler->is_busying())
				return false;
			m_event_handler->remove();
			m_event_handler=nullptr;
		}
		return true;
	}

	/*
		ResultHandler defines as:
			void handler(const exception_type& e, uint64_t affected=0);
		Copies will be made of the handler as required.
		If an error occurred, value of affected is undefined.
		Note: parameter affected must has default value.
	*/
	template<typename Params, typename ResultHandler>
	void execute(ResultHandler handler, const char* query_text, size_t text_length, const Params& params)
	{
		T* pThis = static_cast<T*>(this);
		pThis->open_command(query_text, text_length, [handler, params](const typename T::exception_type& e, std::shared_ptr<Command>& command) mutable {
			if(e)
			{
				command->close([command, e, handler](const typename T::exception_type& ae) mutable {
					handler(e ? e : ae, 0);
				});
				return;
			}
			command->execute(params, [command, handler](const typename T::exception_type& e, uint64_t affected) mutable {
				command->close([command, handler, e, affected](const typename T::exception_type& ae) mutable {
					handler(e ? e : ae, affected);
				});
			});
		});
	}
	template<typename Params, typename ResultHandler>
	void execute(ResultHandler handler, const char* query_text, const Params& params)
	{
		return execute(std::forward<ResultHandler>(handler), query_text, strlen(query_text), params);
	}
	template<typename Params, typename ResultHandler>
	void execute(ResultHandler handler, const std::string& query_text, const Params& params)
	{
		return execute(std::forward<ResultHandler>(handler), query_text.data(), query_text.length(), params);
	}

	template<typename... Params, typename ResultHandler>
	void execute_direct(ResultHandler handler, const char* query_text, size_t text_length, const Params&... params)
	{
		execute(std::forward<ResultHandler>(handler), query_text, text_length, std::forward_as_tuple(params...));
	}
	template<typename... Params, typename ResultHandler>
	void execute_direct(ResultHandler handler, const char* query_text, const Params&... params)
	{
		execute(std::forward<ResultHandler>(handler), query_text, std::forward_as_tuple(params...));
	}
	template<typename... Params, typename ResultHandler>
	void execute_direct(ResultHandler handler, const std::string& query_text, const Params&... params)
	{
		execute(std::forward<ResultHandler>(handler), query_text, query_text, std::forward_as_tuple(params...));
	}

	/*
		ResultHandler defines as:
			void handler(const exception_type& e, uint64_t insert_id=0);
		Copies will be made of the handler as required.
		If an error occurred, value of insert_id is undefined.
		If the command is not insert statement, value of insert_id is zero.
		Note: parameter insert_id must has default value.
	*/
	template<typename Params, typename ResultHandler>
	void insert(ResultHandler handler, const char* query_text, size_t text_length, const Params& params)
	{
		T* pThis = static_cast<T*>(this);
		pThis->open_command(query_text, text_length, [handler, params](const typename T::exception_type& e, std::shared_ptr<Command>& command) {
			if(e)
			{
				command->close([command, e, handler](const typename T::exception_type& ae) mutable {
					handler(e ? e : ae, 0);
				});
			}
			else
			{
				command->execute(params, [command, handler](const typename T::exception_type& e, uint64_t affected) {
					auto insert_id = 0;
					if(!e && affected>0)
						insert_id  = command->insert_id();
					command->close([command, handler, e, insert_id](const typename T::exception_type& ae) mutable {
						handler(e ? e : ae, insert_id);
					});
				});
			}
		});
	}

	template<typename Params, typename ResultHandler>
	void insert(ResultHandler&& handler, const char* query_text, const Params& params)
	{
		insert(std::forward<ResultHandler>(handler), query_text, strlen(query_text), params);
	}

	template<typename Params, typename ResultHandler>
	void insert(ResultHandler&& handler, const std::string& query_text, const Params& params)
	{
		insert(std::forward<ResultHandler>(handler), query_text.data(), query_text.length(), params);
	}

	template<typename... Params, typename ResultHandler>
	void insert_direct(ResultHandler&& handler, const char* query_text, size_t text_length, const Params&... params)
	{
		insert(std::forward<ResultHandler>(handler), query_text, text_length, std::forward_as_tuple(params...));
	}

	template<typename... Params, typename ResultHandler>
	void insert_direct(ResultHandler&& handler, const char* query_text, const Params&... params)
	{
		insert(std::forward<ResultHandler>(handler), query_text, strlen(query_text), std::forward_as_tuple(params...));
	}

	template<typename... Params, typename ResultHandler>
	void insert_direct(ResultHandler&& handler, const std::string& query_text, const Params&... params)
	{
		insert(std::forward<ResultHandler>(handler), query_text.data(), query_text.length(), std::forward_as_tuple(params...));
	}

	/*
		RowHandler defines as:
			void row_handler(const Values& values);
		FinishHandler defines as:
			void finish_handler(const exception_type& e);
		If a row is fetched, the row handler is called.
		If an error occurred or the operation is completed, the result handler is called.
	*/
	template<typename Params, typename Values, typename RowHandler, typename FinishHandler>
	void query_explicit(const char* query_text, size_t text_length, const Params& params, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		T* pThis = static_cast<T*>(this);
		pThis->open_command(query_text, text_length, [values, row_handler, finish_handler, params](const typename T::exception_type& e, const std::shared_ptr<Command>& command) mutable {
			if(e)
			{
				finish_handler(e);
			}
			else
			{
				command->execute(params, [command, values, row_handler, finish_handler](const typename T::exception_type& e, uint64_t affected) mutable {
					auto helper=detail::make_fetch_helper(values, row_handler, finish_handler);
					helper->template start<Command, typename T::exception_type>(command);
				});
			}
		});
	}

	template<typename Params, typename Values, typename RowHandler, typename FinishHandler>
	void query_explicit(const char* query_text, const Params& params, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text, strlen(query_text), params, std::forward<Values>(values), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}
	template<typename Params, typename Values, typename RowHandler, typename FinishHandler>
	void query_explicit(const std::string& query_text, const Params& params, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text.data(), query_text.size(), params, std::forward<Values>(values), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}
	template<typename Values, typename RowHandler, typename FinishHandler>
	void query_explicit(const char* query_text, size_t text_length, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text, text_length, std::make_tuple(), std::forward<Values>(values), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}
	template<typename Values, typename RowHandler, typename FinishHandler>
	void query_explicit(const char* query_text, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text, strlen(query_text), std::make_tuple(), std::forward<Values>(values), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}
	template<typename Values, typename RowHandler, typename FinishHandler>
	void query_explicit(const std::string& query_text, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text, std::make_tuple(), std::forward<Values>(values), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}

	template<typename Params, typename RowHandler, typename FinishHandler>
	void query(const char* query_text, size_t text_length, const Params& params, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text, text_length, params, detail::make_values(row_handler), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}
	template<typename Params, typename RowHandler, typename FinishHandler>
	void query(const char* query_text, const Params& params, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text, params, detail::make_values(row_handler), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}
	template<typename Params, typename RowHandler, typename FinishHandler>
	void query(const std::string& query_text, const Params& params, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text, params, detail::make_values(row_handler), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}
	template<typename RowHandler, typename FinishHandler>
	void query(const char* query_text, size_t text_length, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text, text_length, detail::make_values(row_handler), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}
	template<typename RowHandler, typename FinishHandler>
	void query(const char* query_text, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text, detail::make_values(row_handler), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}
	template<typename RowHandler, typename FinishHandler>
	void query(const std::string& query_text, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		query_explicit(query_text, detail::make_values(row_handler), std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}

	template<typename Params, typename FinishHandler, typename... RowHandlers>
	void query_multi_with_params(const char* query_text, size_t text_length, const Params& params, FinishHandler&& finish_handler, RowHandlers&&... row_handlers)
	{
		T* pThis = static_cast<T*>(this);
		pThis->open_command(query_text, text_length, [params, finish_handler, row_handlers...](const typename T::exception_type& e, const std::shared_ptr<Command>& command) mutable {
			if (e)
			{
				finish_handler(e);
			}
			else
			{
				command->execute(params, [=](const typename T::exception_type& e, uint64_t affected) mutable {
					if (e)
						finish_handler(e);
					else
						qtl::detail::async_fetch_command<typename T::exception_type>(command, std::forward<FinishHandler>(finish_handler), std::forward<RowHandlers>(row_handlers)...);
				});
			}
		});
	}
	template<typename Params, typename FinishHandler, typename... RowHandlers>
	void query_multi_with_params(const char* query_text, const Params& params, FinishHandler&& finish_handler, RowHandlers&&... row_handlers)
	{
		query_multi_with_params(query_text, strlen(query_text), params, std::forward<FinishHandler>(finish_handler), std::forward<RowHandlers>(row_handlers)...);
	}
	template<typename Params, typename FinishHandler, typename... RowHandlers>
	void query_multi_with_params(const std::string& query_text, const Params& params, FinishHandler&& finish_handler, RowHandlers&&... row_handlers)
	{
		query_multi_with_params(query_text.data(), query_text.size(), params, std::forward<FinishHandler>(finish_handler), std::forward<RowHandlers>(row_handlers)...);
	}
	template<typename FinishHandler, typename... RowHandlers>
	void query_multi(const char* query_text, size_t text_length, FinishHandler&& finish_handler, RowHandlers&&... row_handlers)
	{
		query_multi_with_params<std::tuple<>, FinishHandler, RowHandlers...>(query_text, text_length, std::make_tuple(), std::forward<FinishHandler>(finish_handler), std::forward<RowHandlers>(row_handlers)...);
	}
	template<typename FinishHandler, typename... RowHandlers>
	void query_multi(const char* query_text, FinishHandler&& finish_handler, RowHandlers&&... row_handlers)
	{
		query_multi_with_params<std::tuple<>, FinishHandler, RowHandlers...>(query_text, strlen(query_text), std::make_tuple(), std::forward<FinishHandler>(finish_handler), std::forward<RowHandlers>(row_handlers)...);
	}
	template<typename FinishHandler, typename... RowHandlers>
	void query_multi(const std::string& query_text, FinishHandler&& finish_handler, RowHandlers&&... row_handlers)
	{
		query_multi_with_params<std::tuple<>, FinishHandler, RowHandlers...>(query_text.data(), query_text.size(), std::make_tuple(), std::forward<FinishHandler>(finish_handler), std::forward<RowHandlers>(row_handlers)...);
	}

protected:
	qtl::event* m_event_handler { nullptr };
};

}

#endif //_QTL_ASYNC_H_
