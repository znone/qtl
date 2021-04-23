#ifndef _QTL_ASIO_H_
#define _QTL_ASIO_H_

#include "qtl_async.hpp"
#include <asio/version.hpp>
#define ASIO_STANDALONE
#if defined(_MSC_VER) && _WIN32_WINNT<0x0600
#define ASIO_ENABLE_CANCELIO 1 
#endif
#if ASIO_VERSION < 101200
#include <asio/io_service.hpp>
#else
#include <asio/io_context.hpp>
#endif // ASIO_VERSION
#include <asio/strand.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/async_result.hpp>
#include <asio/steady_timer.hpp>

#if ASIO_VERSION < 100000
#error The asio version required by QTL is at least 10.0 
#endif

namespace qtl
{

namespace NS_ASIO = ::asio;

namespace asio
{

class service
{
public:
#if ASIO_VERSION < 101200
	typedef NS_ASIO::io_service service_type;
#else
	typedef NS_ASIO::io_context service_type;
#endif // ASIO_VERSION

	service() NOEXCEPT { }
	explicit service(int concurrency_hint) : _service(concurrency_hint) { }

	void reset()
	{
		_service.reset();
	}

	void run()
	{
		_service.run();
	}

	void stop()
	{
		_service.stop();
	}

	service_type& context() NOEXCEPT { return _service; }

private:

	class event_item : public qtl::event
	{
	public:
		event_item(service& service, qtl::socket_type fd)
			: _service(service), _strand(service.context()), _socket(service.context(), NS_ASIO::ip::tcp::v4(), fd), _timer(service.context()), _busying(false)
		{
		}

		NS_ASIO::ip::tcp::socket& next_layer() { return _socket; }

	public: // qtl::event
		virtual void set_io_handler(int flags, long timeout, std::function<void(int)>&& handler) override
		{
			if (flags&qtl::event::ef_read)
			{
#if ASIO_VERSION < 101200
				_socket.async_read_some(NS_ASIO::null_buffers(), _strand.wrap([this, handler](const NS_ASIO::error_code& ec, size_t bytes_transferred) {
#else
				_socket.async_wait(NS_ASIO::socket_base::wait_read, _strand.wrap([this, handler](const NS_ASIO::error_code& ec) {
#endif // ASIO_VERSION
					if (!ec)
						handler(qtl::event::ef_read);
					else if (ec == NS_ASIO::error::make_error_code(NS_ASIO::error::operation_aborted))
						handler(qtl::event::ef_timeout);
					else
						handler(qtl::event::ef_exception);
					_busying = false;
					_timer.cancel();
				}));
				_busying = true;
			}
			if (flags&qtl::event::ef_write)
			{
#if ASIO_VERSION < 101200
				_socket.async_write_some(NS_ASIO::null_buffers(), _strand.wrap([this, handler](const NS_ASIO::error_code& ec, size_t bytes_transferred) {
#else
				_socket.async_wait(NS_ASIO::socket_base::wait_write, _strand.wrap([this, handler](const NS_ASIO::error_code& ec) {
#endif //ASIO_VERSION
					if (!ec)
						handler(qtl::event::ef_write);
					else if (ec == NS_ASIO::error::make_error_code(NS_ASIO::error::operation_aborted))
						handler(qtl::event::ef_timeout);
					else
						handler(qtl::event::ef_exception);
					_timer.cancel();
					_busying = false;
				}));
				_busying = true;
			}
			if (timeout > 0)
			{
#if ASIO_VERSION < 101200
				_timer.expires_from_now(std::chrono::seconds(timeout));
#else
				_timer.expires_after(NS_ASIO::chrono::seconds(timeout));
#endif // ASIO_VERSION
				_timer.async_wait(_strand.wrap([this, handler](NS_ASIO::error_code ec) {
					if (!ec)
					{
						_socket.cancel(ec);
					}
				}));
			}
		}

		virtual void remove() override
		{
			if (_busying) return;
#if ASIO_VERSION >= 101200 && (!defined(_WIN32) || _WIN32_WINNT >= 0x0603 )
			_socket.release();
#endif //Windows 8.1
			_service.remove(this);
		}
		virtual bool is_busying() override
		{
			return _busying;
		}

	private:
		service& _service;
		service_type::strand _strand;
		NS_ASIO::ip::tcp::socket _socket;
		NS_ASIO::steady_timer _timer;
		bool _busying;
	};

public:

	template<typename Connection>
	event_item* add(Connection* connection)
	{
		event_item* item = new event_item(*this, connection->socket());
		std::lock_guard<std::mutex> lock(_mutex);
		_events.push_back(std::unique_ptr<event_item>(item));
		return item;
	}

private:
	service_type _service;
	std::mutex _mutex;
	std::vector<std::unique_ptr<event_item>> _events;

	void remove(event_item* item)
	{
		std::lock_guard<std::mutex> lock(_mutex);
		auto it = std::find_if(_events.begin(), _events.end(), [item](std::unique_ptr<event_item>& v) {
			return item==v.get();
		});
		if (it != _events.end()) _events.erase(it);
	}
};

#if ASIO_VERSION < 101200

template <typename Handler, typename Signature>
using async_init_type  = NS_ASIO::detail::async_result_init<Handler, Signature>;

template <typename Handler, typename Signature>
inline typename NS_ASIO::handler_type<Handler, Signature>::type 
get_async_handler(async_init_type<Handler, Signature>& init)
{
	return init.handler;
}

#else

template <typename Handler, typename Signature>
using async_init_type = NS_ASIO::async_completion<Handler, Signature>;

template <typename Handler, typename Signature>
inline typename async_init_type<Handler, Signature>::completion_handler_type 
get_async_handler(async_init_type<Handler, Signature>& init)
{
	return init.completion_handler;
}

#endif // ASIO_VERSION

template<typename Connection, typename OpenHandler, typename... Args>
inline ASIO_INITFN_RESULT_TYPE(OpenHandler, void(typename Connection::exception_type)) 
async_open(service& service, Connection& db, OpenHandler&& handler, Args&&... args)
{
#if ASIO_VERSION < 101200
	async_init_type<OpenHandler,
		void(typename Connection::exception_type)> init(std::forward<OpenHandler>(handler));
#else
	async_init_type<OpenHandler,
		void(typename Connection::exception_type)> init(handler);
#endif

	db.open(service, get_async_handler(init), std::forward<Args>(args)...);
	return init.result.get();
}

template<typename Connection, typename CloseHandler, typename... Args>
inline ASIO_INITFN_RESULT_TYPE(CloseHandler, void()) 
async_close(Connection& db, CloseHandler&& handler, Args&&... args)
{
#if ASIO_VERSION < 101200
	async_init_type<CloseHandler,
		void()> init(std::forward<CloseHandler>(std::forward<CloseHandler>(handler)));
#else
	async_init_type<CloseHandler,
		void()> init(std::forward<CloseHandler>(handler));
#endif

	db.close(get_async_handler(init), std::forward<Args>(args)...);
	return init.result.get();
}

template<typename Connection, typename ExecuteHandler, typename... Args>
inline ASIO_INITFN_RESULT_TYPE(ExecuteHandler, void(typename Connection::exception_type, uint64_t))  
async_execute(Connection& db, ExecuteHandler&& handler, Args&&... args)
{
#if ASIO_VERSION < 101200
	async_init_type<ExecuteHandler,
		void(typename Connection::exception_type, uint64_t)> init(std::forward<ExecuteHandler>(handler));
#else
	async_init_type<ExecuteHandler,
		void(typename Connection::exception_type, uint64_t)> init(handler);
#endif

	db.execute(get_async_handler(init), std::forward<Args>(args)...);
	return init.result.get();
}

template<typename Connection, typename ExecuteHandler, typename... Args>
inline ASIO_INITFN_RESULT_TYPE(ExecuteHandler, void(typename Connection::exception_type, uint64_t))  
async_execute_direct(Connection& db, ExecuteHandler&& handler, Args&&... args)
{
#if ASIO_VERSION < 101200
	async_init_type<ExecuteHandler,
		void(typename Connection::exception_type, uint64_t)> init(std::forward<ExecuteHandler>(handler));
#else
	async_init_type<ExecuteHandler,
		void(typename Connection::exception_type, uint64_t)> init(handler);
#endif

	db.execute_direct(get_async_handler(init), std::forward<Args>(args)...);
	return init.result.get();
}

template<typename Connection, typename ExecuteHandler, typename... Args>
inline ASIO_INITFN_RESULT_TYPE(ExecuteHandler, void(typename Connection::exception_type, uint64_t)) 
 async_insert(Connection& db, ExecuteHandler&& handler, Args&&... args)
{
#if ASIO_VERSION < 101200
	async_init_type<ExecuteHandler,
		void(typename Connection::exception_type, uint64_t)> init(std::forward<ExecuteHandler>(handler));
#else
	async_init_type<ExecuteHandler,
		void(typename Connection::exception_type, uint64_t)> init(handler);
#endif

	db.insert(get_async_handler(init), std::forward<Args>(args)...);
	return init.result.get();
}

template<typename Connection, typename ExecuteHandler, typename... Args>
inline ASIO_INITFN_RESULT_TYPE(ExecuteHandler, void(typename Connection::exception_type, uint64_t)) 
 async_insert_direct(Connection& db, ExecuteHandler&& handler, Args&&... args)
{
#if ASIO_VERSION < 101200
	async_init_type<ExecuteHandler,
		void(typename Connection::exception_type, uint64_t)> init(std::forward<ExecuteHandler>(handler));
#else
	async_init_type<ExecuteHandler,
		void(typename Connection::exception_type, uint64_t)> init(handler);
#endif

	db.insert_direct(get_async_handler(init), std::forward<Args>(args)...);
	return init.result.get();
}

template<typename Connection, typename FinishHandler, typename... Args>
inline ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type)) 
 async_query(Connection& db, FinishHandler&& handler, Args&&... args)
{
#if ASIO_VERSION < 101200
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(std::forward<FinishHandler>(handler));
#else
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(handler);
#endif

	db.query(std::forward<Args>(args)..., get_async_handler(init));
	return init.result.get();
}

template<typename Connection, typename FinishHandler, typename... Args>
inline ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type)) 
async_query_explicit(Connection& db, FinishHandler&& handler, Args&&... args)
{
#if ASIO_VERSION < 101200
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(std::forward<FinishHandler>(handler));
#else
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(handler);
#endif

	db.query_explicit(std::forward<Args>(args)..., get_async_handler(init));
	return init.result.get();
}

template<typename Connection, typename A1, typename A2, typename FinishHandler, typename... RowHandlers>
inline ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type))
async_query_multi_with_params(Connection& db, A1&& a1, A2&& a2, FinishHandler&& handler, RowHandlers&&... row_handlers)
{
#if ASIO_VERSION < 101200
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(std::forward<FinishHandler>(handler));
#else
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(handler);
#endif

	db.query_multi_with_params(std::forward<A1>(a1), std::forward<A2>(a2), get_async_handler(init), std::forward<RowHandlers>(row_handlers)...);
	return init.result.get();
}

template<typename Connection, typename A1, typename FinishHandler, typename... RowHandlers>
inline ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type))
async_query_multi_with_params(Connection& db, A1&& a1, FinishHandler&& handler, RowHandlers&&... row_handlers)
{
#if ASIO_VERSION < 101200
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(std::forward<FinishHandler>(handler));
#else
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(handler);
#endif

	db.query_multi_with_params(std::forward<A1>(a1), get_async_handler(init), std::forward<RowHandlers>(row_handlers)...);
	return init.result.get();
}

template<typename Connection, typename A1, typename A2, typename FinishHandler, typename... RowHandlers>
inline ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type))
async_query_multi(Connection& db, A1&& a1, A2&& a2, FinishHandler&& handler, RowHandlers&&... row_handlers)
{
#if ASIO_VERSION < 101200
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(std::forward<FinishHandler>(handler));
#else
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(handler);
#endif

	db.query_multi(std::forward<A1>(a1), std::forward<A2>(a2), get_async_handler(init), std::forward<RowHandlers>(row_handlers)...);
	return init.result.get();
}

template<typename Connection, typename A1, typename FinishHandler, typename... RowHandlers>
inline ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type))
async_query_multi(Connection& db, A1&& a1, FinishHandler&& handler, RowHandlers&&... row_handlers)
{
#if ASIO_VERSION < 101200
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(std::forward<FinishHandler>(handler));
#else
	async_init_type<FinishHandler,
		void(typename Connection::exception_type)> init(handler);
#endif

	db.query_multi(std::forward<A1>(a1), get_async_handler(init), std::forward<RowHandlers>(row_handlers)...);
	return init.result.get();
}

}

}

#endif //_QTL_ASIO_H_
