#ifndef _QTL_PFR_H_
#define _QTL_PFR_H_

#pragma once

#ifndef _QTL_ENABLE_CPP17
#error The functions that use boost::pfr in the file require C++17 compiler
#endif

#include <boost/pfr.hpp>
#include <boost/preprocessor.hpp>
#include "qtl_minmax.hpp"

namespace qtl
{

namespace pfr
{

	template<class T, typename = typename std::enable_if<std::is_class<T>::value>::type>
	struct all_bind
	{
		all_bind() : _record(*new(_defvalue)T()) { }
		explicit all_bind(T& value) : _record(value) { }
		template<typename... Args>
		explicit all_bind(Args&&... args)
			: _record(*new(_defvalue) T(std::forward<Args>(args)...))
		{
		}
		~all_bind()
		{
			detail::destroy_trivially(&_record, reinterpret_cast<T*>(_defvalue));
		}

		template<typename Command>
		void bind(Command& command)
		{
			bind_record(command, boost::pfr::structure_tie(_record));
		}

		operator T& () { return _record; }

	private:
		char _defvalue[sizeof(T)];
		T& _record;
	};

	template<class T, size_t... Indexes>
	struct partition_bind
	{
		partition_bind() : _record(*new(_defvalue)T()) { check_indexes(); }
		explicit partition_bind(T& value) : _record(value) { check_indexes(); }
		template<typename... Args>
		explicit partition_bind(Args&&... args)
			: _record(*new(_defvalue) T(std::forward<Args>(args)...))
		{
		}
		~partition_bind()
		{
			detail::destroy_trivially(&_record, reinterpret_cast<T*>(_defvalue));
		}

		template<typename Command>
		void bind(Command& command)
		{
			const size_t count = command.get_column_count();
			this->bind_field<Command, size_t(0), Indexes...>(command, count);
		}

		operator T& () { return _record; }

	private:
		char _defvalue[sizeof(T)];
		T& _record;

		template<typename Command, size_t No, size_t I, size_t... Others>
		void bind_field(Command& command, const size_t column_count)
		{
			if(No < column_count)
				qtl::bind_field(command, No, boost::pfr::get<I>(_record));
			if constexpr (sizeof...(Others) > 0)
				this->bind_field<Command, No + 1, Others...>(command, column_count);
		}


		static void check_indexes()
		{
			static_assert(sizeof...(Indexes) <= boost::pfr::tuple_size<T>::value &&
				qtl::max<size_t, Indexes...> <= boost::pfr::tuple_size<T>::value);
		}
	};
		
	template<typename T, size_t... Is>
	inline auto bind_some(std::index_sequence<Is...> )
	{
		return partition_bind<T, Is...>();
	}

	template<typename T, size_t... Is>
	inline auto bind_some(T& obj, std::index_sequence<Is...>)
	{
		return partition_bind<T, Is...>(obj);
	}

	template<typename T, size_t N>
	inline auto bind_front()
	{
		return bind_some<T>(std::make_index_sequence<N>());
	}

	template<typename T, size_t N>
	inline auto bind_front(T& obj)
	{
		return bind_some<T>(obj, std::make_index_sequence<N>());
	}
}

template<typename Command, typename T>
struct record_binder<Command, pfr::all_bind<T>>
{
	void operator()(Command& command, pfr::all_bind<T>&& record) const
	{
		record.bind(command);
	}
};

template<typename Command, typename T, size_t... Indexes>
struct record_binder<Command, pfr::partition_bind<T, Indexes...>>
{
	void operator()(Command& command, pfr::partition_bind<T, Indexes...>&& record) const
	{
		record.bind(command);
	}
};

#if defined(_QTL_ENABLE_CPP20) && defined(BOOST_PFR_CORE_NAME_HPP)

namespace pfr
{

	struct simple_matcher
	{
		bool operator()(const std::string_view& member, const std::string_view& column) const
		{
			if (member.size() != column.size())
				return false;

#ifdef _MSC_VER
			return _strnicmp(member.data(), column.data(), member.size()) == 0;
#else
			return strncasecmp(member.data(), column.data(), member.size()) == 0;
#endif
		}
	};

	template<class T, typename Matcher, typename = typename std::enable_if<std::is_class<T>::value>::type>
	struct auto_bind_t
	{
		auto_bind_t(Matcher matcher) : _record(*new(_defvalue)T()), _matcher(matcher) { }
		explicit auto_bind_t(T& value, Matcher matcher) : _record(value), _matcher(matcher) { }
		template<typename... Args>
		explicit auto_bind_t(Matcher matcher, Args&&... args)
			: _record(*new(_defvalue) T(std::forward<Args>(args)...)), _matcher(matcher)
		{
		}
		~auto_bind_t()
		{
			detail::destroy_trivially(&_record, reinterpret_cast<T*>(_defvalue));
		}

		template<typename Command>
		void bind(Command& command)
		{
			this->bind_field<Command, boost::pfr::tuple_size<T>::value - 1>(command);
		}

		operator T& () { return _record; }

	private:
		char _defvalue[sizeof(T)];
		T& _record;
		Matcher _matcher;

		template<typename Command, size_t No>
		void bind_field(Command& command)
		{
			size_t count = command.get_column_count();
			for (size_t i = 0; i != count; i++)
			{
				if (_matcher(boost::pfr::get_name<No, T>(), command.get_column_name(i)))
					qtl::bind_field(command, i, boost::pfr::get<No>(_record));
			}

			if constexpr (No > 0)
				this->bind_field<Command, No - 1>(command);
		}
	};

	template<class T, typename Matcher = simple_matcher>
	inline auto_bind_t<T, Matcher> auto_bind(T& v, Matcher matcher = Matcher())
	{
		return auto_bind_t<T, Matcher>(v, matcher);
	}

	template<class T, typename Matcher = simple_matcher>
	inline auto_bind_t<T, Matcher> auto_bind(Matcher matcher = Matcher())
	{
		return auto_bind_t<T, Matcher>(matcher);
	}

	template<class T, typename Matcher, typename... Args>
	inline auto_bind_t<T, Matcher> auto_bind(Matcher matcher, Args... args)
	{
		return auto_bind_t<T, Matcher>(matcher, args...);
	}

}

template<typename Command, typename T, typename Matcher>
struct record_binder<Command, pfr::auto_bind_t<T, Matcher>>
{
	void operator()(Command& command, pfr::auto_bind_t<T, Matcher>&& record) const
	{
		record.bind(command);
	}
};

#endif

template<class T, typename... Members>
struct bind_struct_t
{
public:
	bind_struct_t(Members T::*... members) : _value(*new(_defobj)T()), _fields(std::tie(_value.*members...)) {  }
	bind_struct_t(T& obj, Members T::*... members) : _value(obj), _fields(std::tie(_value.*members...)) { }
	~bind_struct_t()
	{
		detail::destroy_trivially(&_value, reinterpret_cast<T*>(_defobj));
	}

	template<typename Command>
	void bind(Command& command)
	{
		bind_record(command, std::forward<std::tuple<Members&...>>(_fields));
	}

	operator T& () { return _value; }

private:
	char _defobj[sizeof(T)];
	T& _value;
	std::tuple<Members&...> _fields;
};

template<class T, typename... Members>
inline struct bind_struct_t<T, Members...> bind_struct(Members T::*... members)
{
	return bind_struct_t<T, Members...>(members...);
}

template<class T, typename... Members>
inline struct bind_struct_t<T, Members...> bind_struct(T& obj, Members T::*... members)
{
	return bind_struct_t<T, Members...>(obj, members...);
}

template<typename Command, typename T, typename... Members>
struct record_binder<Command, bind_struct_t<T, Members...>>
{
	void operator()(Command& command, bind_struct_t<T, Members...>&& record) const
	{
		record.bind(command);
	}
};

#define _QTL_BIND_FIELD(z, i, fields) \
	&BOOST_PP_TUPLE_ELEM(0, fields)::BOOST_PP_TUPLE_ELEM(i, BOOST_PP_TUPLE_POP_FRONT(fields))

#define _QTL_BIND_OBJ_FIELD(z, i, fields) \
	&std::decay<decltype(BOOST_PP_TUPLE_ELEM(0, fields))>::type::BOOST_PP_TUPLE_ELEM(i, BOOST_PP_TUPLE_POP_FRONT(fields))

#define QTL_BIND_STRUCT(S, ...) \
   qtl::bind_struct<S>(BOOST_PP_ENUM(BOOST_PP_TUPLE_SIZE((__VA_ARGS__)), _QTL_BIND_FIELD, (S, __VA_ARGS__))) \

#define QTL_BIND_OBJECT(obj, ...) \
   qtl::bind_struct<std::decay<decltype(obj)>::type>(obj, BOOST_PP_ENUM(BOOST_PP_TUPLE_SIZE((__VA_ARGS__)), _QTL_BIND_OBJ_FIELD, (obj, __VA_ARGS__)))

}

#endif //_QTL_PFR_H_
