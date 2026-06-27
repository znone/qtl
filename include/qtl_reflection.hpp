#ifndef _QTL_REFLECTION_H_
#define _QTL_REFLECTION_H_
#pragma once

#ifdef _QTL_ENABLE_CPP26

#include <meta>
#include "qtl_minmax.hpp"

namespace qtl
{

template<class T, typename = typename std::enable_if<std::is_class<T>::value>::type>
struct all_bind
{
	all_bind() : _record(*new(_defvalue)T()) { }
	explicit all_bind(T& value) : _record(value) { }
	template<typename... Args>
	explicit all_bind(Args&&... args)
		: _record(*new(_defvalue) T(args...))
	{
	}
	~all_bind()
	{
		detail::destroy_trivially(&_record, reinterpret_cast<T*>(_defvalue));
	}

	template<typename Command>
	void bind(Command& command)
	{
		size_t i = 0;
		const size_t count = command.get_column_count();
		template for (constexpr auto member : std::define_static_array(std::meta::nonstatic_data_members_of(^^T, std::meta::access_context::unchecked())))
		{
			if(i < count)
				qtl::bind_field(command, i, _record.[:member:]);
			i++;
		}
	}

	operator T&() { return _record; }

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
		: _record(*new(_defvalue) T(args...))
	{
	}
	~partition_bind()
	{
		detail::destroy_trivially(&_record, reinterpret_cast<T*>(_defvalue));
	}

	template<typename Command>
	void bind(Command& command)
	{
		size_t i = 0;
		const size_t count = command.get_column_count();
		constexpr auto members = std::define_static_array(std::meta::nonstatic_data_members_of(^^T, std::meta::access_context::unchecked()));
		template for (constexpr size_t I : {Indexes...})
		{
			if( i < count )
				qtl::bind_field(command, I, _record.[:members[I]:]);
			++i;
		}
	}

	operator T&() { return _record; }

private:
	char _defvalue[sizeof(T)];
	T& _record;

	static void check_indexes()
	{
		constexpr size_t member_count = std::meta::nonstatic_data_members_of(^^T, std::meta::access_context::unchecked()).size();
		static_assert(sizeof...(Indexes) <= member_count &&
			qtl::max<size_t, Indexes...> <= member_count);
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

template<typename Command, typename T>
struct record_binder<Command, all_bind<T>>
{
	void operator()(Command& command, all_bind<T>&& record) const
	{
		record.bind(command);
	}
};

template<typename Command, typename T, size_t... Indexes>
struct record_binder<Command, partition_bind<T, Indexes...>>
{
	void operator()(Command& command, partition_bind<T, Indexes...>&& record) const
	{
		record.bind(command);
	}
};

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
		: _record(*new(_defvalue) T(args...)), _matcher(matcher)
	{
	}
	~auto_bind_t()
	{
		detail::destroy_trivially(&_record, reinterpret_cast<T*>(_defvalue));
	}

	template<typename Command>
	void bind(Command& command)
	{
		template for (constexpr auto member : std::define_static_array(std::meta::nonstatic_data_members_of(^^T, std::meta::access_context::unchecked()))) {
			this->bind_field(command, std::meta::identifier_of(member), _record.[:member:]);
		}
	}

	operator T&() { return _record; }

private:
	char _defvalue[sizeof(T)];
	T& _record;
	Matcher _matcher;

	template<typename Command, typename Type>
	void bind_field(Command& command, const std::string_view& member_name, Type&& member)
	{
		size_t count = command.get_column_count();
		for (size_t i = 0; i != count; i++)
		{
			if (_matcher(member_name, command.get_column_name(i)))
				qtl::bind_field(command, i, std::forward<Type>(member));
		}
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

template<typename Command, typename T, typename Matcher>
struct record_binder<Command, auto_bind_t<T, Matcher>>
{
	void operator()(Command& command, auto_bind_t<T, Matcher>&& record) const
	{
		record.bind(command);
	}
};

}

#endif


#endif //_QTL_REFLECTION_H_

