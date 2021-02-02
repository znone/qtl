#ifndef _SQL_POSTGRES_H_
#define _SQL_POSTGRES_H_

#pragma once

#include <string>
#include <map>
#include <vector>
#include <array>
#include <exception>
#include <sstream>
#include <chrono>
#include <algorithm>
#include "qtl_common.hpp"

#define FRONTEND

#include <libpq-fe.h>
#include <pgtypes_error.h>
#include <pgtypes_interval.h>
#include <pgtypes_timestamp.h>
#include <pgtypes_numeric.h>
#include <pgtypes_date.h>

extern "C"
{
#include <c.h>
#include <postgres.h>
#include <catalog/pg_type.h>
}


#ifdef open
#undef open
#endif //open

#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif
#ifdef sprintf
#undef sprintf
#endif
#ifdef vfprintf
#undef vfprintf
#endif
#ifdef fprintf
#undef fprintf
#endif
#ifdef printf
#undef printf
#endif

namespace qtl
{
namespace postgres
{

namespace detail
{

	inline int16_t ntoh(int16_t v)
	{
		return ntohs(v);
	}
	inline uint16_t ntoh(uint16_t v)
	{
		return ntohs(v);
	}
	inline int32_t ntoh(int32_t v)
	{
		return ntohl(v);
	}
	inline uint32_t ntoh(uint32_t v)
	{
		return ntohl(v);
	}
	inline uint64_t ntoh(uint64_t v)
	{
#ifdef _WIN32
		return ntohll(v);
#else
		return be64toh(v);
#endif
	}
	inline int64_t ntoh(int64_t v)
	{
		return ntoh(static_cast<uint64_t>(v));
	}

	template<typename T>
	inline T& ntoh_inplace(typename std::enable_if<std::is_integral<T>::value && !std::is_const<T>::value, T>::type& v)
	{
		v = ntoh(v);
		return v;
	}

	inline int16_t hton(int16_t v)
	{
		return htons(v);
	}
	inline uint16_t hton(uint16_t v)
	{
		return htons(v);
	}
	inline int32_t hton(int32_t v)
	{
		return htonl(v);
	}
	inline uint32_t hton(uint32_t v)
	{
		return htonl(v);
	}
	inline uint64_t hton(uint64_t v)
	{
#ifdef _WIN32
		return htonll(v);
#else
		return htobe64(v);
#endif
	}
	inline int64_t hton(int64_t v)
	{
		return hton(static_cast<uint64_t>(v));
	}

	template<typename T>
	inline T& hton_inplace(typename std::enable_if<std::is_integral<T>::value && !std::is_const<T>::value>::type& v)
	{
		v = hton(v);
		return v;
	}

}

class base_database;
class result;

class error : public std::exception
{
public:
	error() : m_errmsg(nullptr) { }
	explicit error(PGconn* conn, PGVerbosity verbosity = PQERRORS_DEFAULT, PGContextVisibility show_context = PQSHOW_CONTEXT_ERRORS)
	{
		PQsetErrorVerbosity(conn, verbosity);
		PQsetErrorContextVisibility(conn, show_context);
		const char* errmsg = PQerrorMessage(conn);
		if (errmsg) m_errmsg = errmsg;
		else m_errmsg.clear();
	}

	explicit error(PGresult* res)
	{
		const char* errmsg = PQresultErrorMessage(res);
		if (errmsg) m_errmsg = errmsg;
		else m_errmsg.clear();
	}

	virtual const char* what() const NOEXCEPT override { return m_errmsg.data(); }

private:
	std::string m_errmsg;
};

inline void verify_pgtypes_error(int ret)
{
	if(ret && errno != 0)
		throw std::system_error(std::error_code(errno, std::generic_category()));
}

struct interval
{
	::interval* value;

	interval()
	{
		value = PGTYPESinterval_new();
	}
	explicit interval(char* str)
	{
		 value = PGTYPESinterval_from_asc(str, nullptr);
	}
	interval(const interval& src) : interval()
	{
		verify_pgtypes_error(PGTYPESinterval_copy(src.value, value));
	}
	interval(interval&& src)
	{
		value = src.value;
		src.value = PGTYPESinterval_new();
	}
	~interval()
	{
		PGTYPESinterval_free(value);
	}

	std::string to_string() const
	{
		return PGTYPESinterval_to_asc(value);
	}

	interval& operator=(const interval& src)
	{
		if(&src!=this)
			verify_pgtypes_error(PGTYPESinterval_copy(src.value, value));
		return *this;
	}
};

struct timestamp
{
	::timestamp value;

	timestamp() = default;

	static timestamp now()
	{
		timestamp result;
		PGTYPEStimestamp_current(&result.value);
		return result;
	}
	explicit timestamp(char* str)
	{
		value = PGTYPEStimestamp_from_asc(str, nullptr);
		verify_pgtypes_error(1);
	}
	
	int format(char* str, int n, const char* format) const
	{
		timestamp temp = *this;
		return PGTYPEStimestamp_fmt_asc(&temp.value, str, n, format);
	}
	static timestamp parse(char* str, const char* format)
	{
		timestamp result;
		verify_pgtypes_error(PGTYPEStimestamp_defmt_asc(str, format, &result.value));
		return result;
	}

	std::string to_string() const
	{
		char* str = PGTYPEStimestamp_to_asc(value);
		std::string result = str;
		PGTYPESchar_free(str);
		return result;
	}

	timestamp& operator += (const interval& span)
	{
		verify_pgtypes_error(PGTYPEStimestamp_add_interval(&value, span.value, &value));
		return *this;
	}

	timestamp& operator -= (const interval& span)
	{
		verify_pgtypes_error(PGTYPEStimestamp_sub_interval(&value, span.value, &value));
		return *this;
	}
};

inline timestamp operator+(const timestamp& a, const interval& b)
{
	timestamp result=a;
	return result+=b;
}

inline timestamp operator-(const timestamp& a, const interval& b)
{
	timestamp result=a;
	result -= b;
	return result;
}


struct timestamptz
{
	::TimestampTz value;
	/*
	timestamptz() = default;
	explicit timestamptz(pg_time_t v)
	{
		value = (TimestampTz)v -
			((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
		value *= USECS_PER_SEC;
	}

	static timestamptz now()
	{
		timestamptz result;
		auto tp = std::chrono::system_clock::now();
		int sec = tp.time_since_epoch().count()*std::nano::num/std::nano::den;
		int usec = tp.time_since_epoch().count()*std::nano::num % std::nano::den;

		result.value = (TimestampTz)sec -
			((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
		result.value = (result.value * USECS_PER_SEC) + usec;

		return result;
	}
	*/
};

struct date
{
	::date value;

	date() = default;
	explicit date(timestamp dt)
	{
		value = PGTYPESdate_from_timestamp(dt.value);
	}
	explicit date(char* str)
	{
		value = PGTYPESdate_from_asc(str, nullptr);
		verify_pgtypes_error(1);
	}
	explicit date(int year, int month, int day)
	{
		int mdy[3] = { month, day, year };
		PGTYPESdate_mdyjul(mdy, &value);
	}

	std::string to_string() const
	{
		char* str = PGTYPESdate_to_asc(value);
		std::string result = str;
		PGTYPESchar_free(str);
		return str;
	}

	static date now()
	{
		date result;
		PGTYPESdate_today(&result.value);
		return result;
	}

	static date parse(char* str, const char* format)
	{
		date result;
		verify_pgtypes_error(PGTYPESdate_defmt_asc(&result.value, format, str));
		return result;
	}

	std::string format(const char* format)
	{
		std::string result;
		result.resize(128);
		verify_pgtypes_error(PGTYPESdate_fmt_asc(value, format, const_cast<char*>(result.data())));
		result.resize(strlen(result.data()));
		return result;
	}

	std::tuple<int, int, int> get_date()
	{
		int mdy[3];
		PGTYPESdate_julmdy(value, mdy);
		return std::make_tuple(mdy[2], mdy[0], mdy[1]);
	}

	int dayofweek()
	{
		return PGTYPESdate_dayofweek(value);
	}
};

struct decimal
{
	::decimal value;
};

struct numeric
{
	::numeric* value;

	numeric()
	{
		value = PGTYPESnumeric_new();
	}
	numeric(int v) : numeric()
	{
		verify_pgtypes_error(PGTYPESnumeric_from_int(v, value));
	}
	numeric(long v) : numeric()
	{
		verify_pgtypes_error(PGTYPESnumeric_from_long(v, value));
	}
	numeric(double v) : numeric()
	{
		verify_pgtypes_error(PGTYPESnumeric_from_double(v, value));
	}
	numeric(const decimal& v) : numeric()
	{
		verify_pgtypes_error(PGTYPESnumeric_from_decimal(const_cast<::decimal*>(&v.value), value));
	}
	numeric(const numeric& src) : numeric()
	{
		verify_pgtypes_error(PGTYPESnumeric_copy(src.value, value));
	}
	explicit numeric(const char* str)
	{
		value = PGTYPESnumeric_from_asc(const_cast<char*>(str), nullptr);
	}
	~numeric() 
	{
		PGTYPESnumeric_free(value);
	}

	operator double() const
	{
		double result;
		verify_pgtypes_error(PGTYPESnumeric_to_double(value, &result));
		return result;
	}

	operator int() const
	{
		int result;
		verify_pgtypes_error(PGTYPESnumeric_to_int(value, &result));
		return result;
	}

	operator long() const
	{
		long result;
		verify_pgtypes_error(PGTYPESnumeric_to_long(value, &result));
		return result;
	}

	operator decimal() const
	{
		decimal result;
		verify_pgtypes_error(PGTYPESnumeric_to_decimal(value, &result.value));
		return result;
	}

	int compare(const numeric& other) const
	{
		return PGTYPESnumeric_cmp(value, other.value);
	}

	inline numeric& operator+=(const numeric& b)
	{
		verify_pgtypes_error(PGTYPESnumeric_add(value, b.value, value));
		return *this;
	}

	inline numeric& operator-=(const numeric& b)
	{
		verify_pgtypes_error(PGTYPESnumeric_sub(value, b.value, value));
		return *this;
	}

	inline numeric& operator*=(const numeric& b)
	{
		verify_pgtypes_error(PGTYPESnumeric_mul(value, b.value, value));
		return *this;
	}

	inline numeric& operator/=(const numeric& b)
	{
		verify_pgtypes_error(PGTYPESnumeric_div(value, b.value, value));
		return *this;
	}

	std::string to_string(int dscale=-1) const
	{
		char* str = PGTYPESnumeric_to_asc(value, dscale);
		std::string result = str;
		PGTYPESchar_free(str);
		return result;
	}
};

inline numeric operator+(const numeric& a, const numeric& b)
{
	numeric result;
	verify_pgtypes_error(PGTYPESnumeric_add(a.value, b.value, result.value));
	return result;
}

inline numeric operator-(const numeric& a, const numeric& b)
{
	numeric result;
	verify_pgtypes_error(PGTYPESnumeric_sub(a.value, b.value, result.value));
	return result;
}

inline numeric operator*(const numeric& a, const numeric& b)
{
	numeric result;
	verify_pgtypes_error(PGTYPESnumeric_mul(a.value, b.value, result.value));
	return result;
}

inline numeric operator/(const numeric& a, const numeric& b)
{
	numeric result;
	verify_pgtypes_error(PGTYPESnumeric_div(a.value, b.value, result.value));
	return result;
}

inline bool operator==(const numeric& a, const numeric& b)
{
	return a.compare(b) == 0;
}

inline bool operator<(const numeric& a, const numeric& b)
{
	return a.compare(b) < 0;
}

inline bool operator>(const numeric& a, const numeric& b)
{
	return a.compare(b) > 0;
}

inline bool operator<=(const numeric& a, const numeric& b)
{
	return a.compare(b) <= 0;
}

inline bool operator>=(const numeric& a, const numeric& b)
{
	return a.compare(b) >= 0;
}

inline bool operator!=(const numeric& a, const numeric& b)
{
	return a.compare(b) != 0;
}

/*
	template<typename T>
	struct oid_traits
	{
		typedef T value_type;
		static Oid type();
		static const value_type& get(const char*);
		static std::pair<const char*, size_t> data(const T& v);
	};
*/

template<typename T, Oid id>
struct base_object_traits
{
	typedef T value_type;
	enum { type = id };
	static bool is_match(Oid v)
	{
		return v == type;
	}
};

template<typename T>
struct object_traits;

#define QTL_POSTGRES_DEFOID(T, oid) \
template<> struct object_traits<T> : public base_object_traits<T, oid> { \
	static value_type get(const char* data, size_t n) { return *reinterpret_cast<const value_type*>(data); } \
	static std::pair<const char*, size_t> data(const T& v, std::vector<char>& /*data*/) { \
		return std::make_pair(reinterpret_cast<const char*>(&v), sizeof(T)); \
	} \
};

QTL_POSTGRES_DEFOID(bool, BOOLOID)
QTL_POSTGRES_DEFOID(char, CHAROID)
QTL_POSTGRES_DEFOID(float, FLOAT4OID)
QTL_POSTGRES_DEFOID(double, FLOAT8OID)

template<typename T, Oid id>
struct integral_traits : public base_object_traits<T, id>
{
	typedef typename base_object_traits<T, id>::value_type value_type;
	static value_type get(const char* data, size_t n)
	{
		return detail::ntoh(*reinterpret_cast<const value_type*>(data));
	}
	static std::pair<const char*, size_t> data(value_type v, std::vector<char>& data)
	{
		data.resize(sizeof(value_type));
		*reinterpret_cast<value_type*>(data.data()) = detail::hton(v);
		return std::make_pair(data.data(), data.size());
	}
};

template<> struct object_traits<int16_t> : public integral_traits<int16_t, INT2OID>
{
};

template<> struct object_traits<int32_t> : public integral_traits<int32_t, INT4OID>
{
};

template<> struct object_traits<int64_t> : public integral_traits<int64_t, INT8OID>
{
};

template<> struct object_traits<const char*> : public base_object_traits<const char*, TEXTOID>
{
	static bool is_match(Oid v)
	{
		return v == TEXTOID || v == VARCHAROID || v == BPCHAROID;
	}
	static const char* get(const char* data, size_t n) { return data; }
	static std::pair<const char*, size_t> data(const char* v, std::vector<char>& /*data*/)
	{
		return std::make_pair(v, strlen(v));
	}
};

template<> struct object_traits<char*> : public object_traits<const char*>
{
};

template<> struct object_traits<std::string> : public base_object_traits<std::string, TEXTOID>
{
	static bool is_match(Oid v)
	{
		return v == TEXTOID || v == VARCHAROID || v == BPCHAROID;
	}
	static value_type get(const char* data, size_t n) { return std::string(data, n); }
	static std::pair<const char*, size_t> data(const std::string& v, std::vector<char>& /*data*/)
	{
		return std::make_pair(v.data(), v.size());
	}
};

template<> struct object_traits<timestamp> : public base_object_traits<timestamp, TIMESTAMPOID>
{
	static value_type get(const char* data, size_t n)
	{
		value_type result = *reinterpret_cast<const timestamp*>(data);
		result.value = detail::ntoh(result.value);
		return result;
	}
	static std::pair<const char*, size_t> data(const timestamp& v, std::vector<char>& data)
	{
		data.resize(sizeof(timestamp));
		*reinterpret_cast<int64_t*>(data.data()) = detail::hton(v.value);
		return std::make_pair(data.data(), data.size());
	}
};

template<> struct object_traits<timestamptz> : public base_object_traits<timestamptz, TIMESTAMPTZOID>
{
	static value_type get(const char* data, size_t n) 
	{
		value_type result = *reinterpret_cast<const timestamptz*>(data);
		result.value = detail::ntoh(result.value);
		return result;
	}
	static std::pair<const char*, size_t> data(const timestamptz& v, std::vector<char>& data)
	{
		data.resize(sizeof(timestamptz));
		*reinterpret_cast<int64_t*>(data.data()) = detail::hton(v.value);
		return std::make_pair(data.data(), data.size());
	}
};

template<> struct object_traits<interval> : public base_object_traits<interval, INTERVALOID>
{
	static value_type get(const char* data, size_t n)
	{
		interval result;
		const ::interval* value = reinterpret_cast<const ::interval*>(data);
		result.value->time = detail::ntoh(value->time);
		result.value->month = detail::ntoh(value->month);
		return std::move(result);
	}
	static std::pair<const char*, size_t> data(const interval& v, std::vector<char>& data)
	{
		data.resize(sizeof(::interval));
		::interval* value = reinterpret_cast<::interval*>(data.data());
		value->time = detail::hton(v.value->time);
		value->month = detail::hton(v.value->month);
		return std::make_pair(data.data(), data.size());
	}
};

template<> struct object_traits<date> : public base_object_traits<date, DATEOID>
{
	static value_type get(const char* data, size_t n)
	{
		date result = *reinterpret_cast<const date*>(data);
		result.value = detail::ntoh(result.value);
		return result;
	}
	static std::pair<const char*, size_t> data(const date& v, std::vector<char>& data)
	{
		data.resize(sizeof(date));
		reinterpret_cast<date*>(data.data())->value = detail::hton(v.value);
		return std::make_pair(data.data(), data.size());
	}
};

struct binder
{
	binder() = default;
	template<typename T>
	explicit binder(const T& v)
	{
		m_type = object_traits<T>::value();
		auto pair = object_traits<T>::data(v);
		m_value = pair.first;
		m_length = pair.second;
	}
	binder(const char* data, size_t n, Oid oid)
	{
		m_type = oid;
		m_value = data;
		m_length = n;
	}

	Oid constexpr type() const { return m_type; }
	size_t length() const { return m_length; }
	const char* value() const { return m_value; }

	template<typename T>
	T get()
	{
		if (!object_traits<T>::is_match(m_type))
			throw std::bad_cast();

		return object_traits<T>::get(m_value, m_length);
	}

	void bind(std::nullptr_t)
	{
		m_value = nullptr;
		m_length = 0;
	}
	void bind(qtl::null)
	{
		bind(nullptr);
	}

	template<typename T>
	void bind(const T& v)
	{
		typedef typename std::decay<T>::type param_type;
		if (m_type!=0 && !object_traits<param_type>::is_match(m_type))
			throw std::bad_cast();

		auto pair = object_traits<param_type>::data(v, m_data);
		m_value = pair.first;
		m_length = pair.second;
	}
	void bind(const char* data, size_t length)
	{
		m_value = data;
		m_length = length;
	}

private:
	Oid m_type;
	const char* m_value;
	size_t m_length;
	std::vector<char> m_data;
};

template<size_t N, size_t I, typename Arg, typename... Other>
inline void make_binder_list_helper(std::array<binder, N>& binders, Arg&& arg, Other&&... other)
{
	binders[I]=binder(arg);
	make_binder_list_helper<N, I+1>(binders, std::forward<Other>(other)...);
}

template<typename... Args>
inline std::array<binder, sizeof...(Args)> make_binder_list(Args&&... args)
{
	std::array<binder, sizeof...(Args)> binders;
	binders.reserve(sizeof...(Args));
	make_binder_list_helper<sizeof...(Args), 0>(binders, std::forward<Args>(args)...);
	return binders;
}

template<typename T>
inline bool in_impl(const T& from, const T& to)
{
	return std::equal_to<T>()(from, to);
}

template<typename T, typename... Ts >
inline bool in_impl(const T& from, const T& to, const Ts&... other)
{
	return std::equal_to<T>()(from, to) ||  in_impl(from, other...);
}

template<typename T, T... values>
inline bool in(const T& v)
{
	return in_impl(v, values...);
}

class result
{
public:
	result(PGresult* res) : m_res(res) { }
	result(const result&) = delete;
	result(result&& src)
	{
		m_res = src.m_res;
		src.m_res = nullptr;
	}

	result& operator=(const result&) = delete;
	result& operator=(result&& src)
	{
		if (this != &src)
		{
			clear();
			m_res = src.m_res;
			src.m_res = nullptr;
		}
		return *this;
	}
	~result()
	{
		clear();
	}

	PGresult* handle() const { return m_res; }
	operator bool() const { return m_res != nullptr; }

	ExecStatusType status() const
	{
		return PQresultStatus(m_res);
	}

	long long affected_rows() const
	{
		char* result = PQcmdTuples(m_res);
		if (result)
			return strtoll(result, nullptr, 10);
		else
			return 0LL;
	}

	unsigned int get_column_count() const { return PQnfields(m_res); }

	int get_param_count() const
	{
		return PQnparams(m_res);
	}

	Oid get_param_type(int col) const
	{
		return PQparamtype(m_res, col);
	}

	const char* get_column_name(int col) const
	{
		return PQfname(m_res, col);
	}
	int get_column_index(const char* name) const
	{
		return PQfnumber(m_res, name);
	}
	int get_column_length(int col) const
	{
		return PQfsize(m_res, col);
	}
	Oid get_column_type(int col) const
	{
		return PQftype(m_res, col);
	}

	const char* get_value(int row, int col) const
	{
		return PQgetvalue(m_res, row, col);
	}

	bool is_null(int row, int col) const
	{
		return PQgetisnull(m_res, row, col);
	}

	int length(int row, int col) const
	{
		return PQgetlength(m_res, row, col);
	}

	Oid insert_oid() const
	{
		return PQoidValue(m_res);
	}

	template<ExecStatusType... Excepted>
	void verify_error()
	{
		if (m_res)
		{
			ExecStatusType got = status();
			if (! in<ExecStatusType, Excepted...>(got))
				throw error(m_res);
		}
	}

	void clear()
	{
		if (m_res)
		{
			PQclear(m_res);
			m_res = nullptr;
		}
	}

private:
	PGresult* m_res;
};

class base_statement
{
	friend class error;
public:
	 explicit base_statement(base_database& db);
	 ~base_statement()
	 {
	 }
	 base_statement(const base_statement&) = delete;
	 base_statement(base_statement&& src) 
		 : m_conn(src.m_conn), m_binders(std::move(src.m_binders)), m_res(std::move(src.m_res))
	 {
	 }

	result& get_result() { return m_res; }

	void close()
	{
		m_res=nullptr;
	}

	uint64_t affetced_rows() const
	{
		return m_res.affected_rows();
	}

	void bind_param(size_t index, const char* param, size_t length)
	{
		m_binders[index].bind(param, length);
	}
	template<class Param>
	void bind_param(size_t index, const Param& param)
	{
		m_binders[index].bind(param);
	}

	template<class Type>
	void bind_field(size_t index, Type&& value)
	{
		value = m_binders[index].get<typename std::remove_const<Type>::type>();
	}

	void bind_field(size_t index, char* value, size_t length)
	{
		memcpy(value, m_binders[index].value(), std::min<size_t>(length, m_binders[index].length()));
	}

	template<size_t N>
	void bind_field(size_t index, std::array<char, N>&& value)
	{
		bind_field(index, value.data(), value.size());
	}

	template<typename T>
	void bind_field(size_t index, bind_string_helper<T>&& value)
	{
		value.assign(m_binders[index].value(), m_binders[index].length());
	}

	template<typename Type>
	void bind_field(size_t index, indicator<Type>&& value)
	{
		if (m_res)
		{
			qtl::bind_field(*this, index, value.data);
			value.is_null = m_res.is_null(0, static_cast<int>(index));
			value.length = m_res.length(0, static_cast<int>(index));
			value.is_truncated = m_binders[index].length() < value.length;
		}
	}

protected:
	PGconn* m_conn;
	result m_res;
	std::vector<binder> m_binders;

	template<ExecStatusType... Excepted>
	void verify_error()
	{
		if (m_res)
			m_res.verify_error<Excepted...>();
		else
			throw error(m_conn);
	}
};

class statement : public base_statement
{
public:
	explicit statement(base_database& db) : base_statement(db) 
	{
	}
	statement(const statement&) = delete;
	statement(statement&& src) : base_statement(std::move(src)), _name(std::move(src._name))
	{
	}

	~statement()
	{
		finish(m_res);

		if (!_name.empty())
		{
			std::ostringstream oss;
			oss << "DEALLOCATE " << _name << ";";
			result res = PQexec(m_conn, oss.str().data());
			error e(res.handle());
		}
	}

	void open(const char* command, int nParams=0, const Oid *paramTypes=nullptr)
	{
		_name.resize(sizeof(intptr_t) * 2+1);
		int n = sprintf(const_cast<char*>(_name.data()), "q%p", this);
		_name.resize(n);
		std::transform(_name.begin(), _name.end(), _name.begin(), tolower);
		result res = PQprepare(m_conn, _name.data(), command, nParams, paramTypes);
		res.verify_error<PGRES_COMMAND_OK>();
	}
	template<typename... Types>
	void open(const char* command)
	{
		auto binder_list = make_binder_list(Types()...);
		std::array<Oid, sizeof...(Types)> types;
		std::transform(binder_list.begin(), binder_list.end(), types.begin(), [](const binder& b) {
			return b.type();
		});

		open(command, types.size(), types.data());
	}

	void attach(const char* name)
	{
		result res = PQdescribePrepared(m_conn, name);
		res.verify_error<PGRES_COMMAND_OK>();
		_name = name;
	}

	void execute()
	{
		if(!PQsendQueryPrepared(m_conn, _name.data(), 0, nullptr, nullptr, nullptr, 1))
			throw error(m_conn);
		if (!PQsetSingleRowMode(m_conn))
			throw error(m_conn);
		m_res = PQgetResult(m_conn);
		verify_error<PGRES_COMMAND_OK, PGRES_SINGLE_TUPLE>();
	}

	template<typename Types>
	void execute(const Types& params)
	{
		const size_t count = qtl::params_binder<statement, Types>::size;
		if (count > 0)
		{
			m_binders.resize(count);
			qtl::bind_params(*this, params);

			std::array<const char*, count> values;
			std::array<int, count> lengths;
			std::array<int, count> formats;
			for (size_t i = 0; i != m_binders.size(); i++)
			{
				values[i] = m_binders[i].value();
				lengths[i] = static_cast<int>(m_binders[i].length());
				formats[i] = 1;
			}
			if (!PQsendQueryPrepared(m_conn, _name.data(), static_cast<int>(m_binders.size()), values.data(), lengths.data(), formats.data(), 1))
				throw error(m_conn);
		}
		else
		{
			if (!PQsendQueryPrepared(m_conn, _name.data(), 0, nullptr, nullptr, nullptr, 1))
				throw error(m_conn);
		}
		if (!PQsetSingleRowMode(m_conn))
			throw error(m_conn);
		m_res = PQgetResult(m_conn);
		verify_error<PGRES_COMMAND_OK, PGRES_SINGLE_TUPLE>();
	}

	template<typename Types>
	bool fetch(Types&& values)
	{
		if (m_res)
		{
			ExecStatusType status = m_res.status();
			if (status == PGRES_SINGLE_TUPLE)
			{
				int count = m_res.get_column_count();
				if (count > 0)
				{
					m_binders.resize(count);
					for (int i = 0; i != count; i++)
					{
						m_binders[i]=binder(m_res.get_value(0, i), m_res.length(0, i), 
							m_res.get_column_type(i));
					}
					qtl::bind_record(*this, std::forward<Types>(values));
				}
				m_res = PQgetResult(m_conn);
				return true;
			}
			else
			{
				verify_error<PGRES_TUPLES_OK>();
			}
		}
		return false;
	}

	bool next_result()
	{
		m_res = PQgetResult(m_conn);
		return m_res && m_res.status() == PGRES_SINGLE_TUPLE;
	}

	void reset()
	{
		finish(m_res);
		m_res.clear();
	}

private:
	std::string _name;

	void finish(result& res)
	{
		while (res)
		{
			res = PQgetResult(m_conn);
		}
	}
};

class base_database
{
protected:
	base_database()
	{
		m_conn = nullptr;
	}

public:
	base_database(const base_database&) = delete;
	base_database(base_database&& src)
	{
		m_conn = src.m_conn;
		src.m_conn = nullptr;
	}

	~base_database()
	{
		if (m_conn)
			PQfinish(m_conn);
	}

	base_database& operator==(const base_database&) = delete;
	base_database& operator==(base_database&& src)
	{
		if (this != &src)
		{
			if (m_conn)
				PQfinish(m_conn);
			m_conn = src.m_conn;
			src.m_conn = nullptr;
		}
		return *this;
	}

	const char* errmsg() const
	{
		return PQerrorMessage(m_conn);
	}

	PGconn* handle() { return m_conn; }

	const char* encoding() const
	{
		int encoding = PQclientEncoding(m_conn);
		return (encoding >= 0) ? pg_encoding_to_char(encoding) : nullptr;
	}

	void trace(FILE* stream)
	{
		PQtrace(m_conn, stream);
	}
	void untrace()
	{
		PQuntrace(m_conn);
	}

	const char* current() const
	{
		return PQdb(m_conn);
	}

	const char* user() const
	{
		return PQuser(m_conn);
	}

	const char* host() const
	{
		return PQhost(m_conn);
	}

	const char* password() const
	{
		return PQpass(m_conn);
	}

	const char* port() const
	{
		return PQport(m_conn);
	}

	const char* options() const
	{
		return PQoptions(m_conn);
	}

	ConnStatusType status() const
	{
		return PQstatus(m_conn);
	}

	PGTransactionStatusType transactionStatus() const
	{
		return PQtransactionStatus(m_conn);
	}

	const char* parameterStatus(const char *paramName) const
	{
		return PQparameterStatus(m_conn, paramName);
	}

	void reset()
	{
		if(status() == CONNECTION_BAD)
			PQreset(m_conn);
	}

	bool is_alive()
	{
	}

	PGPing ping()
	{
	}

protected:
	PGconn* m_conn;
	void throw_exception() { throw postgres::error(m_conn); }
};

class simple_statment : public base_statement
{
public:
	simple_statment(base_database& db, qtl::postgres::result&& res) : base_statement(db)
	{
		m_res = std::move(res);
	}

	template<typename ValueProc>
	void fetch_all(ValueProc& proc)
	{
		int row_count = PQntuples(m_res.handle());
		if (row_count > 0)
		{
			int col_count = m_res.get_column_count();
			m_binders.resize(col_count);
			auto values = qtl::detail::make_values(proc);
			for (int i = 0; i != row_count; i++)
			{
				for (int j = 0; j != col_count; j++)
				{
					m_binders[j] = binder(m_res.get_value(i, j), m_res.length(i, j),
						m_res.get_column_type(j));
				}
				qtl::bind_record(*this, std::forward<decltype(values)>(values));
				proc(values);
			}
		}
	}
};

class database : public base_database, public qtl::base_database<database, statement>
{
public:
	database() = default;

	bool open(const std::map<std::string, std::string>& params, bool expand_dbname = false)
	{
		std::vector<const char*> keywords(params.size()+1);
		std::vector<const char*> values(params.size()+1);
		for (auto& param : params)
		{
			keywords.push_back(param.first.data());
			values.push_back(param.second.data());
		}
		keywords.push_back(nullptr);
		values.push_back(nullptr);
		m_conn = PQconnectdbParams(keywords.data(), values.data(), expand_dbname);
		return m_conn != nullptr && status()== CONNECTION_OK;
	}

	bool open(const char * conninfo)
	{
		m_conn = PQconnectdb(conninfo);
		return m_conn != nullptr && status() == CONNECTION_OK;
	}

	bool open(const char* host, const char* user, const char* password,
		unsigned short port = 5432, const char* db = "postgres", const char* options = nullptr)
	{
		char port_text[16];
		sprintf(port_text, "%u", port);
		m_conn = PQsetdbLogin(host, port_text, options, nullptr, db, user, password);
		return m_conn != nullptr && status() == CONNECTION_OK;
	}

	void close()
	{
		PQfinish(m_conn);
		m_conn = nullptr;
	}

	statement open_command(const char* query_text, size_t /*text_length*/)
	{
		statement stmt(*this);
		stmt.open(query_text);
		return stmt;
	}
	statement open_command(const char* query_text)
	{
		return open_command(query_text, 0);
	}
	statement open_command(const std::string& query_text)
	{
		return open_command(query_text.data());
	}

	void simple_execute(const char* query_text, uint64_t* paffected = nullptr)
	{
		qtl::postgres::result res(PQexec(m_conn, query_text));
		if (!res) throw_exception();
		res.verify_error<PGRES_COMMAND_OK, PGRES_TUPLES_OK>();
		if (paffected) *paffected = res.affected_rows();
	}
	template<typename ValueProc>
	void simple_query(const char* query_text, ValueProc&& proc)
	{
		qtl::postgres::result res(PQexec(m_conn, query_text));
		if (!res) throw_exception();
		res.verify_error<PGRES_COMMAND_OK, PGRES_TUPLES_OK>();
		if (res.status() == PGRES_TUPLES_OK)
		{
			simple_statment stmt(*this, std::move(res));
			stmt.fetch_all(std::forward<ValueProc>(proc));
		}
	}

	void auto_commit(bool on)
	{
		if(on)
			simple_execute("SET AUTOCOMMIT TO ON");
		else
			simple_execute("SET AUTOCOMMIT TO OFF");
	}

	void begin_transaction()
	{
		simple_execute("BEGIN");
	}
	void rollback()
	{
		simple_execute("ROLLBACK");
	}
	void commit()
	{
		simple_execute("COMMIT");
	}

};

typedef qtl::transaction<database> transaction;

template<typename Record>
using query_iterator = qtl::query_iterator<statement, Record>;

template<typename Record>
using query_result = qtl::query_result<statement, Record>;

inline base_statement::base_statement(base_database& db) : m_res(nullptr)
{
	m_conn = db.handle();
	m_res = nullptr;
}

}

}


#endif //_SQL_POSTGRES_H_

