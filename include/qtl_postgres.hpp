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
#include <assert.h>
#include "qtl_common.hpp"
#include "qtl_async.hpp"

#define FRONTEND

#include <libpq-fe.h>
#include <libpq/libpq-fs.h>
#include <pgtypes_error.h>
#include <pgtypes_interval.h>
#include <pgtypes_timestamp.h>
#include <pgtypes_numeric.h>
#include <pgtypes_date.h>

extern "C"
{
#include <c.h>
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
#ifdef rename
#undef rename
#endif
#ifdef unlink
#undef unlink
#endif

#if defined(_WIN32) && _WIN32_WINNT < 0x0601

#ifdef _M_IX86

#define _WS2_32_WINSOCK_SWAP_LONGLONG(l)            \
            ( ( ((l) >> 56) & 0x00000000000000FFLL ) |       \
              ( ((l) >> 40) & 0x000000000000FF00LL ) |       \
              ( ((l) >> 24) & 0x0000000000FF0000LL ) |       \
              ( ((l) >>  8) & 0x00000000FF000000LL ) |       \
              ( ((l) <<  8) & 0x000000FF00000000LL ) |       \
              ( ((l) << 24) & 0x0000FF0000000000LL ) |       \
              ( ((l) << 40) & 0x00FF000000000000LL ) |       \
              ( ((l) << 56) & 0xFF00000000000000LL ) )

#ifndef htonll
__inline unsigned __int64 htonll(unsigned __int64 Value)
{
	const unsigned __int64 Retval = _WS2_32_WINSOCK_SWAP_LONGLONG(Value);
	return Retval;
}
#endif /* htonll */

#ifndef ntohll
__inline unsigned __int64 ntohll(unsigned __int64 Value)
{
	const unsigned __int64 Retval = _WS2_32_WINSOCK_SWAP_LONGLONG(Value);
	return Retval;
}

#endif /* ntohll */

#endif

#endif

namespace qtl
{
namespace postgres
{

namespace detail
{

	inline int16_t ntoh(int16_t v)
	{
		return static_cast<int16_t>(ntohs(v));
	}
	inline uint16_t ntoh(uint16_t v)
	{
		return ntohs(v);
	}
	inline int32_t ntoh(int32_t v)
	{
		return static_cast<int32_t>(ntohl(v));
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

	template<typename T, typename = typename std::enable_if<std::is_integral<T>::value && !std::is_const<T>::value>::type>
	inline T& ntoh_inplace(T& v)
	{
		v = ntoh(v);
		return v;
	}

	inline int16_t hton(int16_t v)
	{
		return static_cast<int16_t>(htons(v));
	}
	inline uint16_t hton(uint16_t v)
	{
		return htons(v);
	}
	inline int32_t hton(int32_t v)
	{
		return static_cast<int32_t>(htonl(v));
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

	template<typename T, typename = typename std::enable_if<std::is_integral<T>::value && !std::is_const<T>::value>::type>
	inline T& hton_inplace(T& v)
	{
		v = hton(v);
		return v;
	}

	template<typename T, typename = typename std::enable_if<std::is_integral<T>::value && !std::is_const<T>::value>::type>
	std::pair<std::vector<char>::iterator, size_t> push(std::vector<char>& buffer, T v)
	{
		v = hton_inplace(v);
		char* data = reinterpret_cast<char*>(&v);
		auto it = buffer.insert(buffer.end(), data, data + sizeof(T));
		return std::make_pair(it, sizeof(T));
	}

	template<typename T, typename = typename std::enable_if<std::is_integral<T>::value && !std::is_const<T>::value>::type>
	const char* pop(const char* data, T& v)
	{
		v = ntoh(*reinterpret_cast<const T*>(data));
		return data + sizeof(T);
	}
}

class base_database;
class result;

class error : public std::exception
{
public:
	error() : m_errmsg() { }
	explicit error(PGconn* conn, PGVerbosity verbosity = PQERRORS_DEFAULT, PGContextVisibility show_context = PQSHOW_CONTEXT_ERRORS)
	{
		//PQsetErrorVerbosity(conn, verbosity);
		//PQsetErrorContextVisibility(conn, show_context);
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

	explicit error(const char* errmsg) : m_errmsg(errmsg) { }

	virtual const char* what() const NOEXCEPT override { return m_errmsg.data(); }
	operator bool() const { return !m_errmsg.empty(); }

protected:
	std::string m_errmsg;
};

class timeout : public error
{
public:
	timeout()
	{
		m_errmsg = "timeout";
	}
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

class large_object : public qtl::blobbuf
{
public:
	large_object() : m_conn(nullptr), m_id(InvalidOid), m_fd(-1) { }
	large_object(PGconn* conn, Oid loid, std::ios_base::openmode mode)
	{
		open(conn, loid, mode);
	}
	large_object(const large_object&) = delete;
	large_object(large_object&& src)
	{
		swap(src);
		src.m_conn = nullptr;
		src.m_fd = -1;
	}
	~large_object()
	{
		close();
	}

	static large_object create(PGconn* conn, Oid loid = InvalidOid)
	{
		Oid oid = lo_create(conn, loid);
		if (oid == InvalidOid)
			throw error(conn);
		return large_object(conn, oid, std::ios::in|std::ios::out|std::ios::binary);
	}
	static large_object load(PGconn* conn, const char* filename, Oid loid = InvalidOid)
	{
		Oid oid = lo_import_with_oid(conn, filename, loid);
		if (oid == InvalidOid)
			throw error(conn);
		return large_object(conn, oid, std::ios::in | std::ios::out | std::ios::binary);
	}
	void save(const char* filename) const
	{
		if (lo_export(m_conn, m_id, filename) < 0)
			throw error(m_conn);
	}

	void unlink()
	{
		close();
		if (lo_unlink(m_conn, m_id) < 0)
			throw error(m_conn);
	}

	large_object& operator=(const large_object&) = delete;
	large_object& operator=(large_object&& src)
	{
		if (this != &src)
		{
			swap(src);
			src.close();
		}
		return *this;
	}
	bool is_open() const { return m_fd >= 0; }
	Oid oid() const { return m_id; }

	void open(PGconn* conn, Oid loid, std::ios_base::openmode mode)
	{
		int lomode = 0;
		if (mode&std::ios_base::in)
			lomode |= INV_READ;
		if (mode&std::ios_base::out)
			lomode |= INV_WRITE;
		m_conn = conn;
		m_id = loid;
		m_fd = lo_open(m_conn, loid, lomode);
		if (m_fd < 0)
			throw error(m_conn);

		m_size = size();
		init_buffer(mode);
		if (mode&std::ios_base::trunc)
		{
			if (lo_truncate(m_conn, m_fd, 0) < 0)
				throw error(m_conn);
		}
	}

	void close()
	{
		if (m_fd >= 0)
		{
			overflow();
			if (lo_close(m_conn, m_fd) < 0)
				throw error(m_conn);
			m_fd = -1;
		}
	}

	void flush()
	{
		if (m_fd >= 0)
			overflow();
	}

	size_t size() const
	{
		pg_int64 size = 0;
		if (m_fd >= 0)
		{
			pg_int64 org = lo_tell64(m_conn, m_fd);
			size = lo_lseek64(m_conn, m_fd, 0, SEEK_END);
			lo_lseek64(m_conn, m_fd, org, SEEK_SET);
		}
		return size;
	}

	void resize(size_t n)
	{
		if (m_fd >= 0 && lo_truncate64(m_conn, m_fd, n) < 0)
			throw error(m_conn);
	}

	void swap(large_object& other)
	{
		std::swap(m_conn, other.m_conn);
		std::swap(m_id, other.m_id);
		std::swap(m_fd, other.m_fd);
		qtl::blobbuf::swap(other);
	}

protected:
	enum { default_buffer_size = 4096 };

	virtual bool read_blob(char* buffer, off_type& count, pos_type position) override
	{
		return lo_lseek64(m_conn, m_fd, position, SEEK_SET) >= 0 && lo_read(m_conn, m_fd, buffer, count) > 0;
	}
	virtual void write_blob(const char* buffer, size_t count) override
	{
		if (lo_write(m_conn, m_fd, buffer, count) < 0)
			throw error(m_conn);
	}

private:
	PGconn* m_conn;
	Oid m_id;
	int m_fd;
};

struct array_header
{
	int32_t ndim;
	int32_t flags;
	int32_t elemtype;
	struct dimension {
		int32_t length;
		int32_t lower_bound;
	} dims[1];
};

/*
	template<typename T>
	struct oid_traits
	{
		typedef T value_type;
		static Oid type_id;
		static Oid array_type_id; //optional
		static const char* get(value_type& result, const char* begin, const char* end);
		static std::pair<const char*, size_t> data(const T& v, std::vector<char*>& buffer);
	};
*/

template<typename T, Oid id>
struct base_object_traits
{
	typedef T value_type;
	enum { type_id = id };
	static bool is_match(Oid v)
	{
		return v == type_id;
	}
};

template<typename T>
struct object_traits;

#define QTL_POSTGRES_SIMPLE_TRAITS(T, oid, array_oid) \
template<> struct object_traits<T> : public base_object_traits<T, oid> { \
	enum { array_type_id = array_oid }; \
	static const char* get(value_type& result, const char* data, const char* end) \
	{ \
		result = *reinterpret_cast<const value_type*>(data); \
		return data+sizeof(value_type); \
	} \
	static std::pair<const char*, size_t> data(const T& v, std::vector<char>& /*data*/) { \
		return std::make_pair(reinterpret_cast<const char*>(&v), sizeof(T)); \
	} \
};

QTL_POSTGRES_SIMPLE_TRAITS(bool, BOOLOID, 1000)
QTL_POSTGRES_SIMPLE_TRAITS(char, CHAROID, 1002)
QTL_POSTGRES_SIMPLE_TRAITS(float, FLOAT4OID, FLOAT4ARRAYOID)
QTL_POSTGRES_SIMPLE_TRAITS(double, FLOAT8OID, 1022)

template<typename T, Oid id, Oid array_id>
struct integral_traits : public base_object_traits<T, id>
{
	enum { array_type_id  = array_id };
	typedef typename base_object_traits<T, id>::value_type value_type;
	static const char* get(value_type& v, const char* data, const char* end)
	{
		return detail::pop(data, v);
	}
	static std::pair<const char*, size_t> data(value_type v, std::vector<char>& buffer)
	{
		size_t n = buffer.size();
		detail::push(buffer, v);
		return std::make_pair(buffer.data()+n, buffer.size()-n);
	}
};

template<> struct object_traits<int16_t> : public integral_traits<int16_t, INT2OID, INT2ARRAYOID>
{
};

template<> struct object_traits<int32_t> : public integral_traits<int32_t, INT4OID, INT4ARRAYOID>
{
};

template<> struct object_traits<int64_t> : public integral_traits<int64_t, INT8OID, 1016>
{
};

template<> struct object_traits<Oid> : public integral_traits<Oid, OIDOID, OIDARRAYOID>
{
};

template<typename T>
struct text_traits : public base_object_traits<T, TEXTOID>
{
	enum { array_type_id = TEXTARRAYOID };
};

template<> struct object_traits<const char*> : public text_traits<const char*>
{
	static bool is_match(Oid v)
	{
		return v == TEXTOID || v == VARCHAROID || v == BPCHAROID;
	}
	static const char* get(const char*& result, const char* data, const char* end)
	{
		result = data;
		return end; 
	}
	static std::pair<const char*, size_t> data(const char* v, std::vector<char>& /*buffer*/)
	{
		return std::make_pair(v, strlen(v));
	}
};

template<> struct object_traits<char*> : public object_traits<const char*>
{
};

template<> struct object_traits<std::string> : public text_traits<std::string>
{
	static bool is_match(Oid v)
	{
		return v == TEXTOID || v == VARCHAROID || v == BPCHAROID;
	}
	static const char* get(value_type& result, const char* data, const char* end)
	{ 
		result.assign(data, end); 
		return end;
	}
	static std::pair<const char*, size_t> data(const std::string& v, std::vector<char>& /*buffer*/)
	{
		return std::make_pair(v.data(), v.size());
	}
};

template<> struct object_traits<timestamp> : public base_object_traits<timestamp, TIMESTAMPOID>
{
	enum { array_type_id = TIMESTAMPOID+1 };
	static const char* get(value_type& result, const char* data, const char* end)
	{
		result = *reinterpret_cast<const timestamp*>(data);
		result.value = detail::ntoh(result.value);
		return data+sizeof(timestamp);
	}
	static std::pair<const char*, size_t> data(const timestamp& v, std::vector<char>& buffer)
	{
		size_t n = buffer.size();
		detail::push(buffer, v.value);
		return std::make_pair(buffer.data()+n, buffer.size()-n);
	}
};

template<> struct object_traits<timestamptz> : public base_object_traits<timestamptz, TIMESTAMPTZOID>
{
	enum { array_type_id = TIMESTAMPTZOID+1 };
	static const char* get(value_type& result, const char* data, const char* end)
	{
		result = *reinterpret_cast<const timestamptz*>(data);
		result.value = detail::ntoh(result.value);
		return data+sizeof(timestamptz);
	}
	static std::pair<const char*, size_t> data(const timestamptz& v, std::vector<char>& buffer)
	{
		size_t n = buffer.size();
		detail::push(buffer, v.value);
		return std::make_pair(buffer.data() + n, buffer.size() - n);
	}
};

template<> struct object_traits<interval> : public base_object_traits<interval, INTERVALOID>
{
	enum { array_type_id = INTERVALOID+1 };
	static const char* get(value_type& result, const char* data, const char* end)
	{
		const ::interval* value = reinterpret_cast<const ::interval*>(data);
		result.value->time = detail::ntoh(value->time);
		result.value->month = detail::ntoh(value->month);
		return data+sizeof(interval);
	}
	static std::pair<const char*, size_t> data(const interval& v, std::vector<char>& buffer)
	{
		size_t n = buffer.size();
		detail::push(buffer, v.value->time);
		detail::push(buffer, v.value->month);
		return std::make_pair(buffer.data()+n, buffer.size()-n);
	}
};

template<> struct object_traits<date> : public base_object_traits<date, DATEOID>
{
	enum { array_type_id = 1182 };
	static const char* get(value_type& result, const char* data, const char* end)
	{
		result = *reinterpret_cast<const date*>(data);
		result.value = detail::ntoh(result.value);
		return data+sizeof(date);
	}
	static std::pair<const char*, size_t> data(const date& v, std::vector<char>& buffer)
	{
		size_t n=buffer.size();
		detail::push(buffer, v.value);
		return std::make_pair(buffer.data()+n, buffer.size()-n);
	}
};

template<typename T>
struct bytea_traits : public base_object_traits<T, BYTEAOID>
{
	enum { array_type_id = 1001 };
};

template<> struct object_traits<qtl::const_blob_data> : public bytea_traits<qtl::const_blob_data>
{
	static const char* get(value_type& result, const char* data, const char* end)
	{
		result.data = data;
		result.size = end-data;
		return end;
	}
	static std::pair<const char*, size_t> data(const qtl::const_blob_data& v, std::vector<char>& /*buffer*/)
	{
		assert(v.size <= UINT32_MAX);
		return std::make_pair(static_cast<const char*>(v.data), v.size);
	}
};

template<> struct object_traits<qtl::blob_data> : public bytea_traits<qtl::blob_data>
{
	static const char* get(qtl::blob_data& value, const char* data, const char* end)
	{
		if (value.size < end-data)
			throw std::out_of_range("no enough buffer to receive blob data.");
		memcpy(value.data, data, end-data);
		return end;
	}
	static std::pair<const char*, size_t> data(const qtl::blob_data& v, std::vector<char>& /*buffer*/)
	{
		assert(v.size <= UINT32_MAX);
		return std::make_pair(static_cast<char*>(v.data), v.size);
	}
};

template<> struct object_traits<std::vector<uint8_t>> : public bytea_traits<std::vector<uint8_t>>
{
	static const char* get(value_type& result, const char* data, const char* end)
	{
		result.assign(data, end);
		return end;
	}
	static std::pair<const char*, size_t> data(const std::vector<uint8_t>& v, std::vector<char>& /*buffer*/)
	{
		assert(v.size() <= UINT32_MAX);
		return std::make_pair(reinterpret_cast<const char*>(v.data()), v.size());
	}
};

template<> struct object_traits<large_object> : public base_object_traits<large_object, OIDOID>
{
	enum { array_type_id = OIDARRAYOID };
	static value_type get(PGconn* conn, const char* data, const char* end)
	{
		int32_t oid;
		object_traits<int32_t>::get(oid, data, end);
		return large_object(conn, oid, std::ios::in | std::ios::out | std::ios::binary);
	}
	static std::pair<const char*, size_t> data(const large_object& v, std::vector<char>& buffer)
	{
		return object_traits<int32_t>::data(v.oid(), buffer);
	}
};

template<typename T, Oid id> 
struct vector_traits : public base_object_traits<std::vector<T>, id>
{
	typedef typename base_object_traits<std::vector<T>, id>::value_type value_type;
	static const char* get(value_type& result, const char* data, const char* end)
	{
		if (end - data < sizeof(array_header))
			throw std::overflow_error("insufficient data left in message");

		array_header header = *reinterpret_cast<const array_header*>(data);
		detail::ntoh_inplace(header.ndim);
		detail::ntoh_inplace(header.flags);
		detail::ntoh_inplace(header.elemtype);
		detail::ntoh_inplace(header.dims[0].length);
		detail::ntoh_inplace(header.dims[0].lower_bound);
		if (header.ndim != 1 || !object_traits<T>::is_match(header.elemtype))
			throw std::bad_cast();

		data += sizeof(array_header);
		result.reserve(header.dims[0].length);

		for (int32_t i = 0; i != header.dims[0].length; i++)
		{
			int32_t size;
			T value;
			data = detail::pop(data, size);
			if (end - data < size)
				throw std::overflow_error("insufficient data left in message");
			data = object_traits<T>::get(value, data, data + size);
			if (data > end)
				throw std::overflow_error("insufficient data left in message");
			result.push_back(value);
		}
		return data;
	}
	static std::pair<const char*, size_t> data(const std::vector<T>& v, std::vector<char>& buffer)
	{
		assert(v.size() <= INT32_MAX);
		size_t n = buffer.size();
		buffer.resize(n+sizeof(array_header));
		array_header* header = reinterpret_cast<array_header*>(buffer.data()+n);
		header->ndim = detail::hton(1);
		header->flags = detail::hton(0);
		header->elemtype = detail::hton(static_cast<int32_t>(object_traits<T>::type_id));
		header->dims[0].length = detail::hton(static_cast<int32_t>(v.size()));
		header->dims[0].lower_bound = detail::hton(1);

		std::vector<char> temp;
		for (const T& e : v)
		{
			std::pair<const char*, size_t> blob = object_traits<T>::data(e, temp);
			detail::push(buffer, static_cast<int32_t>(blob.second));
			buffer.insert(buffer.end(), blob.first, blob.first + blob.second);
		}
		return std::make_pair(buffer.data()+n, buffer.size()-n);
	}
};

template<typename Iterator, Oid id>
struct iterator_traits : public base_object_traits<Iterator, id>
{
	static const char* get(Iterator first, Iterator last, const char* data, const char* end)
	{
		if (end - data < sizeof(array_header))
			throw std::overflow_error("insufficient data left in message");

		array_header header = *reinterpret_cast<const array_header*>(data);
		detail::ntoh_inplace(header.ndim);
		detail::ntoh_inplace(header.flags);
		detail::ntoh_inplace(header.elemtype);
		detail::ntoh_inplace(header.dims[0].length);
		detail::ntoh_inplace(header.dims[0].lower_bound);
		if (header.ndim != 1 || !object_traits<typename std::iterator_traits<Iterator>::value_type>::is_match(header.elemtype))
			throw std::bad_cast();

		data += sizeof(array_header);
		if (std::distance(first, last) < header.dims[0].length)
			throw std::out_of_range("length of array out of range");

		Iterator it = first;
		for (int32_t i = 0; i != header.dims[0].length; i++, it++)
		{
			int32_t size;
			data = detail::pop(data, size);
			if (end - data < size)
				throw std::overflow_error("insufficient data left in message");
			data = object_traits<typename std::iterator_traits<Iterator>::value_type>::get(*it, data, data + size);
			if (data >= end)
				throw std::overflow_error("insufficient data left in message");
		}
		return data;
	}
	static std::pair<const char*, size_t> data(Iterator first, Iterator last, std::vector<char>& buffer)
	{
		assert(std::distance(first, last) <= INT32_MAX);
		size_t n = buffer.size();
		buffer.resize(n + sizeof(array_header));
		array_header* header = reinterpret_cast<array_header*>(buffer.data() + n);
		header->ndim = detail::hton(1);
		header->flags = detail::hton(0);
		header->elemtype = detail::hton(static_cast<int32_t>(object_traits<typename std::iterator_traits<Iterator>::value_type>::type_id));
		header->dims[0].length = detail::hton(static_cast<int32_t>(std::distance(first, last)));
		header->dims[0].lower_bound = detail::hton(1);

		std::vector<char> temp;
		for (Iterator it=first; it!=last; it++)
		{
			std::pair<const char*, size_t> blob = object_traits<typename std::iterator_traits<Iterator>::value_type>::data(*it, temp);
			detail::push(buffer, static_cast<int32_t>(blob.second));
			buffer.insert(buffer.end(), blob.first, blob.first + blob.second);
		}
		return std::make_pair(buffer.data() + n, buffer.size() - n);
	}
};

template<typename Iterator, Oid id>
struct range_traits : public base_object_traits<std::pair<Iterator, Iterator>, id>
{
	static const char* get(std::pair<Iterator, Iterator>& result, const char* data, const char* end)
	{
		return iterator_traits<Iterator, id>::get(result.first, result.second, data, end);
	}
	static std::pair<const char*, size_t> data(const std::pair<Iterator, Iterator>& v, std::vector<char>& buffer)
	{
		return iterator_traits<Iterator, id>::data(v.first, v.second, buffer);
	}
};

template<typename T> 
struct object_traits<std::vector<T>> : public vector_traits<T, object_traits<T>::array_type_id>
{
};

template<typename Iterator>
struct object_traits<std::pair<typename std::enable_if<std::is_object<typename std::iterator_traits<Iterator>::value_type>::value, Iterator>::type, Iterator>> : 
	public range_traits<Iterator, object_traits<typename std::iterator_traits<Iterator>::value_type>::array_type_id>
{
};

template<typename T, size_t N, Oid id>
struct carray_traits : public base_object_traits<T(&)[N], id>
{
	static const char* get(T (&result)[N], const char* data, const char* end)
	{
		return iterator_traits<T*, id>::get(std::begin(result), std::end(result), data, end);
	}
	static std::pair<const char*, size_t> data(const T (&v)[N], std::vector<char>& buffer)
	{
		return iterator_traits<const T*, id>::data(std::begin(v), std::end(v), buffer);
	}
};

template<typename T, size_t N, Oid id>
struct array_traits : public base_object_traits<std::array<T, N>, id>
{
	static const char* get(std::array<T, N>& result, const char* data, const char* end)
	{
		return iterator_traits<T*, id>::get(std::begin(result), std::end(result), data, end);
	}
	static std::pair<const char*, size_t> data(const std::array<T, N>& v, std::vector<char>& buffer)
	{
		return iterator_traits<T*, id>::data(std::begin(v), std::end(v), buffer);
	}
};

template<typename T, size_t N> struct object_traits<T (&)[N]> : public carray_traits<T, N, object_traits<T>::array_type_id>
{
};

template<typename T, size_t N> struct object_traits<std::array<T, N>> : public array_traits<T, N, object_traits<T>::array_type_id>
{
};

namespace detail
{

	struct field_header
	{
		Oid type;
		int32_t length;
	};

	template<typename Type>
	static const char* get_field(Type& field, const char* data, const char* end)
	{
		field_header header = *reinterpret_cast<const field_header*>(data);
		detail::ntoh_inplace(header.type);
		detail::ntoh_inplace(header.length);
		data += sizeof(field_header);
		if (end - data < header.length)
			throw std::overflow_error("insufficient data left in message");

		return object_traits<Type>::get(field, data, data + header.length);
	}

	template<typename Tuple, size_t N>
	struct get_field_helper
	{
		const char* operator()(Tuple& result, const char* data, const char* end)
		{
			if (end - data < sizeof(field_header))
				throw std::overflow_error("insufficient data left in message");

			auto& field = std::get<std::tuple_size<Tuple>::value - N>(result);
			data = get_field(field, data, end);
			get_field_helper<Tuple, N - 1>()(result, data, end);
			return data;
		}
	};
	template<typename Tuple>
	struct get_field_helper<Tuple, 1>
	{
		const char* operator()(Tuple& result, const char* data, const char* end)
		{
			if (end - data < sizeof(field_header))
				throw std::overflow_error("insufficient data left in message");

			auto& field = std::get<std::tuple_size<Tuple>::value - 1>(result);
			return get_field(field, data, end);
		}
	};

	template<typename Type>
	static void push_field(const Type& field, std::vector<char>& buffer)
	{
		std::vector<char> temp;
		detail::push(buffer, static_cast<int32_t>(object_traits<Type>::type_id));
		auto result = object_traits<Type>::data(field, temp);
		detail::push(buffer, static_cast<int32_t>(result.second));
		buffer.insert(buffer.end(), result.first, result.first + result.second);
	}

	template<typename Tuple, size_t N>
	struct push_field_helper
	{
		void operator()(const Tuple& data, std::vector<char>& buffer)
		{
			const auto& field = std::get<std::tuple_size<Tuple>::value - N>(data);
			push_field(field, buffer);
			push_field_helper<Tuple, N - 1>()(data, buffer);
		}
	};
	template<typename Tuple>
	struct push_field_helper<Tuple, 1>
	{
		void operator()(const Tuple& data, std::vector<char>& buffer)
		{
			const auto& field = std::get<std::tuple_size<Tuple>::value - 1>(data);
			push_field(field, buffer);
		}
	};

	template<typename Tuple>
	static const char* get_fields(Tuple& result, const char* data, const char* end)
	{
		return get_field_helper<Tuple, std::tuple_size<Tuple>::value>()(result, data, end);
	}

	template<typename Tuple>
	static void push_fields(const Tuple& data, std::vector<char>& buffer)
	{
		push_field_helper<Tuple, std::tuple_size<Tuple>::value>()(data, buffer);
	}

}

template<typename Tuple, Oid id>
struct tuple_traits : public base_object_traits<Tuple, id>
{
	typedef typename base_object_traits<Tuple, id>::value_type value_type;
	static const char* get(value_type& result, const char* data, const char* end)
	{
		int32_t count;
		data = detail::pop(data, count);
		if (data >= end)
			throw std::overflow_error("insufficient data left in message");
		if (std::tuple_size<Tuple>::value != count)
			throw std::bad_cast();
		return detail::get_fields(result, data, end);
	}
	static std::pair<const char*, size_t> data(const value_type& v, std::vector<char>& buffer)
	{
		size_t n = buffer.size();
		detail::push(buffer, static_cast<int32_t>(std::tuple_size<value_type>::value));
		detail::push_fields(v, buffer);
		return std::make_pair(buffer.data()+n, buffer.size()-n);
	}
};

template<typename... Types>
struct object_traits<std::tuple<Types...>> : public tuple_traits<std::tuple<Types...>, InvalidOid>
{
};

template<typename T1, typename T2>
struct object_traits<std::pair<T1, T2>> : public tuple_traits<std::pair<T1, T2>, InvalidOid>
{
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

		T v;
		object_traits<T>::get(v, m_value, m_value + m_length);
		return v;
	}
	template<typename T>
	T get(PGconn* conn)
	{
		if (!object_traits<T>::is_match(m_type))
			throw std::bad_cast();

		return object_traits<T>::get(conn, m_value, m_value + m_length);
	}
	template<typename T>
	void get(T& v)
	{
		if (object_traits<T>::type_id!= InvalidOid && !object_traits<T>::is_match(m_type))
			throw std::bad_cast();

		object_traits<T>::get(v, m_value, m_value + m_length);
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

	template<typename T, typename = typename std::enable_if<!std::is_array<T>::value>::type>
	void bind(const T& v)
	{
		typedef typename std::decay<T>::type param_type;
		if (m_type!=0 && !object_traits<param_type>::is_match(m_type))
			throw std::bad_cast();

		auto pair = object_traits<param_type>::data(v, m_data);
		m_value = pair.first;
		m_length = pair.second;
	}
	void bind(const char* data, size_t length=0)
	{
		m_value = data;
		if(length>0) m_length = length;
		else m_length = strlen(data);
	}
	template<typename T, size_t N>
	void bind(const T(&v)[N])
	{
		if (m_type != 0 && !object_traits<T(&)[N]>::is_match(m_type))
			throw std::bad_cast();

		auto pair = object_traits<T(&)[N]>::data(v, m_data);
		m_value = pair.first;
		m_length = pair.second;
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

	template<ExecStatusType... Excepted>
	void verify_error(error& e)
	{
		if (m_res)
		{
			ExecStatusType got = status();
			if (!in<ExecStatusType, Excepted...>(got))
				e = error(m_res);
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
		 : m_conn(src.m_conn), m_binders(std::move(src.m_binders)), m_res(std::move(src.m_res)), _name(std::move(src._name))
	 {
	 }
	 base_statement& operator=(const base_statement&) = delete;
	 base_statement& operator=(base_statement&& src)
	 {
		 if (this != &src)
		 {
			 close();
			 m_conn = src.m_conn;
			 m_binders = std::move(src.m_binders);
			 m_res = std::move(src.m_res);
		 }
		 return *this;
	 }

	result& get_result() { return m_res; }

	void close()
	{
		m_res = nullptr;
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
		if (m_res.is_null(0, static_cast<int>(index)))
			value = Type();
		else
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

	void bind_field(size_t index, large_object&& value)
	{
		if (m_res.is_null(0, static_cast<int>(index)))
			value.close();
		else
			value = m_binders[index].get<large_object>(m_conn);
	}
	void bind_field(size_t index, blob_data&& value)
	{
		if (m_res.is_null(0, static_cast<int>(index)))
		{
			value.data = nullptr;
			value.size = 0;
		}
		else
		{
			m_binders[index].get(value);
		}
	}

	template<typename... Types>
	void bind_field(size_t index, std::tuple<Types...>&& value)
	{
		if (m_res.is_null(0, static_cast<int>(index)))
			value = std::tuple<Types...>();
		else
			m_binders[index].get(value);
	}

#ifdef _QTL_ENABLE_CPP17

	template<typename T>
	inline void bind_field(size_t index, std::optional<T>&& value)
	{
		if (m_res.is_null(0, static_cast<int>(index)))
		{
			value.reset();
		}
		else
		{
			T v;
			bind_field(index, v);
			value = std::move(v);
		}
	}

	void bind_field(size_t index, std::any&& value)
	{
		if (m_res.is_null(0, static_cast<int>(index)))
		{
			value = nullptr;
		}
		else
		{
			Oid oid = m_res.get_column_type(index);
			switch (oid)
			{
			case object_traits<bool>::type_id:
				value = field_cast<bool>(index);
				break;
			case object_traits<char>::type_id:
				value = field_cast<char>(index);
				break;
			case object_traits<float>::type_id:
				value = field_cast<float>(index);
				break;
			case object_traits<double>::type_id:
				value = field_cast<double>(index);
				break;
			case object_traits<int16_t>::type_id:
				value = field_cast<int16_t>(index);
				break;
			case object_traits<int32_t>::type_id:
				value = field_cast<int32_t>(index);
				break;
			case object_traits<int64_t>::type_id:
				value = field_cast<int64_t>(index);
				break;
			case object_traits<Oid>::type_id:
				value = field_cast<Oid>(index);
				break;
			case object_traits<std::string>::type_id:
				value = field_cast<std::string>(index);
				break;
			case object_traits<timestamp>::type_id:
				value = field_cast<timestamp>(index);
				break;
			case object_traits<interval>::type_id:
				value = field_cast<interval>(index);
				break;
			case object_traits<date>::type_id:
				value = field_cast<date>(index);
				break;
			case object_traits<std::vector<uint8_t>>::type_id:
				value = field_cast<std::vector<uint8_t>>(index);
				break;
			case object_traits<bool>::array_type_id:
				value = field_cast<std::vector<bool>>(index);
				break;
			case object_traits<char>::array_type_id:
				value = field_cast<std::vector<char>>(index);
				break;
			case object_traits<float>::array_type_id:
				value = field_cast<std::vector<float>>(index);
				break;
			case object_traits<double>::array_type_id:
				value = field_cast<std::vector<double>>(index);
				break;
			case object_traits<int16_t>::array_type_id:
				value = field_cast<std::vector<int16_t>>(index);
				break;
			case object_traits<int32_t>::array_type_id:
				value = field_cast<std::vector<int32_t>>(index);
				break;
			case object_traits<int64_t>::array_type_id:
				value = field_cast<std::vector<int64_t>>(index);
				break;
			case object_traits<Oid>::array_type_id:
				value = field_cast<std::vector<Oid>>(index);
				break;
			case object_traits<std::string>::array_type_id:
				value = field_cast<std::vector<std::string>>(index);
				break;
			case object_traits<timestamp>::array_type_id:
				value = field_cast<std::vector<timestamp>>(index);
				break;
			case object_traits<interval>::array_type_id:
				value = field_cast<std::vector<interval>>(index);
				break;
			case object_traits<date>::array_type_id:
				value = field_cast<std::vector<date>>(index);
				break;
			default:
				throw postgres::error("Unsupported field type");
			}
		}
	}

#endif // C++17

protected:
	PGconn* m_conn;
	result m_res;
	std::string _name;
	std::vector<binder> m_binders;

	template<ExecStatusType... Excepted>
	void verify_error()
	{
		if (m_res)
			m_res.verify_error<Excepted...>();
		else
			throw error(m_conn);
	}
	void finish(result& res)
	{
		while (res)
		{
			res = PQgetResult(m_conn);
		}
	}

	template<typename T>
	T field_cast(size_t index)
	{
		T v;
		m_binders[index].get(v);
		return v;
	}
};

class statement : public base_statement
{
public:
	explicit statement(base_database& db) : base_statement(db) 
	{
	}
	statement(const statement&) = delete;
	statement(statement&& src) : base_statement(std::move(src))
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
		verify_error<PGRES_COMMAND_OK, PGRES_SINGLE_TUPLE, PGRES_TUPLES_OK>();
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
};

class base_database
{
protected:
	base_database()
	{
		m_conn = nullptr;
	}

public:
	typedef postgres::error exception_type;

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

	base_database& operator=(const base_database&) = delete;
	base_database& operator=(base_database&& src)
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

	void encoding(const char* encoding)
	{
		if (PQsetClientEncoding(m_conn, encoding))
			throw error(m_conn);
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

	void close()
	{
		PQfinish(m_conn);
		m_conn = nullptr;
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
				qtl::detail::apply(proc, std::forward<decltype(values)>(values));
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

	bool is_alive()
	{
		qtl::postgres::result res(PQexec(m_conn, ""));
		return res && res.status() == PGRES_COMMAND_OK;
	}

};

inline int event_flags(PostgresPollingStatusType status)
{
	int flags = 0;
	if (status == PGRES_POLLING_READING)
		flags |= event::ef_read;
	else if (status == PGRES_POLLING_WRITING)
		flags |= event::ef_write;
	else if (status == PGRES_POLLING_FAILED)
		flags |= event::ef_exception;
	return flags;
}

class async_connection;

template<typename Handler>
inline void async_wait(qtl::event* event, PGconn* conn, int timeout, Handler&& handler)
{
	int flushed = PQflush(conn);
	if (flushed < 0)
	{
		handler(error(conn));
		return;
	}
	if (flushed == 1)
	{
		event->set_io_handler(qtl::event::ef_read | qtl::event::ef_write, timeout,
			[event, conn, timeout, handler](int flags) mutable {
			if (flags&qtl::event::ef_timeout)
			{
				handler(postgres::timeout());
				return;
			}
			if (flags&qtl::event::ef_read)
			{
				if (!PQconsumeInput(conn))
				{
					handler(error(conn));
					return;
				}
			}
			if (flags&(qtl::event::ef_read | qtl::event::ef_write | event::ef_exception))
				async_wait(event, conn, timeout, handler);
		});
	}
	else
	{
		event->set_io_handler(qtl::event::ef_read, 10,
			[event, conn, timeout, handler](int flags) mutable {
			if (flags&qtl::event::ef_timeout)
			{
				handler(postgres::timeout());
			}
			else if (flags&(qtl::event::ef_read | qtl::event::ef_exception))
			{
				if (PQconsumeInput(conn))
				{
					if (!PQisBusy(conn))
						handler(postgres::error());
					else
						async_wait(event, conn, timeout, handler);
				}
				else
				{
					handler(postgres::error(conn));
				}
			}
			else
			{
				handler(postgres::error(conn));
			}
		});
	}
}

class async_statement : public base_statement
{
public:
	async_statement(async_connection& db);
	async_statement(async_statement&& src)
		: base_statement(std::move(src)), m_timeout(2)
	{
		m_event = src.m_event;
		m_timeout = src.m_timeout;
		src.m_event = nullptr;
	}
	async_statement& operator=(async_statement&& src)
	{
		if (this != &src)
		{
			base_statement::operator =(std::move(src));
			m_event = src.m_event;
			m_timeout = src.m_timeout;
			src.m_event = nullptr;
		}
		return *this;
	}
	~async_statement()
	{
		close();
	}

	/*
		Handler defiens as:
		void handler(const qtl::mysql::error& e);
	 */
	template<typename Handler>
	void open(Handler&& handler, const char* command, int nParams = 0, const Oid *paramTypes = nullptr)
	{
		_name.resize(sizeof(intptr_t) * 2 + 1);
		int n = sprintf(const_cast<char*>(_name.data()), "q%p", this);
		_name.resize(n);
		std::transform(_name.begin(), _name.end(), _name.begin(), tolower);
		if (PQsendPrepare(m_conn, _name.data(), command, nParams, paramTypes))
		{
			async_wait([this, handler](error e) mutable {
				if (!e)
				{
					m_res = PQgetResult(m_conn);
					if (m_res)
					{
						m_res.verify_error<PGRES_COMMAND_OK>(e);
						while(m_res)
							m_res = PQgetResult(m_conn);
					}
				}
				handler(e);
			});
		}
		else
		{
			_name.clear();
			handler(error(m_conn));
		}
	}

	template<typename Handler, typename... Types>
	void open(Handler&& handler, const char* command)
	{
		auto binder_list = make_binder_list(Types()...);
		std::array<Oid, sizeof...(Types)> types;
		std::transform(binder_list.begin(), binder_list.end(), types.begin(), [](const binder& b) {
			return b.type();
		});

		open(std::forward<Handler>(handler), command, types.size(), types.data());
	}

	void close()
	{
		while (m_res)
		{
			m_res = PQgetResult(m_conn);
		}

		if (!_name.empty())
		{
			std::ostringstream oss;
			oss << "DEALLOCATE " << _name << ";";
			result res = PQexec(m_conn, oss.str().data());
			error e;
			res.verify_error<PGRES_COMMAND_OK, PGRES_TUPLES_OK>(e);
			finish(res);
			if(e) throw e;
		}
		base_statement::close();
	}

	template<typename Handler>
	void close(Handler&& handler)
	{
		while (m_res)
		{
			if(PQisBusy(m_conn))
			{
				async_wait([this, handler](const error& e) mutable {
					close(handler);
				});
			}
			else
			{
				m_res = PQgetResult(m_conn);
			}
		}

		if (!_name.empty() && PQstatus(m_conn) == CONNECTION_OK)
		{
			std::ostringstream oss;
			oss << "DEALLOCATE " << _name << ";";
			bool ok = PQsendQuery(m_conn, oss.str().data());
			if (ok)
			{
				async_wait([this, handler](postgres::error e) mutable {
					if (PQstatus(m_conn) == CONNECTION_OK)
					{
						result res(PQgetResult(m_conn));
						if (res)
							res.verify_error<PGRES_COMMAND_OK, PGRES_TUPLES_OK>(e);
						if (!e) _name.clear();
						finish(res);
						handler(e);
					}
					else
					{
						_name.clear();
						handler(error());
					}
				});
			}
			else
			{
				handler(error(m_conn));
			}
		}
		else
		{
			_name.clear();
		}
	}

	/*
		ExecuteHandler defiens as:
		void handler(const qtl::mysql::error& e, uint64_t affected);
	 */
	template<typename ExecuteHandler>
	void execute(ExecuteHandler&& handler)
	{
		if (PQsendQueryPrepared(m_conn, _name.data(), 0, nullptr, nullptr, nullptr, 1) &&
			PQsetSingleRowMode(m_conn))
		{
			async_wait([this, handler](error e) {
				if (!e)
				{
					m_res = PQgetResult(m_conn);
					m_res.verify_error<PGRES_COMMAND_OK, PGRES_SINGLE_TUPLE>(e);
					finish(m_res);
				}
				handler(e);
			});
		}
		else
		{
			handler(error(m_conn));
		}
	}

	template<typename Types, typename Handler>
	void execute(const Types& params, Handler&& handler)
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
			{
				handler(error(m_conn), 0);
				return;
			}
		}
		else
		{
			if (!PQsendQueryPrepared(m_conn, _name.data(), 0, nullptr, nullptr, nullptr, 1))
			{
				handler(error(m_conn), 0);
				return;
			}
		}
		if (!PQsetSingleRowMode(m_conn))
		{
			handler(error(m_conn), 0);
			return;
		}
		if (PQisBusy(m_conn))
		{
			async_wait([this, handler](error e) mutable {
				if (!e)
				{
					m_res = PQgetResult(m_conn);
					m_res.verify_error<PGRES_COMMAND_OK, PGRES_SINGLE_TUPLE>(e);
					int64_t affected = m_res.affected_rows();
					finish(m_res);
					handler(e, affected);
				}
				else
				{
					handler(e, 0);
				}
			});
		}
	}

	template<typename Types, typename RowHandler, typename FinishHandler>
	void fetch(Types&& values, RowHandler&& row_handler, FinishHandler&& finish_handler)
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
						m_binders[i] = binder(m_res.get_value(0, i), m_res.length(0, i),
							m_res.get_column_type(i));
					}
					qtl::bind_record(*this, std::forward<Types>(values));
				}
				row_handler();
				if (PQisBusy(m_conn))
				{
					async_wait([this, &values, row_handler, finish_handler](const error& e) {
						if (e)
						{
							finish_handler(e);
						}
						else
						{
							m_res = PQgetResult(m_conn);
							fetch(std::forward<Types>(values), row_handler, finish_handler);
						}
					});
				}
				else
				{
					m_res = PQgetResult(m_conn);
					fetch(std::forward<Types>(values), row_handler, finish_handler);
				}
			}
			else
			{
				error e;
				m_res.verify_error<PGRES_TUPLES_OK>(e);
				finish_handler(e);
			}
		}
		else
		{
			finish_handler(error());
		}
	}

	template<typename Handler>
	void next_result(Handler&& handler)
	{
		async_wait([this, handler](const error& e) {
			if (e)
			{
				handler(e);
			}
			else
			{
				m_res = PQgetResult(m_conn);
				handler(error());
			}
		});
	}

private:
	event* m_event;
	int m_timeout;
	template<typename Handler>
	void async_wait(Handler&& handler)
	{
		qtl::postgres::async_wait(m_event, m_conn, m_timeout, std::forward<Handler>(handler));
	}
};

class async_connection : public base_database, public qtl::async_connection<async_connection, async_statement>
{
public:
	async_connection() : m_connect_timeout(2), m_query_timeout(2)
	{
	}
	async_connection(async_connection&& src)
		: base_database(std::move(src)), m_connect_timeout(src.m_connect_timeout), m_query_timeout(src.m_query_timeout)
	{
	}
	async_connection& operator=(async_connection&& src)
	{
		if (this != &src)
		{
			base_database::operator=(std::move(src));
			m_connect_timeout = src.m_connect_timeout;
			m_query_timeout = src.m_query_timeout;
		}
		return *this;
	}

	/*
		OpenHandler defines as:
			void handler(const qtl::postgres::error& e) NOEXCEPT;
	*/
	template<typename EventLoop, typename OpenHandler>
	void open(EventLoop& ev, OpenHandler&& handler, const std::map<std::string, std::string>& params, bool expand_dbname = false)
	{
		std::vector<const char*> keywords;
		std::vector<const char*> values;
		keywords.reserve(params.size());
		values.reserve(params.size());
		for (auto& param : params)
		{
			keywords.push_back(param.first.data());
			values.push_back(param.second.data());
		}
		keywords.push_back(nullptr);
		values.push_back(nullptr);
		m_conn = PQconnectStartParams(keywords.data(), values.data(), expand_dbname);
		if (m_conn == nullptr)
			throw std::bad_alloc();
		if (status() == CONNECTION_BAD)
		{
			handler(error(m_conn));
			return;
		}

		if (PQsetnonblocking(m_conn, true)!=0)
			handler(error(m_conn));
		get_options();
		bind(ev);
		wait_connect(std::forward<OpenHandler>(handler));
	}

	template<typename EventLoop, typename OpenHandler>
	void open(EventLoop& ev, OpenHandler&& handler, const char * conninfo)
	{
		m_conn = PQconnectStart(conninfo);
		if (m_conn == nullptr)
			throw std::bad_alloc();
		if (status() == CONNECTION_BAD)
		{
			handler(error(m_conn));
			return;
		}

		PQsetnonblocking(m_conn, true);
		get_options();
		bind(ev);
		wait_connect(std::forward<OpenHandler>(handler));
	}

	template<typename OpenHandler>
	void reset(OpenHandler&& handler)
	{
		PQresetStart(m_conn);
		wait_reset(std::forward<OpenHandler>(handler));
	}

	/*
		Handler defines as:
			void handler(const qtl::mysql::error& e, uint64_t affected) NOEXCEPT;
	*/
	template<typename ExecuteHandler>
	void simple_execute(ExecuteHandler&& handler, const char* query_text) NOEXCEPT
	{
		bool ok = PQsendQuery(m_conn, query_text);
		if (ok)
		{
			async_wait([this, handler](postgres::error e) mutable {
				result res(PQgetResult(m_conn));
				res.verify_error<PGRES_COMMAND_OK, PGRES_TUPLES_OK>(e);
				uint64_t affected = res.affected_rows();
				handler(e, affected);
				while (res)
					res = PQgetResult(m_conn);
			});
		}
		else
		{
			handler(error(m_conn), 0);
		}
	}

	template<typename Handler>
	void auto_commit(Handler&& handler, bool on) NOEXCEPT
	{
		simple_execute(std::forward<Handler>(handler),
			on ? "SET AUTOCOMMIT TO ON" : "SET AUTOCOMMIT TO OFF");
	}

	template<typename Handler>
	void begin_transaction(Handler&& handler) NOEXCEPT
	{
		simple_execute(std::forward<Handler>(handler), "BEGIN");
	}

	template<typename Handler>
	void rollback(Handler&& handler) NOEXCEPT
	{
		simple_execute(std::forward<Handler>(handler), "ROLLBACK");
	}

	template<typename Handler>
	void commit(Handler&& handler) NOEXCEPT
	{
		simple_execute(std::forward<Handler>(handler), "COMMIT");
	}

	/*
		ResultHandler defines as:
			void result_handler(const qtl::postgres::error& e) NOEXCEPT;
	*/
	template<typename RowHandler, typename ResultHandler>
	void simple_query(const char* query, RowHandler&& row_handler, ResultHandler&& result_handler) NOEXCEPT
	{
		bool ok = PQsendQuery(m_conn, query);
		if (ok)
		{
			async_wait([this, row_handler, result_handler](postgres::error e) mutable {
				result res(PQgetResult(m_conn));
				res.verify_error<PGRES_COMMAND_OK, PGRES_TUPLES_OK>(e);
				if (e)
				{
					result_handler(e, 0);
					return;
				}
				uint64_t affected = res.affected_rows();
				while (res && res.status() == PGRES_TUPLES_OK)
				{
					simple_statment stmt(*this, std::move(res));
					stmt.fetch_all(row_handler);
					res = PQgetResult(m_conn);
				}
				result_handler(e, affected);
			});
		}
		else
		{
			result_handler(error(m_conn), 0);
		}
	}

	template<typename Handler>
	void open_command(const char* query_text, size_t /*text_length*/, Handler&& handler)
	{
		std::shared_ptr<async_statement> stmt = std::make_shared<async_statement>(*this);
		stmt->open([stmt, handler](const postgres::error& e) mutable {
			handler(e, stmt);
		}, query_text, 0);
	}

	template<typename Handler>
	void is_alive(Handler&& handler) NOEXCEPT
	{
		simple_execute(std::forward<Handler>(handler), "");
	}

	socket_type socket() const NOEXCEPT { return PQsocket(m_conn); }

	int connect_timeout() const { return m_connect_timeout; }
	void connect_timeout(int timeout) { m_connect_timeout = timeout; }
	int query_timeout() const { return m_query_timeout; }
	void query_timeout(int timeout) { m_query_timeout = timeout; }

private:
	int m_connect_timeout;
	int m_query_timeout;

	void get_options()
	{
		PQconninfoOption* options = PQconninfo(m_conn);
		m_connect_timeout = 2;
		for (PQconninfoOption* option = options; option; option++)
		{
			if (strcmp(option->keyword, "connect_timeout") == 0)
			{
				if (option->val)
					m_connect_timeout = atoi(option->val);
				break;
			}
		}
		PQconninfoFree(options);
	}

	template<typename OpenHandler>
	void wait_connect(OpenHandler&& handler) NOEXCEPT
	{
		PostgresPollingStatusType status = PQconnectPoll(m_conn);
		switch (status)
		{
		case PGRES_POLLING_READING:
		case PGRES_POLLING_WRITING:
			m_event_handler->set_io_handler(event_flags(status), m_connect_timeout,
				[this, handler](int flags) mutable {
				if (flags&event::ef_timeout)
				{
					handler(postgres::timeout());
				}
				else if(flags&(event::ef_read|event::ef_write | event::ef_exception))
					wait_connect(std::forward<OpenHandler>(handler));
			});
			break;
		case PGRES_POLLING_FAILED:
			handler(postgres::error(handle()));
			break;
		case PGRES_POLLING_OK:
			//PQsetnonblocking(m_conn, true);
			handler(postgres::error());
		}
	}

	template<typename OpenHandler>
	void wait_reset(OpenHandler&& handler) NOEXCEPT
	{
		PostgresPollingStatusType status = PQresetPoll(m_conn);
		switch (status)
		{
		case PGRES_POLLING_READING:
		case PGRES_POLLING_WRITING:
			m_event_handler->set_io_handler(event_flags(status), m_connect_timeout,
				[this, handler](int flags) mutable {
				if (flags&event::ef_timeout)
				{
					handler(postgres::timeout());
				}
				else if (flags&(event::ef_read | event::ef_write | event::ef_exception))
					wait_reset(std::forward<OpenHandler>(handler));
			});
			break;
		case PGRES_POLLING_FAILED:
			handler(postgres::error(m_conn));
			break;
		case PGRES_POLLING_OK:
			handler(postgres::error());
		}
	}

	template<typename Handler>
	void async_wait(Handler&& handler)
	{
		qtl::postgres::async_wait(event(), m_conn, m_query_timeout, std::forward<Handler>(handler));
	}

};

inline async_statement::async_statement(async_connection& db)
	: base_statement(static_cast<base_database&>(db))
{
	m_event = db.event();
	m_timeout = db.query_timeout();
}


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

