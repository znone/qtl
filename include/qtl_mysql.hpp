#ifndef _QTL_MYSQL_H_
#define _QTL_MYSQL_H_

#include <mysql.h>
#include <errmsg.h>

#include <time.h>
#include <memory.h>
#include <assert.h>
#include <stdint.h>
#include <string>
#include <vector>
#include <array>
#include <utility>
#include <functional>
#include <algorithm>
#include <system_error>
#include "qtl_common.hpp"
#include "qtl_async.hpp"

#if LIBMYSQL_VERSION_ID >=80000
typedef bool my_bool;
#endif //MySQL 8

#ifdef MARIADB_VERSION_ID
#define IS_LONGDATA(t) ((t) >= MYSQL_TYPE_TINY_BLOB && (t) <= MYSQL_TYPE_STRING)
#endif //MariaDB

namespace qtl
{

namespace mysql
{

struct init
{
	init(int argc=-1, char **argv=nullptr, char **groups=nullptr) 
	{
		//my_init();
		mysql_library_init(argc, argv, groups);
	}
	~init()
	{
		mysql_library_end();
	}
};

struct thread_init
{
	thread_init()
	{
		mysql_thread_init();
	}
	~thread_init()
	{
		mysql_thread_end();
	}
};

class binder : public MYSQL_BIND
{
	friend class statement;
public:
	binder()
	{
		init();
	}
	void init()
	{
		memset(this, 0, sizeof(MYSQL_BIND));
	}
	void bind()
	{
		init();
		buffer_type=MYSQL_TYPE_NULL;
	}
	void bind(null)
	{
		bind();
	}
	void bind(std::nullptr_t)
	{
		bind();
	}
	void bind(bool& v)
	{
		init();
		buffer_type = MYSQL_TYPE_BIT;
		buffer = &v;
		buffer_length = 1;
	}
	void bind(int8_t& v)
	{
		init();
		buffer_type=MYSQL_TYPE_TINY;
		buffer=&v;
	}
	void bind(uint8_t& v)
	{
		init();
		buffer_type=MYSQL_TYPE_TINY;
		buffer=&v;
		is_unsigned=true;
	}
	void bind(int16_t& v)
	{
		init();
		buffer_type=MYSQL_TYPE_SHORT;
		buffer=&v;
	}
	void bind(uint16_t& v)
	{
		init();
		buffer_type=MYSQL_TYPE_SHORT;
		buffer=&v;
		is_unsigned=true;
	}
	void bind(int32_t& v)
	{
		init();
		buffer_type=MYSQL_TYPE_LONG;
		buffer=&v;
	}
	void bind(uint32_t& v)
	{
		init();
		buffer_type=MYSQL_TYPE_LONG;
		buffer=&v;
		is_unsigned=true;
	}
	void bind(int64_t& v)
	{
		init();
		buffer_type=MYSQL_TYPE_LONGLONG;
		buffer=&v;
	}
	void bind(uint64_t& v)
	{
		init();
		buffer_type=MYSQL_TYPE_LONGLONG;
		buffer=&v;
		is_unsigned=true;
	}
	void bind(double& v)
	{
		init();
		buffer_type=MYSQL_TYPE_DOUBLE;
		buffer=&v;
	}
	void bind(float& v)
	{
		init();
		buffer_type=MYSQL_TYPE_FLOAT;
		buffer=&v;
	}
	void bind(MYSQL_TIME& v, enum_field_types type=MYSQL_TYPE_TIMESTAMP)
	{
		init();
		buffer_type=type;
		buffer=&v;
	}
	void bind(void* data, unsigned long length, enum_field_types type=MYSQL_TYPE_BLOB)
	{
		init();
		buffer_type=type;
		buffer=data;
		buffer_length=length;
	}
	void bind(const const_blob_data& data, enum_field_types type=MYSQL_TYPE_BLOB)
	{
		init();
		buffer_type=type;
		buffer=const_cast<void*>(data.data);
		buffer_length=data.size;
	}
	void bind(blob_data& data, enum_field_types type=MYSQL_TYPE_BLOB)
	{
		init();
		buffer_type=type;
		buffer=data.data;
		buffer_length=data.size;
	}
};


template<typename T>
inline void bind(binder& binder, const T& v)
{
	binder.bind(const_cast<T&>(v));
}

template<typename T>
inline void bind(binder& binder, T&& v)
{
	binder.bind(v);
}

inline void bind(binder& binder, const char* str, size_t length=0)
{
	if(length==0) length=strlen(str);
	binder.bind(const_cast<char*>(str), static_cast<unsigned long>(length), MYSQL_TYPE_VAR_STRING);
}

class base_statement;
class basic_database;

class error : public std::exception
{
public:
	error() : m_error(0) { }
	error(unsigned int err, const char* errmsg) : m_error(err), m_errmsg(errmsg) { }
	explicit error(const base_statement& stmt);
	explicit error(const basic_database& db);
	error(const error& src) = default;
	virtual ~error() throw() { }
	int code() const throw() { return m_error; }
	operator bool() const { return m_error != 0;  }
	virtual const char* what() const NOEXCEPT override { return m_errmsg.data(); }
private:
	unsigned int m_error;
	std::string m_errmsg;
};

class blobbuf : public qtl::blobbuf
{
public:
	blobbuf() : m_stmt(nullptr), m_field(0) 
	{
	}
	blobbuf(const blobbuf&) = default;
	blobbuf& operator=(const blobbuf&) = default;
	virtual ~blobbuf() { overflow();  }

	void open(MYSQL_STMT* stmt, int field, const binder& b, std::ios_base::openmode mode)
	{
		if (m_stmt && m_field)
		{
			overflow();
		}

		assert(stmt != nullptr);
		m_stmt = stmt;
		m_field = field;
		m_binder = b;
		if (b.length) m_size = *b.length;
		init_buffer(mode);
	}

	void swap(blobbuf& other)
	{
		std::swap(m_stmt, other.m_stmt);
		std::swap(m_binder, other.m_binder);
		std::swap(m_field, other.m_field);
		qtl::blobbuf::swap(other);
	}

private:
	MYSQL_STMT* m_stmt;
	binder m_binder;
	int m_field;

protected:
	virtual bool read_blob(char* buffer, off_type& count, pos_type position) override
	{
		m_binder.buffer = buffer;
		m_binder.buffer_length = count;
		int ret = mysql_stmt_fetch_column(m_stmt, &m_binder, m_field, position);
		switch (ret)
		{
		case 0:
			count = std::min(m_binder.buffer_length, *m_binder.length);
			return true;
		case CR_NO_DATA:
			return false;
		default:
			throw error(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
		}
	}

	virtual void write_blob(const char* buffer, size_t count) override
	{
		int ret = mysql_stmt_send_long_data(m_stmt, m_field, buffer, count);
		if (ret != 0)
			throw error(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
	}
};

struct time : public MYSQL_TIME
{
	time()
	{
		memset(this, 0, sizeof(MYSQL_TIME));
		time_type = MYSQL_TIMESTAMP_NONE;
	}
	time(const struct tm& tm)
	{
		memset(this, 0, sizeof(MYSQL_TIME));
		year = tm.tm_year + 1900;
		month = tm.tm_mon + 1;
		day = tm.tm_mday;
		hour = tm.tm_hour;
		minute = tm.tm_min;
		second = tm.tm_sec;
		time_type = MYSQL_TIMESTAMP_DATETIME;
	}
	time(time_t value)
	{
		struct tm tm;
#if defined(_MSC_VER)
		localtime_s(&tm, &value);
#elif defined(_POSIX_VERSION)
		localtime_r(&value, &tm);
#else
		tm = *localtime(&value);
#endif
		new(this)time(tm);
	}
	time(const time& src)
	{
		memcpy(this, &src, sizeof(MYSQL_TIME));
	}
	time& operator=(const time& src)
	{
		if (this != &src)
			memcpy(this, &src, sizeof(MYSQL_TIME));
		return *this;
	}

	static time now()
	{
		time_t value;
		::time(&value);
		return time(value);
	}

	time_t as_tm(struct tm& tm) const
	{
		tm.tm_year = year - 1900;
		tm.tm_mon = month - 1;
		tm.tm_mday = day;
		tm.tm_hour = hour;
		tm.tm_min = minute;
		tm.tm_sec = second;
		return mktime(&tm);
	}
	time_t get_time() const
	{
		struct tm tm;
		return as_tm(tm);
	}
};

class base_statement
{
protected:
	base_statement() : m_stmt(nullptr), m_result(nullptr) {}
	base_statement(const base_statement&) = delete;
	explicit base_statement(basic_database& db);
	base_statement(base_statement&& src)
		: m_stmt(src.m_stmt), m_result(src.m_result),
		m_binders(std::move(src.m_binders)), m_binderAddins(std::move(src.m_binderAddins))
	{
		src.m_stmt=nullptr;
		src.m_result=nullptr;
	}
	base_statement& operator=(const base_statement&) = delete;
	base_statement& operator=(base_statement&& src)
	{
		if(this!=&src)
		{
			m_stmt=src.m_stmt;
			m_result=src.m_result;
			src.m_stmt=nullptr;
			src.m_result=nullptr;
			m_binders=std::move(src.m_binders);
			m_binderAddins=std::move(src.m_binderAddins);
		}
		return *this;
	}

public:
	operator MYSQL_STMT*() { return m_stmt; }

	unsigned int get_parameter_count() const { return mysql_stmt_param_count(m_stmt); }
	unsigned int get_column_count() const { return mysql_stmt_field_count(m_stmt); }

	unsigned long length(unsigned int index) const
	{
		return m_binderAddins[index].m_length;
	}

	bool is_null(unsigned int index) const
	{
		return m_binderAddins[index].m_isNull!=0;
	}

	size_t find_field(const char* name) const
	{
		if(m_result)
		{
			for(size_t i=0; i!=m_result->field_count; i++)
			{
				if(strncmp(m_result->fields[i].name, name, m_result->fields[i].name_length)==0)
					return i;
			}
		}
		return -1;
	}

	my_ulonglong affetced_rows()
	{
		return mysql_stmt_affected_rows(m_stmt);
	}
	my_ulonglong insert_id()
	{
		return mysql_stmt_insert_id(m_stmt);
	}

	binder* get_binder(unsigned long index)
	{
		return &m_binders[index];
	}

	unsigned int error() const
	{
		return mysql_stmt_errno(m_stmt);
	}
	const char* errmsg() const
	{
		return mysql_stmt_error(m_stmt);
	}

	MYSQL_RES* result() { return m_result; }

	void bind_param(size_t index, const char* param, size_t length)
	{
		bind(m_binders[index], param, length);
	}

	void bind_param(size_t index, const std::nullptr_t&)
	{
		m_binders[index].bind();
	}

	void bind_param(size_t index, std::istream& param)
	{
		m_binders[index].bind(nullptr, 0, MYSQL_TYPE_LONG_BLOB);
		m_binderAddins[index].m_after_fetch = [this, index, &param](const binder&) {
			std::array<char, blob_buffer_size> buffer;
			unsigned long readed = 0;
			while (!param.eof() && !param.fail())
			{
				param.read(buffer.data(), buffer.size());
				readed = (unsigned long)param.gcount();
				if (readed > 0)
				{
					if (mysql_stmt_send_long_data(m_stmt, index, buffer.data(), readed) != 0)
						throw_exception();
				}
			}
		};
	}

	void bind_param(size_t index, const blob_writer& param)
	{
		m_binders[index].bind(nullptr, 0, MYSQL_TYPE_LONG_BLOB);
		m_binderAddins[index].m_after_fetch = [this, index, &param](const binder& b) {
			blobbuf buf;
			buf.open(m_stmt, index, b, std::ios::out);
			std::ostream s(&buf);
			param(s);
		};
	}

	template<class Param>
	void bind_param(size_t index, const Param& param)
	{
		bind(m_binders[index], param);
	}

	template<class Type>
	void bind_field(size_t index, Type&& value)
	{
		if (m_result)
		{
			bind(m_binders[index], std::forward<Type>(value));
			m_binderAddins[index].m_after_fetch = 
				if_null<typename std::remove_reference<Type>::type>(value);
		}
	}

	void bind_field(size_t index, char* value, size_t length)
	{
		m_binders[index].bind(value, length - 1, MYSQL_TYPE_VAR_STRING);
		m_binderAddins[index].m_after_fetch = [](const binder& bind) {
			if (*bind.is_null)
				memset(bind.buffer, 0, bind.buffer_length + 1);
			else
			{
				char* text = reinterpret_cast<char*>(bind.buffer);
				text[*bind.length] = '\0';
			}
		};
	}

	template<size_t N>
	void bind_field(size_t index, std::array<char, N>&& value)
	{
		bind_field(index, value.data(), value.size());
	}

	template<typename T>
	void bind_field(size_t index, bind_string_helper<T>&& value)
	{
		if (m_result)
		{
			MYSQL_FIELD* field = mysql_fetch_field_direct(m_result, (unsigned int)index);
			if (field == nullptr) throw_exception();
			value.clear();
			typename bind_string_helper<T>::char_type* data = value.alloc(field->length);
			m_binderAddins[index].m_before_fetch = [this, value](binder& b) mutable {
				if (value.size() < b.buffer_length)
				{
					value.alloc(b.buffer_length);
					if (b.buffer != value.data())
					{
						b.buffer = const_cast<char*>(value.data());
						mysql_stmt_bind_result(m_stmt, &m_binders.front());
					}
				}
			};
			m_binderAddins[index].m_after_fetch = [value](const binder& b) mutable {
				if (*b.is_null) value.clear();
				else value.truncate(*b.length);
			};
			m_binders[index].bind((void*)data, field->length, field->type);
		}
	}

	void bind_field(size_t index, std::ostream&& value)
	{
		if (m_result)
		{
			MYSQL_FIELD* field = mysql_fetch_field_direct(m_result, (unsigned int)index);
			assert(IS_LONGDATA(field->type));
			m_binders[index].bind(nullptr, 0, field->type);
			m_binderAddins[index].m_after_fetch = [this, index, &value](const binder& b) {
				unsigned long readed = 0;
				std::array<char, blob_buffer_size> buffer;
				binder& bb = const_cast<binder&>(b);
				if (*b.is_null) return;
				bb.buffer = const_cast<char*>(buffer.data());
				bb.buffer_length = buffer.size();
				while (readed <= *b.length)
				{
					int ret = mysql_stmt_fetch_column(m_stmt, &bb, index, readed);
					if (ret != 0)
						throw_exception();
					value.write(buffer.data(), std::min(b.buffer_length, *b.length - b.offset));
					readed += bb.buffer_length;
				}
			};
		}
	}

	void bind_field(size_t index, blobbuf&& value)
	{
		if (m_result)
		{
			MYSQL_FIELD* field = mysql_fetch_field_direct(m_result, (unsigned int)index);
			assert(IS_LONGDATA(field->type));
			m_binders[index].bind(nullptr, 0, field->type);
			m_binderAddins[index].m_after_fetch = [this, index, &value](const binder& b) {
				if (*b.is_null) return;
				value.open(m_stmt, index, b, std::ios::in);
			};
		}
	}

	template<typename Type>
	void bind_field(size_t index, indicator<Type>&& value)
	{
		if (m_result)
		{
			qtl::bind_field(*this, index, value.data);
			binder_addin& addin = m_binderAddins[index];
			auto fetch_fun = addin.m_after_fetch;
			addin.m_after_fetch = [&addin, fetch_fun, &value](const binder& b) {
				value.is_null = *b.is_null != 0;
				value.length = *b.length;
				value.is_truncated = addin.is_truncated;
				if (fetch_fun) fetch_fun(b);
			};
		}
	}

#ifdef _QTL_ENABLE_CPP17

	template<typename T>
	inline void bind_field(size_t index, std::optional<T>&& value)
	{
		if (m_result)
		{
			qtl::bind_field(*this, index, *value);
			binder_addin& addin = m_binderAddins[index];
			auto fetch_fun = addin.m_after_fetch;
			addin.m_after_fetch = [&addin, fetch_fun, &value](const binder& b) {
				if (fetch_fun) fetch_fun(b);
				if (*b.is_null) value.reset();
			};
		}
	}

	inline void bind_field(size_t index, std::any&& value)
	{
		if (m_result)
		{
			MYSQL_FIELD* field = mysql_fetch_field_direct(m_result, (unsigned int)index);
			if (field == nullptr) throw_exception();
			switch (field->type)
			{
			case MYSQL_TYPE_NULL:
				value.reset();
				break;
			case MYSQL_TYPE_BIT:
				value.emplace<bool>();
				bind_field(index, std::any_cast<bool&>(value));
				break;
			case MYSQL_TYPE_TINY:
				value.emplace<int8_t>();
				bind_field(index, std::any_cast<int8_t&>(value));
				break;
			case MYSQL_TYPE_SHORT:
				value.emplace<int16_t>();
				bind_field(index, std::any_cast<int16_t&>(value));
				break;
			case MYSQL_TYPE_LONG:
				value.emplace<int32_t>();
				bind_field(index, std::any_cast<int32_t&>(value));
				break;
			case MYSQL_TYPE_LONGLONG:
				value.emplace<int64_t>();
				bind_field(index, std::any_cast<int64_t&>(value));
				break;
			case MYSQL_TYPE_FLOAT:
				value.emplace<float>();
				bind_field(index, std::any_cast<float&>(value));
				break;
			case MYSQL_TYPE_DOUBLE:
				value.emplace<double>();
				bind_field(index, std::any_cast<double&>(value));
				break;
			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_TIME:
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_TIMESTAMP:
			case MYSQL_TYPE_TIMESTAMP2:
			case MYSQL_TYPE_DATETIME2:
			case MYSQL_TYPE_TIME2:
				value.emplace<qtl::mysql::time>();
				bind_field(index, std::any_cast<qtl::mysql::time&>(value));
				break;
			case MYSQL_TYPE_VARCHAR:
			case MYSQL_TYPE_VAR_STRING:
			case MYSQL_TYPE_STRING:
			case MYSQL_TYPE_ENUM:
#if LIBMYSQL_VERSION_ID >= 50700
			case MYSQL_TYPE_JSON:
#endif 
			case MYSQL_TYPE_DECIMAL:
			case MYSQL_TYPE_NEWDECIMAL:
			case MYSQL_TYPE_GEOMETRY:
				value.emplace<std::string>();
				bind_field(index, qtl::bind_string(std::any_cast<std::string&>(value)));
				break;
			case MYSQL_TYPE_TINY_BLOB:
			case MYSQL_TYPE_MEDIUM_BLOB:
			case MYSQL_TYPE_BLOB:
			case MYSQL_TYPE_LONG_BLOB:
				value.emplace<blobbuf>();
				bind_field(index, std::forward<blobbuf>(std::any_cast<blobbuf&>(value)));
				break;
			default:
				throw mysql::error(CR_UNSUPPORTED_PARAM_TYPE, "Unsupported field type");
			}
			binder_addin& addin = m_binderAddins[index];
			auto fetch_fun = addin.m_after_fetch;
			addin.m_after_fetch = [&addin, fetch_fun, &value](const binder& b) {
				if (fetch_fun) fetch_fun(b);
				if (*b.is_null) value.reset();
			};
		}
	}

#endif // C++17

	void close()
	{
		if (m_result)
		{
			mysql_free_result(m_result);
			m_result = nullptr;
		}
		if (m_stmt)
		{
			mysql_stmt_close(m_stmt);
			m_stmt = nullptr;
		}
	}

protected:
	MYSQL_STMT* m_stmt;
	MYSQL_RES* m_result;
	std::vector<binder> m_binders;

	struct binder_addin
	{
		unsigned long m_length;
		my_bool m_isNull;
		my_bool m_error;
		bool is_truncated;
		std::function<void(binder&)> m_before_fetch;
		std::function<void(const binder&)> m_after_fetch;
	};
	std::vector<binder_addin> m_binderAddins;

	void resize_binders(size_t n)
	{
		m_binders.resize(n);
		m_binderAddins.resize(n);
	}
	void set_binders()
	{
		for(size_t i=0; i!=m_binders.size(); i++)
		{
			m_binderAddins[i].m_length=0;
			m_binders[i].length=&m_binderAddins[i].m_length;
			m_binderAddins[i].m_isNull=0;
			m_binders[i].is_null=&m_binderAddins[i].m_isNull;
			m_binderAddins[i].m_error=0;
			m_binders[i].error=&m_binderAddins[i].m_error;
		}
	}

	void throw_exception() const { throw mysql::error(*this); }

	template<typename Value>
	struct if_null
	{
		explicit if_null(Value& value, Value&& def=Value()) : m_value(value), m_def(std::move(def)) { }
		void operator()(const binder& b)
		{
			if(*b.is_null) m_value=m_def;
		}
		Value& m_value;
		Value m_def;
	};

};

class statement : public base_statement
{
public:
	statement() = default;
	explicit statement(basic_database& db) : base_statement(db) { }
	statement(statement&& src) : base_statement(std::move(src)) { }
	statement& operator=(statement&& src) 
	{
		base_statement::operator =(std::move(src));
		return *this;
	}
	~statement()
	{
		close();
	}

	void open(const char *query_text, unsigned long text_length=0)
	{
		mysql_stmt_reset(m_stmt);
		if(text_length==0) text_length=(unsigned long)strlen(query_text);
		if(mysql_stmt_prepare(m_stmt, query_text, text_length)!=0)
			throw_exception();
	}

	void execute()
	{
		resize_binders(0);
		if(mysql_stmt_execute(m_stmt)!=0)
			throw_exception();
	}

	template<typename BindProc>
	void execute_custom(BindProc&& bind_proc)
	{
		unsigned long count = mysql_stmt_param_count(m_stmt);
		if (count > 0)
		{
			resize_binders(count);
			bind_proc(*this);
			if (mysql_stmt_bind_param(m_stmt, &m_binders.front()))
				throw_exception();
			for (size_t i = 0; i != count; i++)
			{
				if (m_binderAddins[i].m_after_fetch)
					m_binderAddins[i].m_after_fetch(m_binders[i]);
			}
		}
		if (mysql_stmt_execute(m_stmt) != 0)
			throw_exception();
	}

	template<typename Types>
	void execute(const Types& params)
	{
		execute_custom([&params](statement& stmt) {
			qtl::bind_params(stmt, params);
		});
	}

	template<typename Types>
	bool fetch(Types&& values)
	{
		if(m_result==nullptr)
		{
			unsigned long count=mysql_stmt_field_count(m_stmt);
			if(count>0)
			{
				m_result=mysql_stmt_result_metadata(m_stmt);
				if(m_result==nullptr) throw_exception();
				resize_binders(count);
				qtl::bind_record(*this, std::forward<Types>(values));
				set_binders();
				if(mysql_stmt_bind_result(m_stmt, m_binders.data())!=0)
					throw_exception();
			}
		}
		return fetch();
	}

	bool fetch()
	{
		for (size_t i = 0; i != m_binders.size(); i++)
		{
			if (m_binderAddins[i].m_before_fetch)
				m_binderAddins[i].m_before_fetch(m_binders[i]);
		}
		int err=mysql_stmt_fetch(m_stmt);
		if(err==0 || err==MYSQL_DATA_TRUNCATED)
		{
			for(size_t i=0; i!=m_binders.size(); i++)
			{
				m_binderAddins[i].is_truncated = (err==MYSQL_DATA_TRUNCATED);
				if(m_binderAddins[i].m_after_fetch)
					m_binderAddins[i].m_after_fetch(m_binders[i]);
			}
			return true;
		}
		else if(err==1)
			throw_exception();
		return false;
	}

	bool next_result()
	{
		if(m_result)
		{
			mysql_free_result(m_result);
			m_result=nullptr;
			mysql_stmt_free_result(m_stmt);
		}
		int ret=0;
		do
		{
			ret=mysql_stmt_next_result(m_stmt);
			if(ret>0) throw_exception();
		}while(ret==0 && mysql_stmt_field_count(m_stmt)<=0);
		return ret==0;
	}

	bool reset() { return mysql_stmt_reset(m_stmt)!=0; }
};

/*
struct LocalInfile
{
	int read(char *buf, unsigned int buf_len);
	void close();
	int error(char *error_msg, unsigned int error_msg_len);
};
*/
template<typename LocalInfile>
struct local_infile_factory
{
	static int local_infile_init(void **ptr, const char *filename, void *userdata)
	{
		LocalInfile* object = nullptr;
		try
		{
			object = new LocalInfile(filename);
		}
		catch (...)
		{
			return -1;
		}
		return ptr ? 0 : -1;
	}

	static int local_infile_read(void *ptr, char *buf, unsigned int buf_len)
	{
		LocalInfile* object = reinterpret_cast<LocalInfile>(ptr);
		return object->read(buf, buf_len);
	}

	static void local_infile_end(void *ptr)
	{
		LocalInfile* object = reinterpret_cast<LocalInfile>(ptr);
		object->close();
		delete object;
	}

	static int local_infile_error(void *ptr, char *error_msg, unsigned int error_msg_len)
	{
		LocalInfile* object = reinterpret_cast<LocalInfile>(ptr);
		return object->error(error_msg, error_msg_len);
	}
};

class local_file
{
public:
	local_file(const char* filename)
	{
		m_fp = fopen(filename, "rb");
		if (m_fp == nullptr)
			throw std::system_error(std::make_error_code(std::errc(errno)));
	}
	int read(char *buf, unsigned int buf_len)
	{
		return fread(buf, 1, buf_len, m_fp);
	}
	void close()
	{
		fclose(m_fp);
	}
	int error(char *error_msg, unsigned int error_msg_len)
	{
		int errcode = errno;
		memset(error_msg, 0, error_msg_len);
		strncpy(error_msg, strerror(errcode), error_msg_len - 1);
		return errcode;
	}

private:
	FILE* m_fp;
};

class basic_database
{
protected:
	basic_database()
	{
		m_mysql = mysql_init(nullptr);
	}

public:
	typedef mysql::error exception_type;

	~basic_database()
	{
		if(m_mysql)
			mysql_close(m_mysql);
	}
	basic_database(const basic_database&) = delete;
	basic_database(basic_database&& src)
	{
		m_mysql=src.m_mysql;
		src.m_mysql=nullptr;
	}
	basic_database& operator==(const basic_database&) = delete;
	basic_database& operator==(basic_database&& src)
	{
		if(this!=&src)
		{
			if(m_mysql)
				mysql_close(m_mysql);
			m_mysql=src.m_mysql;
			src.m_mysql=nullptr;
		}
		return *this;
	}

	MYSQL* handle() { return m_mysql; }

	void options(enum mysql_option option, const void *arg)
	{
		if(mysql_options(m_mysql, option, arg)!=0)
		throw_exception();
	}
	void charset_name(const char* charset)
	{
		if(mysql_set_character_set(m_mysql, charset)!=0)
			throw_exception();
	}
	void protocol(mysql_protocol_type type)
	{
		return options(MYSQL_OPT_PROTOCOL, &type);
	}
	void reconnect(my_bool re)
	{
		return options(MYSQL_OPT_RECONNECT, &re);
	}

	const char* current() const
	{
		return m_mysql->db;
	}

	unsigned int error() const
	{
		return mysql_errno(m_mysql);
	}
	const char* errmsg() const
	{
		return mysql_error(m_mysql);
	}

	uint64_t affected_rows()
	{
		return mysql_affected_rows(m_mysql);
	}

	unsigned int field_count()
	{
		return mysql_field_count(m_mysql);
	}

	uint64_t insert_id()
	{
		return mysql_insert_id(m_mysql);
	}

protected:
	MYSQL* m_mysql;
	void throw_exception() { throw mysql::error(*this); }
};

#if MARIADB_VERSION_ID >= 050500

class async_connection;

#endif //MariaDB

class database : public basic_database, public qtl::base_database<database, statement>
{
public:
	database() = default;

	bool open(const char *host, const char *user, const char *passwd, const char *db,
		unsigned long clientflag = 0, unsigned int port = 0, const char *unix_socket = nullptr)
	{
		if (m_mysql == nullptr) m_mysql = mysql_init(nullptr);
		return mysql_real_connect(m_mysql, host, user, passwd, db, port, unix_socket, clientflag) != nullptr;
	}
	void close()
	{
		mysql_close(m_mysql);
		m_mysql = nullptr;
	}

	statement open_command(const char* query_text, size_t text_length)
	{
		statement stmt(*this);
		stmt.open(query_text, text_length);
		return stmt;
	}
	statement open_command(const char* query_text)
	{
		return open_command(query_text, strlen(query_text));
	}
	statement open_command(const std::string& query_text)
	{
		return open_command(query_text.data(), query_text.length());
	}

	void refresh(unsigned int options)
	{
		if (mysql_refresh(m_mysql, options) < 0)
			throw_exception();
	}

	void select(const char* db)
	{
		if (mysql_select_db(m_mysql, db) != 0)
			throw_exception();
	}

	void simple_execute(const char* query_text, uint64_t* paffected = nullptr)
	{
		if (mysql_query(m_mysql, query_text) != 0)
			throw_exception();
		if (paffected) *paffected = affected_rows();
	}
	void simple_execute(const char* query_text, unsigned long text_length, uint64_t* paffected = nullptr)
	{
		if (text_length == 0) text_length = (unsigned long)strlen(query_text);
		if (mysql_real_query(m_mysql, query_text, text_length) != 0)
			throw_exception();
		if (paffected) *paffected = affected_rows();
	}

	void auto_commit(bool on)
	{
		if (mysql_autocommit(m_mysql, on ? 1 : 0) != 0)
			throw_exception();
	}
	void begin_transaction()
	{
		auto_commit(false);
	}
	void rollback()
	{
		if (mysql_rollback(m_mysql) != 0)
			throw_exception();
		auto_commit(true);
	}
	void commit()
	{
		if (mysql_commit(m_mysql) != 0)
			throw_exception();
		auto_commit(true);
	}

	bool is_alive()
	{
		return mysql_ping(m_mysql) == 0;
	}

	template<typename Pred>
	bool simple_query(const char* query, unsigned long length, Pred&& pred)
	{
		simple_execute(query, length);

		unsigned int fieldCount = mysql_field_count(m_mysql);
		MYSQL_RES* result = mysql_store_result(m_mysql);
		if (fieldCount > 0 && result)
		{
			MYSQL_RES* result = mysql_store_result(m_mysql);
			MYSQL_ROW row;
			while (row = mysql_fetch_row(result))
			{
				if (!pred(*this, row, fieldCount))
					break;
			}
			mysql_free_result(result);
			return true;
		}
		return false;
	}

	template<typename LocalInfile>
	void set_local_infile_factory(local_infile_factory<LocalInfile>* factory)
	{
		typedef local_infile_factory<LocalInfile> factory_type;
		if (factory == nullptr)
		{
			reset_local_infile();
		}
		else
		{
			mysql_set_local_infile_handler(m_mysql, &factory_type::local_infile_init,
				&factory_type::local_infile_read,
				&factory_type::local_infile_end,
				&factory_type::local_infile_error, factory);
		}
	}
	void reset_local_infile()
	{
		mysql_set_local_infile_default(m_mysql);
	}

#if MARIADB_VERSION_ID >= 050500

	async_connection async_mode();

#endif //MariaDB

};

#if MARIADB_VERSION_ID >= 100000

inline int event_flags(int status) NOEXCEPT
{
	int flags = 0;
	if (status&MYSQL_WAIT_READ)
		flags |= event::ef_read;
	if (status&MYSQL_WAIT_WRITE)
		flags |= event::ef_write;
	if (status&MYSQL_WAIT_EXCEPT)
		flags |= event::ef_exception;
	return flags;
}

inline int mysql_status(int flags) NOEXCEPT
{
	int status = 0;
	if (flags&event::ef_read)
		status |= MYSQL_WAIT_READ;
	if (flags&event::ef_write)
		status |= MYSQL_WAIT_WRITE;
	if (flags&event::ef_exception)
		status |= MYSQL_WAIT_EXCEPT;
	if (flags&event::ef_timeout)
		status |= MYSQL_WAIT_TIMEOUT;
	return status;
}

class async_connection;

class async_statement : public base_statement
{
public:
	async_statement() = default;
	async_statement(async_connection& db);
	async_statement(async_statement&& src) 
		: base_statement(std::move(src))
	{
		m_event=src.m_event;
		src.m_event=nullptr;
	}
	async_statement& operator=(async_statement&& src)
	{
		base_statement::operator =(std::move(src));
		m_event=src.m_event;
		src.m_event=nullptr;
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
	void open(Handler&& handler, const char *query_text, size_t text_length=0)
	{
		if(text_length==0) text_length=strlen(query_text);
		if(m_stmt)
		{
			std::string text(query_text, text_length);
			reset([this, text, handler](const mysql::error& e) mutable {
				if(e)
				{
					handler(e);
					return;
				}

				prepare(text.data(), text.size(), std::forward<Handler>(handler));
			});
		}
		else
		{
			prepare(query_text, text_length, std::forward<Handler>(handler));
		}
	}

	/*
		ExecuteHandler defiens as:
		void handler(const qtl::mysql::error& e, uint64_t affected);
	 */
	template<typename ExecuteHandler>
	void execute(ExecuteHandler&& handler)
	{
		resize_binders(0);
		int ret=0;
		int status = mysql_stmt_execute_start(&ret, m_stmt);
		if(status)
			wait_execute(status, std::forward<ExecuteHandler>(handler));
		else if(ret)
			handler(mysql::error(*this), 0);
		else
			handler(mysql::error(), affetced_rows());
	}

	template<typename Types, typename Handler>
	void execute(const Types& params, Handler&& handler)
	{
		unsigned long count=mysql_stmt_param_count(m_stmt);
		if(count>0)
		{
			resize_binders(count);
			qtl::bind_params(*this, params);
			if(mysql_stmt_bind_param(m_stmt, &m_binders.front()))
				throw_exception();
			for(size_t i=0; i!=count; i++)
			{
				if(m_binderAddins[i].m_after_fetch)
					m_binderAddins[i].m_after_fetch(m_binders[i]);
			}
		}
		execute(std::forward<Handler>(handler));
	}

	template<typename Types, typename RowHandler, typename FinishHandler>
	void fetch(Types&& values, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		if(m_result==nullptr)
		{
			unsigned long count=mysql_stmt_field_count(m_stmt);
			if(count>0)
			{
				m_result=mysql_stmt_result_metadata(m_stmt);
				if(m_result==nullptr) throw_exception();
				resize_binders(count);
				qtl::bind_record(*this, std::forward<Types>(values));
				set_binders();
				if(mysql_stmt_bind_result(m_stmt, m_binders.data())!=0)
				{
					finish_handler(mysql::error(*this));
					return;
				}
			}
		}
		fetch(std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}

	template<typename RowHandler, typename FinishHandler>
	void fetch(RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		int ret = 0;
		int status = start_fetch(&ret);
		if (status == 0)
			status = after_fetch(status, ret, std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
		if (status)
			wait_fetch(status, std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
	}

	template<typename Handler>
	void reset(Handler&& handler)
	{
		my_bool ret=false;
		int status = mysql_stmt_reset_start(&ret, m_stmt);
		if(status)
			wait_operation<my_bool>(status, &mysql_stmt_reset_cont, handler);
		else
			handler( (ret) ? mysql::error(*this) : mysql::error());
	}

	void close()
	{
		base_statement::close();
	}

	template<typename Handler>
	void close(Handler&& handler)
	{
		if(m_result)
		{
			my_bool ret=0;
			int status = mysql_stmt_free_result_start(&ret, m_stmt);
			if(status)
			{
				this->wait_free_result(status, [this, handler](const mysql::error& e) mutable {
					m_result=nullptr;
					if (e)
						handler(e);
					else
						close(handler);
				});
			}
			else if(ret)
				handler(mysql::error(*this));
			else
				m_result=nullptr;
		}
		if(m_stmt)
		{
			my_bool ret=0;
			int status = mysql_stmt_close_start(&ret, m_stmt);
			if(status)
			{
				wait_operation<my_bool>(status, &mysql_stmt_close_cont, [this, handler](const mysql::error& e) mutable {
					m_stmt=nullptr;
					handler(e);
				});;
			}
			else
			{
				m_stmt = nullptr;
				handler((ret) ? mysql::error(*this) : mysql::error());
			}
		}
	}

	template<typename Handler>
	void next_result(Handler&& handler)
	{
		if(m_result)
		{
			mysql_free_result(m_result);
			m_result = nullptr;
		}
		int ret=0;
		do
		{
			int status = mysql_stmt_next_result_start(&ret, m_stmt);
			if (status)
			{
				wait_next_result(status, std::forward<Handler>(handler));
				return;
			}
		}while(ret==0 && mysql_stmt_field_count(m_stmt)<=0);
		handler((ret) ? mysql::error(*this) : mysql::error());
	}


private:
	event* m_event;

	template<typename Handler>
	void prepare(const char *query_text, size_t text_length, Handler&& handler)
	{
		int ret;
		int status=mysql_stmt_prepare_start(&ret, m_stmt, query_text, (unsigned long)text_length);
		if(status)
			wait_operation<int>(status, &mysql_stmt_prepare_cont, handler);
		else
			handler((ret) ? mysql::error(*this) : mysql::error());
	}

	template<typename Ret, typename Func, typename Handler>
	void wait_operation(int status, Func func, Handler&& handler)
	{
		m_event->set_io_handler(event_flags(status), mysql_get_timeout_value(m_stmt->mysql),
			[this, func, handler](int flags) mutable {
				Ret ret = 0;
				int status = func(&ret, m_stmt, mysql_status(flags));
				if (status)
					wait_operation<Ret>(status, func, handler);
				else
					handler((ret) ? mysql::error(*this) : mysql::error());
		});
	}

	template<typename ExecuteHandler>
	void wait_execute(int status, ExecuteHandler&& handler)
	{
		m_event->set_io_handler(event_flags(status), mysql_get_timeout_value(m_stmt->mysql),
			[this, handler](int flags) mutable {
				int ret = 0;
				int status = mysql_stmt_execute_cont(&ret, m_stmt, mysql_status(flags));
				if (status)
					wait_execute(status, handler);
				else if(ret)
					handler(mysql::error(*this), 0);
				else
					handler(mysql::error(), affetced_rows());
		});
	}

	template<typename RowHandler, typename FinishHandler>
	void wait_fetch(int status, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		m_event->set_io_handler(event_flags(status), mysql_get_timeout_value(m_stmt->mysql),
			[this, row_handler, finish_handler](int flags) mutable {
				int ret = 0;
				int status = mysql_stmt_fetch_cont(&ret, m_stmt, mysql_status(flags));
				if (status == 0)
					status = after_fetch(status, ret, std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
				if (status)
					wait_fetch(status, std::forward<RowHandler>(row_handler), std::forward<FinishHandler>(finish_handler));
		});
	}

	int start_fetch(int* ret)
	{
		for (size_t i = 0; i != m_binders.size(); i++)
		{
			if (m_binderAddins[i].m_before_fetch)
				m_binderAddins[i].m_before_fetch(m_binders[i]);
		}
		return mysql_stmt_fetch_start(ret, m_stmt);
	}

	template<typename RowHandler, typename FinishHandler>
	int after_fetch(int status, int ret, RowHandler&& row_handler, FinishHandler&& finish_handler)
	{
		while (status == 0)
		{
			if (ret == 0 || ret == MYSQL_DATA_TRUNCATED)
			{
				for (size_t i = 0; i != m_binders.size(); i++)
				{
					m_binderAddins[i].is_truncated = (ret == MYSQL_DATA_TRUNCATED);
					if (m_binderAddins[i].m_after_fetch)
						m_binderAddins[i].m_after_fetch(m_binders[i]);
				}
				if (row_handler())
					status = start_fetch(&ret);
				else
				{
					finish_handler(mysql::error());
					break;
				}
			}
			else if (ret == 1)
			{
				finish_handler(mysql::error(*this));
				break;
			}
			else
			{
				finish_handler(mysql::error());
				break;
			}
		};
		return status;
	}

	template<typename ResultHandler>
	void wait_free_result(int status, ResultHandler&& handler) NOEXCEPT
	{
		m_event->set_io_handler(event_flags(status), mysql_get_timeout_value(m_stmt->mysql),
			[this, handler](int flags) mutable {
				my_bool ret = false;
				int status = mysql_stmt_free_result_cont(&ret, m_stmt, mysql_status(flags));
				if (status)
					wait_free_result(status, handler);
				else
					handler(mysql::error());
		});
	}

	template<typename Handler>
	void wait_next_result(int status, Handler&& handler)
	{
		m_event->set_io_handler(event_flags(status), mysql_get_timeout_value(m_stmt->mysql),
			[this, handler](int flags) mutable {
			int ret = 0;
			int status = mysql_stmt_next_result_cont(&ret, m_stmt, mysql_status(flags));
			if (status)
				wait_next_result(status, handler);
			else if (ret)
				handler(mysql::error(*this));
			else if (mysql_stmt_field_count(m_stmt) > 0)
				handler(mysql::error());
			else
				next_result(std::forward<Handler>(handler));
		});
	}

};

class async_connection : public basic_database, public qtl::async_connection<async_connection, async_statement>
{
public:
	async_connection() = default;

	/*
		OpenHandler defines as:
			void handler(const qtl::mysql::error& e) NOEXCEPT;
	*/
	template<typename EventLoop, typename OpenHandler>
	void open(EventLoop& ev, OpenHandler&& handler, const char *host, const char *user, const char *passwd, const char *db,
		unsigned long clientflag = 0, unsigned int port = 0, const char *unix_socket = nullptr)
	{
		if (m_mysql == nullptr)
			m_mysql = mysql_init(nullptr);

		mysql_options(m_mysql, MYSQL_OPT_NONBLOCK, 0);

		MYSQL* ret = nullptr;
		int status = mysql_real_connect_start(&ret, m_mysql, host, user, passwd, db, port, unix_socket, clientflag);
		bind(ev);
		if (status)
			wait_connect(status, std::forward<OpenHandler>(handler));
		else
			handler((ret == nullptr) ? mysql::error(*this) : mysql::error());
	}

	/*
		CloseHandler defines as:
			void handler() NOEXCEPT;
	*/
	template<typename CloseHandler >
	void close(CloseHandler&& handler) NOEXCEPT
	{
		int status  = mysql_close_start(m_mysql);
		if (status)
		{
			wait_close(status, [this, handler]() mutable {
				handler();
				m_mysql = nullptr;
			});
		}
		else
		{
			handler();
			m_mysql = nullptr;
		}
	}

	/*
		Handler defines as:
			void handler(const qtl::mysql::error& e) NOEXCEPT;
	*/
	template<typename Handler>
	void refresh(Handler&& handler, unsigned int options) NOEXCEPT
	{
		int ret = 0;
		int status = mysql_refresh_start(&ret, m_mysql, options);
		if (status)
			wait_operation(status, &mysql_refresh_cont, std::forward<Handler>(handler));
		else
			handler((ret) ? mysql::error(*this) : mysql::error());
	}

	template<typename Handler>
	void select(Handler&& handler, const char* db) NOEXCEPT
	{
		int ret = 0;
		int status = mysql_select_db_start(&ret, m_mysql, db);
		if (status)
			wait_operation(status, &mysql_select_db_cont, std::forward<Handler>(handler));
		else
			handler((ret) ? mysql::error(*this) : mysql::error());
	}

	template<typename Handler>
	void is_alive(Handler&& handler) NOEXCEPT
	{
		int ret = 0;
		int status = mysql_ping_start(&ret, m_mysql);
		if (status)
			wait_operation(status, &mysql_ping_cont, std::forward<Handler>(handler));
		else
			handler((ret) ? mysql::error(*this) : mysql::error());
	}

	/*
		Handler defines as:
			void handler(const qtl::mysql::error& e, uint64_t affected) NOEXCEPT;
	*/
	template<typename ExecuteHandler>
	void simple_execute(ExecuteHandler&& handler, const char* query_text) NOEXCEPT
	{
		int ret = 0;
		int status = mysql_query_start(&ret, m_mysql, query_text);
		if (status)
		{
			wait_operation(status, &mysql_query_cont, [this, handler](const mysql::error& e) {
				uint64_t affected = 0;
				if (!e) affected = affected_rows();
				handler(e, affected);
			});
		}
		else
		{
			uint64_t affected = 0;
			if (ret >= 0) affected = affected_rows();
			handler((ret) ? mysql::error(*this) : mysql::error(), affected);
		}
	}

	template<typename ExecuteHandler>
	void simple_execute(ExecuteHandler&& handler, const char* query_text, unsigned long text_length) NOEXCEPT
	{
		int ret = 0;
		int status = mysql_real_query_start(&ret, m_mysql, query_text, text_length);
		if (status)
		{
			wait_operation(status, &mysql_real_query_cont, [this, handler](const mysql::error& e) mutable {
				uint64_t affected = 0;
				if (!e) affected = affected_rows();
				handler(e, affected);
			});
		}
		else
		{
			uint64_t affected = 0;
			if (ret >= 0) affected = affected_rows();
			handler((ret) ? mysql::error(*this) : mysql::error(), affected);
		}
	}

	template<typename Handler>
	void auto_commit(Handler&& handler, bool on) NOEXCEPT
	{
		my_bool ret;
		int status = mysql_autocommit_start(&ret, m_mysql, on ? 1 : 0);
		if(status)
			wait_operation(status, &mysql_autocommit_cont, std::forward<Handler>(handler));
		else
			handler((ret) ? mysql::error(*this) : mysql::error());
	}

	template<typename Handler>
	void begin_transaction(Handler&& handler) NOEXCEPT
	{
		auto_commit(handler, false);
	}

	template<typename Handler>
	void rollback(Handler&& handler) NOEXCEPT
	{
		my_bool ret;
		int status = mysql_rollback_start(&ret, m_mysql);
		if (status)
			wait_operation(status, &mysql_rollback_cont, std::forward<Handler>(handler));
		else
			handler((ret) ? mysql::error(*this) : mysql::error());
	}

	template<typename Handler>
	void commit(Handler&& handler) NOEXCEPT
	{
		my_bool ret;
		int status = mysql_commit_start(&ret, m_mysql);
		if (status)
			wait_operation(status, &mysql_commit_cont, std::forward<Handler>(handler));
		else
			handler((ret) ? mysql::error(*this) : mysql::error());
	}

	/*
		RowHandler defines as:
			bool row_handler(MYSQL_ROW row, int field_count) NOEXCEPT;
		ResultHandler defines as:
			void result_handler(const qtl::mysql::error& e, size_t row_count) NOEXCEPT;
	*/
	template<typename RowHandler, typename ResultHandler>
	void simple_query(const char* query, unsigned long length, RowHandler&& row_handler, ResultHandler&& result_handler) NOEXCEPT
	{
		simple_execute([this, row_handler, result_handler](const mysql::error& e, uint64_t affected) mutable {
			if (e)
			{
				result_handler(e, 0);
				return;
			}

			unsigned int field_count = mysql_field_count(m_mysql);
			if (field_count > 0)
			{
				MYSQL_RES* result = nullptr;
				int status = mysql_store_result_start(&result, m_mysql);
				if (status)
					wait_query(status, field_count, row_handler, result_handler);
				else if (result)
					fetch_rows(result, field_count, 0, row_handler, result_handler);
				else
					result_handler(mysql::error(*this), 0);
			}
			else
			{
				result_handler(mysql::error(), 0);
			}
		}, query, length);
	}

	template<typename Handler>
	void open_command(const char* query_text, size_t text_length, Handler&& handler)
	{
		std::shared_ptr<async_statement> stmt=std::make_shared<async_statement>(*this);
		stmt->open([stmt, handler](const mysql::error& e) mutable {
			handler(e, stmt);
		}, query_text, (unsigned long)text_length);
	}

	socket_type socket() const NOEXCEPT { return mysql_get_socket(m_mysql); }

private:
	template<typename OpenHandler>
	void wait_connect(int status, OpenHandler&& handler) NOEXCEPT
	{
		m_event_handler->set_io_handler(event_flags(status), mysql_get_timeout_value(m_mysql),
			[this, handler](int flags) mutable {
			MYSQL* ret = nullptr;
			int status = mysql_real_connect_cont(&ret, m_mysql, mysql_status(flags));
			if (status)
				wait_connect(status, handler);
			else
				handler((ret == nullptr) ? mysql::error(*this) : mysql::error());
		});
	}

	template<typename CloseHandler>
	void wait_close(int status, CloseHandler&& handler) NOEXCEPT
	{
		m_event_handler->set_io_handler(event_flags(status), mysql_get_timeout_value(m_mysql),
			[this, handler](int flags) mutable {
			MYSQL* ret = nullptr;
			int status = mysql_close_cont(m_mysql, mysql_status(flags));
			if (status)
				wait_close(status, handler);
			else
				handler();
		});
	}

	template<typename Func, typename Handler>
	void wait_operation(int status, Func func, Handler&& handler) NOEXCEPT
	{
		m_event_handler->set_io_handler(event_flags(status), mysql_get_timeout_value(m_mysql),
			[this, func, handler](int flags) mutable {
			int ret = 0;
			int status = func(&ret, m_mysql, mysql_status(flags));
			if (status)
				wait_operation(status, func, handler);
			else
				handler((ret) ? mysql::error(*this) : mysql::error());
		});
	}

	template<typename RowHandler, typename ResultHandler>
	void wait_query(int status, int field_count, RowHandler&& row_handler, ResultHandler&& result_handler) NOEXCEPT
	{
		m_event_handler->set_io_handler(event_flags(status), mysql_get_timeout_value(m_mysql),
			[this, field_count, row_handler, result_handler](int flags) mutable {
			MYSQL_RES* result = 0;
			int status = mysql_store_result_cont(&result, m_mysql, mysql_status(flags));
			if (status)
				wait_query(status, field_count, row_handler, result_handler);
			else if (result)
				fetch_rows(result, field_count, 0, row_handler, result_handler);
			else
				result_handler(mysql::error(*this), 0);
		});
	}

	template<typename RowHandler, typename ResultHandler>
	void fetch_rows(MYSQL_RES* result, int field_count, size_t row_count, RowHandler&& row_handler, ResultHandler&& result_handler) NOEXCEPT
	{
		MYSQL_ROW row;
		int status = mysql_fetch_row_start(&row, result);
		if (status)
			wait_fetch(status, result, field_count, row_count, row_handler, result_handler);
		else if(row && row_handler(row, field_count))
			fetch_rows(result, field_count, row_count + 1, row_handler, result_handler);
		else
			free_result(result, row_count, result_handler);
	}

	template<typename ResultHandler>
	void free_result(MYSQL_RES* result, size_t row_count, ResultHandler&& result_handler) NOEXCEPT
	{
		int status = mysql_free_result_start(result);
		if (status)
			wait_free_result(status, result, row_count, result_handler);
		else
			result_handler(mysql::error(), row_count);
	}

	template<typename RowHandler, typename ResultHandler>
	void wait_fetch(int status, MYSQL_RES* result, int field_count,  size_t row_count, RowHandler&& row_handler, ResultHandler&& result_handler)
	{
		m_event_handler->set_io_handler(event_flags(status), mysql_get_timeout_value(m_mysql),
			[this, result, field_count, row_count, row_handler, result_handler](int flags) mutable {
			MYSQL_ROW row;
			int status = mysql_fetch_row_cont(&row, result, mysql_status(flags));
			if (status)
				wait_fetch(status, result, field_count, row_count, row_handler, result_handler);
			else if (result && row_handler(row, field_count))
				fetch_rows(result, field_count, row_count+1, row_handler, result_handler);
			else
				free_result(result, row_count, result_handler);
		});
	}

	template<typename ResultHandler>
	void wait_free_result(int status, MYSQL_RES* result, size_t row_count, ResultHandler&& handler) NOEXCEPT
	{
		m_event_handler->set_io_handler(event_flags(status), mysql_get_timeout_value(m_mysql),
			[this, result, row_count, handler](int flags) mutable {
			MYSQL* ret = nullptr;
			int status = mysql_free_result_cont(result, mysql_status(flags));
			if (status)
				wait_free_result(status, result, row_count, handler);
			else
				handler(mysql::error(), row_count);
		});
	}

};

inline 	async_statement::async_statement(async_connection& db) 
: base_statement(static_cast<basic_database&>(db)) 
{
	m_event=db.event();
}

#endif //MariaDB 10.0

typedef qtl::transaction<database> transaction;

template<typename Record>
using query_iterator = qtl::query_iterator<statement, Record>;

template<typename Record>
using query_result = qtl::query_result<statement, Record>;

template<typename Params>
inline statement& operator<<(statement& stmt, const Params& params)
{
	stmt.reset();
	stmt.execute(params);
	return stmt;
}

inline error::error(const base_statement& stmt)
{
	const char* errmsg=stmt.errmsg();
	m_error=stmt.error();
	if(errmsg) m_errmsg=errmsg;
}

inline error::error(const basic_database& db)
{
	const char* errmsg=db.errmsg();
	m_error=db.error();
	if(errmsg) m_errmsg=errmsg;
}

inline base_statement::base_statement(basic_database& db)
{
	m_stmt=mysql_stmt_init(db.handle());
	m_result=nullptr;
}

}

}

#endif //_QTL_MYSQL_H_
