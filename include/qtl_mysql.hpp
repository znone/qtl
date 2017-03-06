#ifndef _MYDTL_MYSQL_H_
#define _MYDTL_MYSQL_H_

#include <mysql.h>
#include <errmsg.h>
#include <time.h>
#include <memory.h>
#include <assert.h>
#include <stdint.h>
#include <string>
#include <vector>
#include <array>
#include <functional>
#include <algorithm>
#include "qtl_common.hpp"

namespace qtl
{

namespace mysql
{

struct init
{
	init(int argc=-1, char **argv=NULL, char **groups=NULL) 
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

class statement;
class database;

class error : public std::exception
{
public:
	error(int err, const char* errmsg) : m_error(err), m_errmsg(errmsg) { }
	explicit error(statement& stmt);
	explicit error(database& db);
	error(const error& src) = default;
	virtual ~error() throw() { }
	int code() const throw() { return m_error; }
	virtual const char* what() const throw() override { return m_errmsg.data(); }
private:
	int m_error;
	std::string m_errmsg;
};

class statement final
{
public:
	statement() : m_stmt(NULL), m_result(NULL) {}
	explicit statement(database& db);
	statement(const statement&) = delete;
	statement(statement&& src)
		: m_stmt(src.m_stmt), m_result(src.m_result),
		m_binders(std::move(src.m_binders)), m_binderAddins(std::move(src.m_binderAddins))
	{
		src.m_stmt=NULL;
		src.m_result=NULL;
	}
	statement& operator=(const statement&) = delete;
	statement& operator=(statement&& src)
	{
		if(this!=&src)
		{
			m_stmt=src.m_stmt;
			m_result=src.m_result;
			src.m_stmt=NULL;
			src.m_result=NULL;
			m_binders=std::move(src.m_binders);
			m_binderAddins=std::move(src.m_binderAddins);
		}
		return *this;
	}
	~statement()
	{
		close();
	}

	operator MYSQL_STMT*() { return m_stmt; }

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

	template<typename Types>
	void execute(const Types& params)
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
		if(mysql_stmt_execute(m_stmt)!=0)
			throw_exception();
	}

	template<typename Types>
	bool fetch(Types&& values)
	{
		if(m_result==NULL)
		{
			unsigned long count=mysql_stmt_field_count(m_stmt);
			if(count>0)
			{
				m_result=mysql_stmt_result_metadata(m_stmt);
				if(m_result==NULL) throw_exception();
				resize_binders(count);
				qtl::bind_record(*this, std::forward<Types>(values));
				set_binders();
				if(mysql_stmt_bind_result(m_stmt, m_binders.data())!=0)
					throw_exception();
			}
		}
		return fetch();
	}

	template<class Param>
	void bind_param(size_t index, const Param& param)
	{
		qtl::bind(m_binders[index], param);
	}

	template<class Type>
	void bind_field(size_t index, Type&& value)
	{
		if(m_result)
		{
			qtl::bind(m_binders[index], std::forward<Type>(value));
			m_binderAddins[index].m_after_fetch=if_null<typename std::remove_reference<Type>::type>(value);
		}
	}

	void bind_param(size_t index, const std::nullptr_t&)
	{
		m_binders[index].bind();
	}

	void bind_field(size_t index, char* value, size_t length)
	{
		m_binders[index].bind(value, length-1, MYSQL_TYPE_VAR_STRING);
		m_binderAddins[index].m_after_fetch=[](const binder& bind) {
			if(*bind.is_null)
				memset(bind.buffer, 0, bind.buffer_length+1);
			else
			{
				char* text=reinterpret_cast<char*>(bind.buffer);
				text[*bind.length]='\0';
			}
		};
	}

	template<size_t N>
	void bind_field(size_t index, std::array<char, N>&& value)
	{
		bind_field(index, value.data(), value.size());
	}

	void bind_field(size_t index, std::string&& value)
	{
		if(m_result)
		{
			MYSQL_FIELD* field=mysql_fetch_field_direct(m_result, (unsigned int)index);
			if(field==NULL) throw_exception();
			value.clear();
			value.resize(field->length);
			m_binderAddins[index].m_after_fetch=resize_binder<std::string>(value);
			qtl::bind(m_binders[index], std::forward<std::string>(value));
			m_binders[index].buffer_type=field->type;
		}
	}
	void bind_param(size_t index, std::istream& param)
	{
		m_binders[index].bind(NULL, 0, MYSQL_TYPE_LONG_BLOB);
		m_binderAddins[index].m_after_fetch=[this, index, &param](const binder&) {
			std::array<char, blob_buffer_size> buffer;
			unsigned long readed=0;
			while(!param.eof() && !param.fail())
			{
				param.read(buffer.data(), buffer.size());
				readed=(unsigned long)param.gcount();
				if(readed>0)
				{
					if(mysql_stmt_send_long_data(m_stmt, index, buffer.data(), readed)!=0)
						throw_exception();
				}
			}
		};
	}
	void bind_field(size_t index, std::ostream&& value)
	{
		if(m_result)
		{
			m_binders[index].bind(NULL, 0, MYSQL_TYPE_LONG_BLOB);
			m_binderAddins[index].m_after_fetch=[this, index, &value](const binder& b) {
				unsigned long readed=0;
				std::array<char, blob_buffer_size> buffer;
				binder& bb=const_cast<binder&>(b);
				if(*b.is_null) return;
				bb.buffer=const_cast<char*>(buffer.data());
				bb.buffer_length=buffer.size();
				while(readed<=*b.length)
				{
					int ret=mysql_stmt_fetch_column(m_stmt, &bb, index, readed);
					if(ret!=0)
						throw_exception();
					value.write(buffer.data(), std::min(b.buffer_length, *b.length-b.offset));
					readed+=bb.buffer_length;
				}
			};
		}
	}

	template<typename Type>
	void bind_field(size_t index, indicator<Type>&& value)
	{
		if(m_result)
		{
			qtl::bind_field(*this, index, value.data);
			binder_addin& addin=m_binderAddins[index];
			auto fetch_fun=addin.m_after_fetch;
			addin.m_after_fetch=[&addin, fetch_fun, &value](const binder& b) {
				value.is_null= *b.is_null!=0;
				value.length=*b.length;
				value.is_truncated=addin.is_truncated;
				if(fetch_fun) fetch_fun(b);
			};
		}
	}

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

	void close()
	{
		if(m_result)
		{
			mysql_free_result(m_result);
			m_result=NULL;
		}
		if(m_stmt)
		{
			mysql_stmt_close(m_stmt);
			m_stmt=NULL;
		}
	}

	bool fetch()
	{
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

	unsigned int error() 
	{
		return mysql_stmt_errno(m_stmt);
	}
	const char* errmsg()
	{
		return mysql_stmt_error(m_stmt);
	}

	MYSQL_RES* result() { return m_result; }
	bool reset() { return mysql_stmt_reset(m_stmt)!=0; }

private:
	MYSQL_STMT* m_stmt;
	MYSQL_RES* m_result;
	std::vector<binder> m_binders;

	struct binder_addin
	{
		unsigned long m_length;
		my_bool m_isNull;
		my_bool m_error;
		bool is_truncated;
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

	void throw_exception() { throw mysql::error(*this); }

private:
	template<typename CharCont>
	struct resize_binder
	{
		resize_binder(CharCont& cont) : m_cont(cont) { }
		void operator()(const binder& b) const
		{
			if(*b.is_null) m_cont.clear();
			else m_cont.resize(*b.length);
		}

		CharCont& m_cont;
	};

	template<typename Value>
	struct if_null
	{
		if_null(Value& value, Value&& def=Value()) : m_value(value), m_def(std::move(def)) { }
		void operator()(const binder& b)
		{
			if(*b.is_null) m_value=m_def;
		}
		Value& m_value;
		Value m_def;
	};
};

class database final : public qtl::base_database<database, statement>
{
public:
	database()
	{
		m_mysql=mysql_init(NULL);
	}
	~database()
	{
		mysql_close(m_mysql);
	}
	database(const database&) = delete;
	database(database&& src)
	{
		m_mysql=src.m_mysql;
		src.m_mysql=NULL;
	}
	database& operator==(const database&) = delete;
	database& operator==(database&& src)
	{
		if(this!=&src)
		{
			mysql_close(m_mysql);
			m_mysql=src.m_mysql;
			src.m_mysql=NULL;
		}
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

	bool open(const char *host, const char *user, const char *passwd, const char *db,
		unsigned long clientflag=0, unsigned int port=0, const char *unix_socket=NULL)
	{
		if(m_mysql==NULL) m_mysql=mysql_init(NULL);
		return mysql_real_connect(m_mysql, host, user, passwd, db, port, unix_socket, clientflag)!=NULL;
	}
	void close()
	{
		mysql_close(m_mysql);
		m_mysql=NULL;
	}
	void refresh(unsigned int options)
	{
		if(mysql_refresh(m_mysql, options)<0)
			throw_exception();
	}

	void select(const char* db)
	{
		if(mysql_select_db(m_mysql, db)!=0)
			throw_exception();
	}

	const char* current() const
	{
		return m_mysql->db;
	}

	unsigned int error() 
	{
		return mysql_errno(m_mysql);
	}
	const char* errmsg()
	{
		return mysql_error(m_mysql);
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

	void simple_execute(const char* query_text, uint64_t* paffected=NULL)
	{
		if(mysql_query(m_mysql, query_text)!=0)
			throw_exception();
		if(paffected) *paffected=affected_rows();
	}
	void simple_execute(const char* query_text, unsigned long text_length, uint64_t* paffected=NULL)
	{
		if(text_length==0) text_length=(unsigned long)strlen(query_text);
		if(mysql_real_query(m_mysql, query_text, text_length)!=0)
			throw_exception();
		if(paffected) *paffected=affected_rows();
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

	void auto_commit(bool on)
	{
		if(mysql_autocommit(m_mysql, on?1:0)!=0)
			throw_exception();
	}
	void begin_transaction()
	{
		auto_commit(false);
	}
	void rollback()
	{
		if(mysql_rollback(m_mysql)!=0)
			throw_exception();
		auto_commit(true);
	}
	void commit()
	{
		if(mysql_commit(m_mysql)!=0)
			throw_exception();
		auto_commit(true);
	}

	bool is_alive()
	{
		return mysql_ping(m_mysql)==0;
	}

	template<typename Pred>
	bool simple_query(const char* query, unsigned long length, Pred&& pred)
	{
		simple_execute(query, length);

		unsigned int fieldCount=mysql_field_count(m_mysql);
		MYSQL_RES* result=mysql_store_result(m_mysql);
		if(fieldCount>0 && result)
		{
			MYSQL_RES* result=mysql_store_result(m_mysql);
			MYSQL_ROW row;
			while(row=mysql_fetch_row(result))
			{
				pred(*this, row, fieldCount);
			}
			mysql_free_result(result);
			return true;
		}
		return false;
	}

private:
	MYSQL* m_mysql;
	void throw_exception() { throw mysql::error(*this); }
};

struct time : public MYSQL_TIME
{
	time() 
	{
		memset(this, 0, sizeof(MYSQL_TIME));
		time_type=MYSQL_TIMESTAMP_NONE;
	}
	time(const struct tm& tm)
	{
		memset(this, 0, sizeof(MYSQL_TIME));
		year=tm.tm_year+1900;
		month=tm.tm_mon+1;
		day=tm.tm_mday;
		hour=tm.tm_hour;
		minute=tm.tm_min;
		second=tm.tm_sec;
		time_type=MYSQL_TIMESTAMP_DATETIME;
	}
	time(time_t value)
	{
		struct tm tm;
#if defined(_MSC_VER)
		localtime_s(&tm, &value);
#elif defined(_POSIX_VERSION)
		localtime_r(&value, &tm);
#else
		tm=*localtime(&value);
#endif
		new(this)time(tm);
	}
	time(const time& src)
	{
		memcpy(this, &src, sizeof(MYSQL_TIME));
	}
	time& operator=(const time& src)
	{
		if(this!=&src)
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
		tm.tm_year=year-1900;
		tm.tm_mon=month-1;
		tm.tm_mday=day;
		tm.tm_hour=hour;
		tm.tm_min=minute;
		tm.tm_sec=second;
		return mktime(&tm);
	}
	time_t get_time() const
	{
		struct tm tm;
		return as_tm(tm);
	}
};

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

inline error::error(statement& stmt)
{
	const char* errmsg=stmt.errmsg();
	m_error=stmt.error();
	if(errmsg) m_errmsg=errmsg;
}

inline error::error(database& db)
{
	const char* errmsg=db.errmsg();
	m_error=db.error();
	if(errmsg) m_errmsg=errmsg;
}

inline statement::statement(database& db)
{
	m_stmt=mysql_stmt_init(db.handle());
	m_result=NULL;
}

}

}

#endif //_MYDTL_MYSQL_H_
