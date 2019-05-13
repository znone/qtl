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

#if LIBMYSQL_VERSION_ID >=80000
typedef bool my_bool;
#endif //MySQL 8

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
	void bind(bool& v)
	{
		init();
		buffer_type = MYSQL_TYPE_TINY;
		buffer = &v;
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

class statement;
class database;

class error : public std::exception
{
public:
	error() : m_error(0) { }
	error(unsigned int err, const char* errmsg) : m_error(err), m_errmsg(errmsg) { }
	explicit error(statement& stmt);
	explicit error(database& db);
	error(const error& src) = default;
	virtual ~error() throw() { }
	int code() const throw() { return m_error; }
	virtual const char* what() const throw() override { return m_errmsg.data(); }
private:
	unsigned int m_error;
	std::string m_errmsg;
};

class blobbuf : public std::streambuf
{
public:
	blobbuf() : m_stmt(nullptr), m_field(0) 
	{
	}
	blobbuf(const blobbuf&) = default;
	blobbuf& operator=(const blobbuf&) = default;
	virtual ~blobbuf() 
	{ 
		overflow();
	}

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
		size_t bufsize;
		if (b.length) m_size = *b.length;
		if (m_size > 0)
			bufsize = std::min<size_t>(blob_buffer_size, m_size);
		else
			bufsize = blob_buffer_size;
		if (mode&std::ios_base::in)
		{
			m_buf.resize(bufsize);
			m_pos = 0;
			setg(m_buf.data(), m_buf.data(), m_buf.data());
		}
		else if (mode&std::ios_base::out)
		{
			m_buf.resize(bufsize);
			m_pos = 0;
			setp(m_buf.data(), m_buf.data() + bufsize);
		}

	}

protected:
	virtual pos_type seekoff(off_type off, std::ios_base::seekdir dir,
		std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override
	{
		if (which&std::ios_base::in)
		{
			pos_type pos = 0;
			pos = seekoff(m_pos, off, dir);
			return seekpos(pos, which);
		}
		return std::streambuf::seekoff(off, dir, which);
	}

	virtual pos_type seekpos(pos_type pos,
		std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override
	{
		if (pos >= m_size)
			return pos_type(off_type(-1));

		if (which&std::ios_base::out)
		{
			if (pos < m_pos || pos >= m_pos + off_type(egptr() - pbase()))
			{
				overflow();
				m_pos = pos;
				setp(m_buf.data(), m_buf.data() + m_buf.size());
			}
			else
			{
				pbump(off_type(pos - pabs()));
			}
		}
		else if (which&std::ios_base::in)
		{
			if (pos < m_pos || pos >= m_pos + off_type(epptr() - eback()))
			{
				m_pos = pos;
				setg(m_buf.data(), m_buf.data(), m_buf.data());
			}
			else
			{
				gbump(off_type(pos - gabs()));
			}
		}
		return pos;
	}

	virtual std::streamsize showmanyc() override
	{
		return m_size - pabs();
	}

	virtual int_type underflow() override
	{
		if (pptr() > pbase())
			overflow();

		off_type count = egptr() - eback();
		pos_type next_pos=0;
		if (count == 0 && eback() == m_buf.data())
		{
			setg(m_buf.data(), m_buf.data(), m_buf.data() + m_buf.size());
			count = m_buf.size();
		}
		else
		{
			next_pos = m_pos + pos_type(count);
		}
		if (next_pos >= m_size)
			return traits_type::eof();

		count = std::min(count, m_size - next_pos);
		m_binder.buffer = m_buf.data();
		m_binder.buffer_length = count;
		m_pos = next_pos;
		int ret = mysql_stmt_fetch_column(m_stmt, &m_binder,  m_field, m_pos);
		switch (ret)
		{
		case 0:
			count = std::min(m_binder.buffer_length,  *m_binder.length);
			setg(eback(), eback(), eback() + count);
			return traits_type::to_int_type(*gptr());
		case CR_NO_DATA:
			return traits_type::eof();
		default:
			throw error(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));
		}
	}

	virtual int_type overflow(int_type ch = traits_type::eof()) override
	{
		if (pptr() != pbase())
		{
			size_t count = pptr() - pbase();
			int ret = mysql_stmt_send_long_data(m_stmt, m_field, pbase(), count);
			if (ret != 0)
				throw error(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));

			//auto intersection = interval_intersection(m_pos, egptr() - eback(), m_pos, epptr() - pbase());
			//if (intersection.first != intersection.second)
			//{
			//	commit(intersection.first, intersection.second);
			//}

			m_pos += count;
			setp(pbase(), epptr());
		}
		if (!traits_type::eq_int_type(ch, traits_type::eof()))
		{
			char_type c = traits_type::to_char_type(ch);
			if (m_pos >= m_size)
				return traits_type::eof();
			int ret = mysql_stmt_send_long_data(m_stmt, m_field, &c, 1);
			if (ret != 0)
				throw error(mysql_stmt_errno(m_stmt), mysql_stmt_error(m_stmt));

			//auto intersection = interval_intersection(m_pos, egptr() - eback(), m_pos, 1);
			//if (intersection.first != intersection.second)
			//{
			//	eback()[intersection.first - m_pos] = c;
			//}
			m_pos += 1;

		}
		return ch;
	}

	virtual int_type pbackfail(int_type c = traits_type::eof()) override
	{
		if (gptr() == 0
			|| gptr() <= eback()
			|| (!traits_type::eq_int_type(traits_type::eof(), c)
				&& !traits_type::eq(traits_type::to_char_type(c), gptr()[-1])))
		{
			return (traits_type::eof());	// can't put back, fail
		}
		else
		{	// back up one position and store put-back character
			gbump(-1);
			if (!traits_type::eq_int_type(traits_type::eof(), c))
				*gptr() = traits_type::to_char_type(c);
			return (traits_type::not_eof(c));
		}
	}



private:
	MYSQL_STMT* m_stmt;
	binder m_binder;
	int m_field;
	std::vector<char> m_buf;
	pos_type m_size;
	pos_type m_pos;	//position in the input sequence

	off_type seekoff(off_type position, off_type off, std::ios_base::seekdir dir)
	{
		off_type result = 0;
		switch (dir)
		{
		case std::ios_base::beg:
			result = off;
			break;
		case std::ios_base::cur:
			result = position + off;
			break;
		case std::ios_base::end:
			result = m_size - off;
		}
		if (result > m_size)
			result = m_size;
		return result;
	}

	pos_type gabs() const // absolute offset of input pointer in blob field
	{
		return m_pos + off_type(gptr() - eback());
	}

	pos_type pabs() const // absolute offset of output pointer in blob field
	{
		return m_pos + off_type(pptr() - pbase());
	}
};

typedef std::function<void(std::ostream&)> blob_writer;

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

	void bind_param(size_t index, const blob_writer& param)
	{
		m_binders[index].bind(NULL, 0, MYSQL_TYPE_LONG_BLOB);
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
		if(m_result)
		{
			bind(m_binders[index], std::forward<Type>(value));
			m_binderAddins[index].m_after_fetch=if_null<typename std::remove_reference<Type>::type>(value);
		}
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

	template<typename T>
	void bind_field(size_t index, bind_string_helper<T>&& value)
	{
		if(m_result)
		{
			MYSQL_FIELD* field=mysql_fetch_field_direct(m_result, (unsigned int)index);
			if(field==NULL) throw_exception();
			value.clear();
			typename bind_string_helper<T>::char_type* data=value.alloc(field->length);
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
			m_binderAddins[index].m_after_fetch= [value](const binder& b) mutable {
				if(*b.is_null) value.clear();
				else value.truncate(*b.length);
			};
			m_binders[index].bind((void*)data, field->length, field->type);
		}
	}

	void bind_field(size_t index, std::ostream&& value)
	{
		if(m_result)
		{
			MYSQL_FIELD* field = mysql_fetch_field_direct(m_result, (unsigned int)index);
			assert(IS_LONGDATA(field->type));
			m_binders[index].bind(NULL, 0, field->type);
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

	void bind_field(size_t index, blobbuf&& value)
	{
		if (m_result)
		{
			MYSQL_FIELD* field = mysql_fetch_field_direct(m_result, (unsigned int)index);
			assert(IS_LONGDATA(field->type));
			m_binders[index].bind(NULL, 0, field->type);
			m_binderAddins[index].m_after_fetch = [this, index, &value](const binder& b) {
				if (*b.is_null) return;
				value.open(m_stmt, index, b, std::ios::in);
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
			m_result=NULL;
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

	void throw_exception() { throw mysql::error(*this); }

private:
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
	typedef mysql::error exception_type;

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

	unsigned int error() const
	{
		return mysql_errno(m_mysql);
	}
	const char* errmsg() const
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
