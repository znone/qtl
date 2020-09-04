#ifndef _QTL_ODCB_H_
#define _QTL_ODCB_H_

#include <sql.h>
#include <sqlext.h>
#include <sstream>
#include <vector>
#include <array>
#include <time.h>
#include <assert.h>
#include <malloc.h>
#include <limits.h>
#include <stdint.h>

#if !defined(_WIN32) || defined(__MINGW32__)
#include <sys/time.h>
#endif //_WIN32

#if (ODBCVER >= 0x0380) && (WIN32_WINNT >= 0x0602)
#define QTL_ODBC_ENABLE_ASYNC_MODE 1
#endif //ODBC 3.80 && Windows


#include "qtl_common.hpp"

namespace qtl
{

namespace odbc
{

template<SQLSMALLINT> class object;
class database;

class error : public std::exception
{
public:
	template<SQLSMALLINT Type>
	error(const object<Type>& h, SQLINTEGER code);
	error(SQLINTEGER code, const char* msg) : m_errno(code), m_errmsg(msg) { }
	SQLINTEGER code() const { return m_errno; }
	operator bool() const { return m_errno!=SQL_SUCCESS || m_errno!=SQL_SUCCESS_WITH_INFO; }
	virtual const char* what() const throw() override { return m_errmsg.data(); }
private:
	SQLINTEGER m_errno;
	std::string m_errmsg;
};

template<SQLSMALLINT Type>
class object
{
public:
	enum { handler_type=Type };
	object() : m_handle(SQL_NULL_HANDLE) { };
	object(const object&) = delete;
	object(object&& src) : m_handle(src.m_handle)
	{
		src.m_handle=SQL_NULL_HANDLE;
	}
	explicit object(SQLHANDLE parent)
	{
		verify_error(SQLAllocHandle(handler_type, parent, &m_handle));
	}
	~object()
	{
		close();
	}
	object& operator=(const object&) = delete;
	object& operator=(object&& src)
	{
		if(this!=&src)
		{
			close();
			m_handle=src.m_handle;
			src.m_handle=NULL;
		}
		return *this;
	}
	SQLHANDLE handle() const { return m_handle; }

	void close()
	{
		if(m_handle)
		{
			verify_error(SQLFreeHandle(handler_type, m_handle));
			m_handle=SQL_NULL_HANDLE;
		}
	}

	void verify_error(SQLINTEGER code) const
	{
		if (code < 0)
			throw odbc::error(*this, code);
	}

protected:
	SQLHANDLE m_handle;
};

class blobbuf : public qtl::blobbuf
{
public:
	blobbuf() : m_stmt(nullptr), m_field(0)
	{
	}
	blobbuf(const blobbuf&) = default;
	blobbuf& operator=(const blobbuf&) = default;
	virtual ~blobbuf() { overflow(); }

	void open(object<SQL_HANDLE_STMT>* stmt, SQLSMALLINT field, std::ios_base::openmode mode)
	{
		if (m_stmt && m_field)
		{
			overflow();
		}

		assert(stmt != SQL_NULL_HANDLE);
		m_stmt = stmt;
		m_field = field;
		m_size = INTMAX_MAX;
		init_buffer(mode);
	}

private:
	object<SQL_HANDLE_STMT>* m_stmt;
	SQLSMALLINT m_field;

protected:
	virtual bool read_blob(char* buffer, off_type& count, pos_type position) override
	{
		SQLLEN indicator=0;
		SQLRETURN ret = SQLGetData(m_stmt->handle(), m_field + 1, SQL_C_BINARY, buffer, count, const_cast<SQLLEN*>(&indicator));
		if (ret != SQL_NO_DATA)
		{
			count = (indicator > count) || (indicator == SQL_NO_TOTAL) ?
				count : indicator;
			m_stmt->verify_error(ret);
			return true;
		}
		else return false;
	}

	virtual void write_blob(const char* buffer, size_t count) override
	{
		m_stmt->verify_error(SQLPutData(m_stmt->handle(), (SQLPOINTER)buffer, count));
	}
};

class environment final : public object<SQL_HANDLE_ENV>
{
public:
	environment() : object(SQL_NULL_HANDLE) 
	{
		verify_error(SQLSetEnvAttr(m_handle, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0));
	}
	environment(environment&& src) : object(std::forward<environment>(src)) { }

	int32_t version() const
	{
		int32_t ver = 0;
		verify_error(SQLGetEnvAttr(m_handle, SQL_ATTR_ODBC_VERSION, &ver, sizeof(DWORD), NULL));
		return ver;
	}
};

class statement final : public object<SQL_HANDLE_STMT>
{
public:
	explicit statement(database& db);
	statement(statement&& src) 
		: object(std::forward<statement>(src)), m_params(std::forward<std::vector<param_data>>(src.m_params))
	{
		m_binded_cols=src.m_binded_cols;
		src.m_binded_cols=false;
		m_blob_buffer=src.m_blob_buffer;
		src.m_blob_buffer=NULL;
	}
	~statement()
	{
		if(m_blob_buffer)
			free(m_blob_buffer);
	}
	statement& operator=(statement&& src)
	{
		if(this!=&src)
		{
			object::operator =(std::forward<statement>(src));
			m_params=std::forward<std::vector<param_data>>(src.m_params);
			m_binded_cols=src.m_binded_cols;
			src.m_binded_cols=false;
			m_blob_buffer=src.m_blob_buffer;
			src.m_blob_buffer=NULL;
		}
		return *this;
	}

	void open(const char* query_text, size_t text_length=SQL_NTS)
	{
		verify_error(SQLPrepare(m_handle, (SQLCHAR*)query_text, text_length));
	}
	void open(const std::string& query_text)
	{
		open(query_text.data(), query_text.size());
	}

	void bind_param(SQLUSMALLINT index, const std::nullptr_t&)
	{
		m_params[index].m_indicator=SQL_NULL_DATA;
		verify_error(SQLBindParameter(m_handle, index+1, 
			SQL_PARAM_INPUT, SQL_C_DEFAULT, SQL_DEFAULT, 0, 0, NULL, 0, &m_params[index].m_indicator));
	}
	void bind_param(SQLUSMALLINT index, const qtl::null&)
	{
		bind_param(index, nullptr);
	}
	void bind_param(SQLUSMALLINT index, const int8_t& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_STINYINT, SQL_TINYINT, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const uint8_t& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_UTINYINT, SQL_TINYINT, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const int16_t& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_SSHORT, SQL_SMALLINT, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const uint16_t& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_USHORT, SQL_SMALLINT, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const int32_t& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const uint32_t& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_ULONG, SQL_INTEGER, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const int64_t& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const uint64_t& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_UBIGINT, SQL_BIGINT, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const double& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const float& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_FLOAT, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const bool& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_BIT, SQL_BIT, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const DATE_STRUCT& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_DATE, SQL_DATE, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const TIME_STRUCT& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_TIME, SQL_TIME, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const TIMESTAMP_STRUCT& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_TIMESTAMP, SQL_TIMESTAMP, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const SQLGUID& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_GUID, SQL_GUID, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const SQL_NUMERIC_STRUCT& v)
	{
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_NUMERIC, SQL_NUMERIC, 
			0, 0, (SQLPOINTER)&v, 0, NULL));
	}
	void bind_param(SQLUSMALLINT index, const char* v, size_t n=SQL_NTS, SQLULEN size=0)
	{
		m_params[index].m_indicator=n;
		if(size==0) size=strlen(v);
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 
			size, 0, (SQLPOINTER)v, 0, &m_params[index].m_indicator));
	}
	void bind_param(SQLUSMALLINT index, const wchar_t* v, size_t n=SQL_NTS, SQLULEN size=0)
	{
		m_params[index].m_indicator=n;
		if(size==0) size=wcslen(v);
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WCHAR, 
			size, 0, (SQLPOINTER)v, 0, &m_params[index].m_indicator));
	}
	void bind_param(SQLUSMALLINT index, const std::string& v)
	{
		bind_param(index, v.data(), v.size(), v.size());
	}
	void bind_param(SQLUSMALLINT index, const std::wstring& v)
	{
		bind_param(index, v.data(), v.size(), v.size());
	}
	void bind_param(SQLUSMALLINT index, const const_blob_data& v)
	{
		m_params[index].m_indicator=v.size;
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_BINARY, 
			v.size, 0, (SQLPOINTER)v.data, 0, &m_params[index].m_indicator));
	}
	void bind_param(SQLUSMALLINT index, std::istream& s)
	{
		if(m_blob_buffer==NULL)
			m_blob_buffer=malloc(blob_buffer_size);
		m_params[index].m_data=m_blob_buffer;
		m_params[index].m_size=blob_buffer_size;
		m_params[index].m_indicator=SQL_LEN_DATA_AT_EXEC(m_params[index].m_size);
		verify_error(SQLBindParameter(m_handle, index+1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_LONGVARBINARY, 
			INT_MAX, 0, &m_params[index], 0, &m_params[index].m_indicator));
		m_params[index].m_after_fetch=[this, &s](const param_data& p) {
			SQLLEN readed=SQL_NULL_DATA;
			while(!s.eof() && !s.fail())
			{
				s.read((char*)p.m_data, p.m_size);
				readed=(unsigned long)s.gcount();
				if(readed>0)
				{
					verify_error(SQLPutData(m_handle, p.m_data, readed));
				}
			}
		};
	}

	void bind_param(SQLUSMALLINT index, const blob_writer& param)
	{
		m_params[index].m_data = nullptr;
		m_params[index].m_size = blob_buffer_size;
		m_params[index].m_indicator = SQL_LEN_DATA_AT_EXEC(m_params[index].m_size);
		verify_error(SQLBindParameter(m_handle, index + 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_LONGVARBINARY,
			INT_MAX, 0, &m_params[index], 0, &m_params[index].m_indicator));
		m_params[index].m_after_fetch = [this, index, &param](const param_data& b) {
			blobbuf buf;
			buf.open(this, index, std::ios::out);
			std::ostream s(&buf);
			param(s);
		};
	}

	void bind_field(SQLUSMALLINT index, bool&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_BIT, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, int8_t&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_STINYINT, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, uint8_t&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_UTINYINT, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, int16_t&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_SSHORT, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, uint16_t&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_USHORT, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, int32_t&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_SLONG, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, uint32_t&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_ULONG, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, int64_t&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_SBIGINT, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, uint64_t&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_UBIGINT, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, float&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_FLOAT, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, double&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_DOUBLE, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, DATE_STRUCT&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_TYPE_DATE, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, TIME_STRUCT&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_TYPE_TIME, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, TIMESTAMP_STRUCT&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_TYPE_TIMESTAMP, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, SQLGUID&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_GUID, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, SQL_NUMERIC_STRUCT&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_NUMERIC, &v, 0, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, char* v, size_t n)
	{
		m_params[index].m_data=v;
		m_params[index].m_size=n;
		m_params[index].m_after_fetch=[](const param_data& p) {
			if(p.m_indicator==SQL_NULL_DATA)
				memset(p.m_data, 0, p.m_size*sizeof(char));
			else
			{
				char* text=reinterpret_cast<char*>(p.m_data);
				text[p.m_indicator]='\0';
			}
		};
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_CHAR, v, n, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, wchar_t* v, size_t n)
	{
		m_params[index].m_data=v;
		m_params[index].m_size=n;
		m_params[index].m_after_fetch=[](const param_data& p) {
			if(p.m_indicator==SQL_NULL_DATA)
				memset(p.m_data, 0, p.m_size*sizeof(wchar_t));
			else
			{
				wchar_t* text=reinterpret_cast<wchar_t*>(p.m_data);
				text[p.m_indicator]='\0';
			}
		};
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_WCHAR, v, n, &m_params[index].m_indicator));
	}
	template<typename T>
	void bind_field(SQLUSMALLINT index, qtl::bind_string_helper<T>&& v)
	{
		SQLLEN length=0;
		verify_error(SQLColAttribute(m_handle, index+1, SQL_DESC_LENGTH, NULL, 0, NULL, &length));
		typename qtl::bind_string_helper<T>::char_type* data=v.alloc(length);
		bind_field(index, data, length+1);
		m_params[index].m_after_fetch=[v](const param_data& p) mutable {
			if(p.m_indicator==SQL_NULL_DATA)
				v.clear();
			else
				v.truncate(p.m_indicator);
		};
	}
	template<size_t N>
	void bind_field(SQLUSMALLINT index, std::array<char, N>&& value)
	{
		bind_field(index, value.data(), value.size());
	}
	template<size_t N>
	void bind_field(SQLUSMALLINT index, std::array<wchar_t, N>&& value)
	{
		bind_field(index, value.data(), value.size());
	}
	void bind_field(SQLUSMALLINT index, qtl::blob_data&& v)
	{
		verify_error(SQLBindCol(m_handle, index+1, SQL_C_BINARY, v.data, v.size, &m_params[index].m_indicator));
	}
	void bind_field(SQLUSMALLINT index, std::ostream&& v)
	{
		if(m_blob_buffer==NULL)
			m_blob_buffer=malloc(blob_buffer_size);
		m_params[index].m_data=m_blob_buffer;
		m_params[index].m_size=blob_buffer_size;
		m_params[index].m_after_fetch=[this, index, &v](const param_data& p) {
			SQLRETURN ret=SQLGetData(m_handle, index+1, SQL_C_BINARY, p.m_data, p.m_size, const_cast<SQLLEN*>(&p.m_indicator));
			while(ret!=SQL_NO_DATA)
			{
				size_t n = (p.m_indicator > blob_buffer_size) || (p.m_indicator == SQL_NO_TOTAL) ?
					blob_buffer_size : p.m_indicator;
				verify_error(ret);
				v.write((const char*)p.m_data, n);
				ret=SQLGetData(m_handle, index+1, SQL_C_BINARY, p.m_data, p.m_size, const_cast<SQLLEN*>(&p.m_indicator));
			}
		};
	}

	void bind_field(SQLUSMALLINT index, blobbuf&& value)
	{
		m_params[index].m_data = nullptr;
		m_params[index].m_size = 0;
		m_params[index].m_after_fetch = [this, index, &value](const param_data& p) {
			value.open(this, index, std::ios::in);
		};
	}

	template<typename Type>
	void bind_field(SQLUSMALLINT index, indicator<Type>&& value)
	{
		qtl::bind_field(*this, index, value.data);
		param_data& param=m_params[index];
		auto fetch_fun=param.m_after_fetch;
		param.m_after_fetch=[fetch_fun, &value](const param_data& p) {
			value.is_truncated=false;
			if(p.m_indicator==SQL_NULL_DATA)
			{
				value.is_null=true;
				value.length=0;
			}
			else if(p.m_indicator>=0)
			{
				value.is_null=false;
				value.length=p.m_indicator;
				if(p.m_size>0 && p.m_indicator>=p.m_size)
					value.is_truncated=true;
			}
			if(fetch_fun) fetch_fun(p);
		};
	}

#ifdef _QTL_ENABLE_CPP17

	template<typename Type>
	void bind_field(SQLUSMALLINT index, std::optional<Type>&& value)
	{
		qtl::bind_field(*this, index, *value);
		param_data& param = m_params[index];
		auto fetch_fun = param.m_after_fetch;
		param.m_after_fetch = [fetch_fun, &value](const param_data& p) {
			if (fetch_fun) fetch_fun(p);
			if (p.m_indicator == SQL_NULL_DATA)
				value.reset();
		};
	}

	void bind_field(SQLUSMALLINT index, std::any&& value)
	{
		SQLLEN type = 0, isUnsigned=SQL_FALSE;
		verify_error(SQLColAttribute(m_handle, index + 1, SQL_DESC_TYPE, NULL, 0, NULL, &type));
		verify_error(SQLColAttribute(m_handle, index + 1, SQL_DESC_UNSIGNED, NULL, 0, NULL, &isUnsigned));
		switch (type)
		{
		case SQL_BIT:
			value.emplace<bool>();
			bind_field(index, std::forward<bool>(std::any_cast<bool&>(value)));
			break;
		case SQL_TINYINT:
			if (isUnsigned)
			{
				value.emplace<uint8_t>();
				bind_field(index, std::forward<uint8_t>(std::any_cast<uint8_t&>(value)));
			}
			else
			{
				value.emplace<int8_t>();
				bind_field(index, std::forward<int8_t>(std::any_cast<int8_t&>(value)));
			}
			break;
		case SQL_SMALLINT:
			if (isUnsigned)
			{
				value.emplace<uint16_t>();
				bind_field(index, std::forward<uint16_t>(std::any_cast<uint16_t&>(value)));
			}
			else
			{
				value.emplace<int16_t>();
				bind_field(index, std::forward<int16_t>(std::any_cast<int16_t&>(value)));
			}
			break;
		case SQL_INTEGER:
			if (isUnsigned)
			{
				value.emplace<uint32_t>();
				bind_field(index, std::forward<uint32_t>(std::any_cast<uint32_t&>(value)));
			}
			else
			{
				value.emplace<int32_t>();
				bind_field(index, std::forward<int32_t>(std::any_cast<int32_t&>(value)));
			}
			break;
		case SQL_BIGINT:
			if (isUnsigned)
			{
				value.emplace<uint64_t>();
				bind_field(index, std::forward<uint64_t>(std::any_cast<uint64_t&>(value)));
			}
			else
			{
				value.emplace<int64_t>();
				bind_field(index, std::forward<int64_t>(std::any_cast<int64_t&>(value)));
			}
			break;
		case SQL_FLOAT:
			value.emplace<float>();
			bind_field(index, std::forward<float>(std::any_cast<float&>(value)));
			break;
		case SQL_DOUBLE:
			value.emplace<double>();
			bind_field(index, std::forward<double>(std::any_cast<double&>(value)));
			break;
		case SQL_NUMERIC:
			value.emplace<SQL_NUMERIC_STRUCT>();
			bind_field(index, std::forward<SQL_NUMERIC_STRUCT>(std::any_cast<SQL_NUMERIC_STRUCT&>(value)));
			break;
		case SQL_TIME:
			value.emplace<SQL_TIME_STRUCT>();
			bind_field(index, std::forward<SQL_TIME_STRUCT>(std::any_cast<SQL_TIME_STRUCT&>(value)));
			break;
		case SQL_DATE:
			value.emplace<SQL_DATE_STRUCT>();
			bind_field(index, std::forward<SQL_DATE_STRUCT>(std::any_cast<SQL_DATE_STRUCT&>(value)));
			break;
		case SQL_TIMESTAMP:
			value.emplace<SQL_TIMESTAMP_STRUCT>();
			bind_field(index, std::forward<SQL_TIMESTAMP_STRUCT>(std::any_cast<SQL_TIMESTAMP_STRUCT&>(value)));
			break;
		case SQL_INTERVAL_MONTH:
		case SQL_INTERVAL_YEAR:
		case SQL_INTERVAL_YEAR_TO_MONTH:
		case SQL_INTERVAL_DAY:
		case SQL_INTERVAL_HOUR:
		case SQL_INTERVAL_MINUTE:
		case SQL_INTERVAL_SECOND:
		case SQL_INTERVAL_DAY_TO_HOUR:
		case SQL_INTERVAL_DAY_TO_MINUTE:
		case SQL_INTERVAL_DAY_TO_SECOND:
		case SQL_INTERVAL_HOUR_TO_MINUTE:
		case SQL_INTERVAL_HOUR_TO_SECOND:
		case SQL_INTERVAL_MINUTE_TO_SECOND:
			value.emplace<SQL_INTERVAL_STRUCT>();
			bind_field(index, std::forward<SQL_INTERVAL_STRUCT>(std::any_cast<SQL_INTERVAL_STRUCT&>(value)));
			break;
		case SQL_CHAR:
			value.emplace<std::string>();
			bind_field(index, qtl::bind_string(std::any_cast<std::string&>(value)));
			break;
		case SQL_GUID:
			value.emplace<SQLGUID>();
			bind_field(index, std::forward<SQLGUID>(std::any_cast<SQLGUID&>(value)));
			break;
		case SQL_BINARY:
			value.emplace<blobbuf>();
			bind_field(index, std::forward<blobbuf>(std::any_cast<blobbuf&>(value)));
			break;
		default:
			throw odbc::error(*this, SQL_ERROR);
		}
		param_data& param = m_params[index];
		auto fetch_fun = param.m_after_fetch;
		param.m_after_fetch = [fetch_fun, &value](const param_data& p) {
			if (fetch_fun) fetch_fun(p);
			if (p.m_indicator == SQL_NULL_DATA)
				value.reset();
		};
	}

#endif // C++17
	template<typename Types>
	void execute(const Types& params)
	{
		SQLSMALLINT count=0;
		verify_error(SQLNumParams(m_handle, &count));
		if(count>0)
		{
			m_params.resize(count);
			qtl::bind_params(*this, params);
		}

		SQLRETURN ret=SQLExecute(m_handle);
		verify_error(ret);
		if(ret==SQL_NEED_DATA)
		{
			SQLPOINTER token;
			size_t i=0;
			ret=SQLParamData(m_handle, &token);
			verify_error(ret);
			while(ret==SQL_NEED_DATA)
			{
				while(i!=count)
				{
					if(&m_params[i]==token)
					{
						if(m_params[i].m_after_fetch)
							m_params[i].m_after_fetch(m_params[i]);
						break;
					}
					++i;
				}
				ret=SQLParamData(m_handle, &token);
				verify_error(ret);
			}
		}
	}

	template<typename Types>
	bool fetch(Types&& values)
	{
		if(!m_binded_cols)
		{
			SQLSMALLINT count=0;
			verify_error(SQLNumResultCols(m_handle, &count));
			if(count>0)
			{
				m_params.resize(count);
				qtl::bind_record(*this, std::forward<Types>(values));
			}
			m_binded_cols=true;
		}
		return fetch();
	}

	bool fetch()
	{
		SQLRETURN ret=SQLFetch(m_handle);
		if(ret==SQL_SUCCESS || ret==SQL_SUCCESS_WITH_INFO)
		{
			for(const param_data& data : m_params)
			{
				if(data.m_after_fetch)
					data.m_after_fetch(data);
			}
			return true;
		}
		verify_error(ret);
		return false;
	}

	bool next_result()
	{
		SQLRETURN ret;
		SQLSMALLINT count=0;
		m_binded_cols=false;
		do
		{
			ret=SQLMoreResults(m_handle);
			if(ret==SQL_ERROR || ret==SQL_INVALID_HANDLE)
				verify_error(ret);
			verify_error(SQLNumResultCols(m_handle, &count));
		}while(count==0);
		return ret==SQL_SUCCESS || ret==SQL_SUCCESS_WITH_INFO;
	}

	SQLLEN affetced_rows()
	{
		SQLLEN count=0;
		verify_error(SQLRowCount(m_handle, &count));
		return count;
	}

	size_t find_field(const char* name) const
	{
		SQLSMALLINT count=0;
		verify_error(SQLNumResultCols(m_handle, &count));
		for(SQLSMALLINT i=0; i!=count; i++)
		{
			SQLCHAR field_name[256]={0};
			SQLSMALLINT name_length=0;
			SQLSMALLINT data_type;
			SQLULEN column_size;
			SQLSMALLINT digits;
			SQLSMALLINT nullable;
			verify_error(SQLDescribeColA(m_handle, i, field_name, sizeof(field_name), &name_length,
				&data_type, &column_size, &digits, &nullable));
			if(strncmp((char*)field_name, name, name_length)==0)
				return i;
		}
		return -1;
	}

	/*
		ODBC do not support this function, but you can use query to instead it:
		For MS SQL Server: SELECT @@IDENTITY;
		For MySQL: SELECT LAST_INSERT_ID();
		For SQLite: SELECT last_insert_rowid();
	 */
	/*uint64_t insert_id()
	{
		assert(false);
		return 0;
	}*/

	void reset()
	{
		verify_error(SQLFreeStmt(m_handle, SQL_RESET_PARAMS));
	}

private:
	struct param_data
	{
		SQLPOINTER m_data;
		SQLLEN m_size;
		SQLLEN m_indicator;
		std::function<void(const param_data&)> m_after_fetch;

		param_data() : m_data(NULL), m_size(0), m_indicator(0) { }
	};
	SQLPOINTER m_blob_buffer;
	std::vector<param_data> m_params;
	bool m_binded_cols;
};

struct connection_parameter
{
	std::string m_name;
	std::string m_prompt;
	std::string m_value;
	std::vector<std::string> m_value_list;
	bool m_optinal;
	bool m_assigned;

	connection_parameter() : m_optinal(false), m_assigned(false) { }
	void reset()
	{
		m_name.clear();
		m_prompt.clear();
		m_value.clear();
		m_value_list.clear();
		m_optinal=false;
		m_assigned=false;
	}
};
typedef std::vector<connection_parameter> connection_parameters;

class database : public object<SQL_HANDLE_DBC>, public qtl::base_database<database, statement>
{
public:
	typedef odbc::error exception_type;

	explicit database(environment& env) : object(env.handle()), m_opened(false)
	{
	}
	database(database&& src) 
		: object(std::forward<database>(src)), m_connection(std::forward<std::string>(src.m_connection)) 
	{
		m_opened=src.m_opened;
		src.m_opened=false;
	}
	~database()
	{
		close();
	}
	database& operator=(database&& src)
	{
		if(this!=&src)
		{
			object::operator =(std::forward<database>(src));
			m_opened=src.m_opened;
			src.m_opened=false;
			m_connection=std::forward<std::string>(src.m_connection);
		}
		return *this;
	}

	void open(const char* server_name, size_t server_name_length, 
		const char* user_name, size_t user_name_length, const char* password, size_t password_length)
	{
		if (m_opened) close();
		verify_error(SQLConnectA(m_handle, (SQLCHAR*)server_name, server_name_length, (SQLCHAR*)user_name, user_name_length, (SQLCHAR*)password, password_length));
		m_opened = true;
	}
	void open(const char* server_name, const char* user_name, const char* password)
	{
		verify_error(SQLConnectA(m_handle, (SQLCHAR*)server_name, SQL_NTS, (SQLCHAR*)user_name, SQL_NTS, (SQLCHAR*)password, SQL_NTS));
	}
	void open(const std::string& server_name, const std::string& user_name, const std::string& password)
	{
		open(server_name.data(), server_name.size(), user_name.data(), user_name.size(), password.data(), password.size());
	}
	void open(const char* input_text, size_t text_length = SQL_NTS, SQLSMALLINT driver_completion = SQL_DRIVER_NOPROMPT, SQLHWND hwnd = NULL)
	{
		m_connection.resize(512);
		SQLSMALLINT out_len;
		if (m_opened) close();
		verify_error(SQLDriverConnectA(m_handle, hwnd, (SQLCHAR*)input_text, (SQLSMALLINT)text_length,
			(SQLCHAR*)m_connection.data(), (SQLSMALLINT)m_connection.size(), &out_len, driver_completion));
		m_connection.resize(out_len);
		m_opened = true;
	}
	void open(const std::string& input_text, SQLSMALLINT driver_completion = SQL_DRIVER_NOPROMPT, SQLHWND hwnd = NULL)
	{
		open(input_text.data(), input_text.size(), driver_completion, hwnd);
	}
	void open(SQLHWND hwnd, SQLSMALLINT driver_completion = SQL_DRIVER_COMPLETE)
	{
		open("", SQL_NTS, driver_completion, hwnd);
	}
	// InputPred like:
	// bool input_parameters(connection_parameters& parameters);
	template<typename InputPred>
	void open(const char* connection_text, size_t text_length, InputPred&& pred)
	{
		SQLSMALLINT length = 0;
		SQLINTEGER ret = SQL_SUCCESS;
		std::string input_text;
		if (m_opened) close();
		if (text_length == SQL_NTS)
			input_text = connection_text;
		else
			input_text.assign(connection_text, text_length);
		m_connection.resize(1024);
		while ((ret = SQLBrowseConnectA(m_handle, (SQLCHAR*)input_text.data(), SQL_NTS,
			(SQLCHAR*)m_connection.data(), m_connection.size(), &length)) == SQL_NEED_DATA)
		{
			connection_parameters parameters;
			parse_browse_string(m_connection.data(), length, parameters);
			if (!pred(parameters))
				throw error(SQL_NEED_DATA, "User cancel operation.");
			input_text = create_connection_text(parameters);
		}
		if (ret == SQL_ERROR || ret == SQL_SUCCESS_WITH_INFO)
			verify_error(ret);
		m_opened = true;
	}
	template<typename InputPred>
	void open(const char* connection_text, InputPred&& pred)
	{
		open(connection_text, SQL_NTS, std::forward<InputPred>(pred));
	}
	template<typename InputPred>
	void open(const std::string& connection_text, InputPred&& pred)
	{
		open(connection_text.data(), connection_text.size(), std::forward<InputPred>(pred));
	}
	void close()
	{
		if(m_opened)
		{
			verify_error(SQLDisconnect(m_handle));
			m_opened=false;
		}
	}

	void set_attribute(SQLINTEGER attr, SQLINTEGER value)
	{
		verify_error(SQLSetConnectAttr(m_handle, attr, (SQLPOINTER)value, 0));
	}
	void set_attribute(SQLINTEGER attr, const char* value)
	{
		verify_error(SQLSetConnectAttr(m_handle, attr, (SQLPOINTER)value, SQL_NTS));
	}
	void set_attribute(SQLINTEGER attr, const std::string& value)
	{
		verify_error(SQLSetConnectAttr(m_handle, attr, (SQLPOINTER)value.data(), value.size()));
	}
	void get_attribute(SQLINTEGER attr, SQLINTEGER& value) const
	{
		verify_error(SQLGetConnectAttr(m_handle, attr, &value, sizeof(SQLINTEGER), 0));
	}
	void get_info(SQLSMALLINT info, std::string& value, SQLSMALLINT size=SQL_MAX_OPTION_STRING_LENGTH) const
	{
		value.resize(size);
		verify_error(SQLGetInfo(m_handle, info, (SQLPOINTER)value.data(), size, &size));
		value.resize(size);
	}

	void auto_commit(bool on)
	{
		set_attribute(SQL_ATTR_AUTOCOMMIT, on ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF);
	}
	void begin_transaction()
	{
		auto_commit(false);
	}
	void rollback()
	{
		verify_error(SQLEndTran(handler_type, m_handle, SQL_ROLLBACK));
		auto_commit(true);
	}
	void commit()
	{
		verify_error(SQLEndTran(handler_type, m_handle, SQL_COMMIT));
		auto_commit(true);
	}

	std::string dbms_name() const
	{
		std::string name;
		get_info(SQL_DBMS_NAME, name);
		return name;
	}
	std::string server_name() const
	{
		std::string name;
		get_info(SQL_SERVER_NAME, name);
		return name;
	}
	std::string user_name() const
	{
		std::string name;
		get_info(SQL_USER_NAME, name);
		return name;
	}
	std::string db_name() const
	{
		std::string name;
		get_info(SQL_DATABASE_NAME, name);
		return name;
	}
	const std::string& connection_text() const
	{
		return m_connection;
	}

	bool is_alive()
	{
		SQLINTEGER value;
		get_attribute(SQL_ATTR_CONNECTION_DEAD, value);
		return value == SQL_CD_FALSE;
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

	void simple_execute(const char* query_text, size_t text_length=SQL_NTS)
	{
		statement command(*this);
		SQLRETURN ret=SQLExecDirectA(command.handle(), (SQLCHAR*)query_text, text_length);
		if(ret!=SQL_SUCCESS && ret!=SQL_NO_DATA)
			verify_error(ret);
	}
	void simple_execute(const std::string& query_text)
	{
		simple_execute(query_text.data(), query_text.size());
	}

private:
	bool m_opened;
	std::string m_connection;

	void parse_browse_string(const char* output_text, size_t text_length, connection_parameters& parameters);
	std::string create_connection_text(const connection_parameters& parameters);
};

struct date : public SQL_DATE_STRUCT
{
	date()
	{
		memset(this, 0, sizeof(SQL_DATE_STRUCT));
	}
};

struct time : public SQL_TIME_STRUCT
{
	time()
	{
		memset(this, 0, sizeof(SQL_TIME_STRUCT));
	}
};

struct timestamp : public SQL_TIMESTAMP_STRUCT
{
	timestamp()
	{
		memset(this, 0, sizeof(SQL_TIMESTAMP_STRUCT));
	}
	timestamp(struct tm& tm)
	{
		year=tm.tm_year+1900;
		month=tm.tm_mon+1;
		day=tm.tm_mday;
		hour=tm.tm_hour;
		minute=tm.tm_min;
		second=tm.tm_sec;
	}
	timestamp(time_t value)
	{
		struct tm tm;
#if defined(_MSC_VER)
		localtime_s(&tm, &value);
#elif defined(_POSIX_VERSION)
		localtime_r(&value, &tm);
#else
		tm=*localtime(&value);
#endif
		new(this)timestamp(tm);
	}
	timestamp(const timestamp& src)
	{
		memcpy(this, &src, sizeof(SQL_TIMESTAMP_STRUCT));
	}
	timestamp& operator=(const timestamp& src)
	{
		if(this!=&src)
			memcpy(this, &src, sizeof(SQL_TIMESTAMP_STRUCT));
		return *this;
	}

	static timestamp now()
	{
		time_t value;
		::time(&value);
		return timestamp(value);
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
	timeval get_timeval() const
	{
		timeval tv;
		struct tm tm;
		tv.tv_sec=as_tm(tm);
		tv.tv_usec=fraction/1000;
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

template<SQLSMALLINT Type>
inline error::error(const object<Type>& h, SQLINTEGER code)
{
	m_errno=code;
	if(code==SQL_ERROR || code==SQL_SUCCESS_WITH_INFO)
	{
		SQLSMALLINT i=0;
		SQLINTEGER err=SQL_SUCCESS;
		SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
		SQLCHAR state[SQL_SQLSTATE_SIZE+1];
		std::ostringstream oss;
		while(SQLGetDiagRecA(object<Type>::handler_type, h.handle(), ++i, state, &err, 
			message, SQL_MAX_MESSAGE_LENGTH, NULL)==SQL_SUCCESS)
		{
			oss<<"["<<state<<"] ("<<err<<") "<<message<<std::endl;
		}
		m_errmsg=oss.str();
	}
	else if(code==SQL_INVALID_HANDLE)
	{
		m_errmsg="Invalid handle.";
	}
}

inline void database::parse_browse_string(const char* output_text, size_t text_length, connection_parameters& parameters)
{
	enum { part_name, part_prompt, part_list, part_value };
	const char* sp=output_text;
	const char* token=sp;
	connection_parameter parameter;
	int part_type=part_name;
	while(sp!=output_text+text_length)
	{
		switch(*sp)
		{
		case ';':
			parameters.emplace_back(parameter);
			parameter.reset();
			part_type=part_name;
			token=sp+1;
			break;
		case '=':
			if(part_type==part_prompt)
				parameter.m_prompt.assign(token, sp-token);
			part_type=part_value;
			token=sp+1;
			break;
		case ':':
			if(part_type==part_name)
				parameter.m_name.assign(token, sp-token);
			part_type=part_prompt;
			token=sp+1;
			break;
		case '{':
			part_type=part_list;
			parameter.m_value_list.clear();
			token=sp+1;
			break;;
		case '}':
		case ',':
			if(part_type==part_list)
				parameter.m_value_list.emplace_back(token, sp-token);
			token=sp+1;
			break;
		case '*':
			if(part_type==part_name && token==sp)
			{
				parameter.m_optinal=true;
				token=sp+1;
			}
			break;
		case '?':
			token=sp+1;
			break;
		}
		++sp;
	}
	if(!parameter.m_name.empty())
		parameters.emplace_back(parameter);
}

inline std::string database::create_connection_text(const connection_parameters& parameters)
{
	std::ostringstream oss;
	for(auto& parameter : parameters)
	{
		if(parameter.m_assigned)
			oss<<parameter.m_name<<'='<<parameter.m_value<<';';
	}
	return oss.str();
}

inline statement::statement(database& db)
	: object(db.handle()), m_blob_buffer(NULL), m_binded_cols(false)
{
}

} //odbc

#ifdef _WIN32

namespace mssql
{

class database : public odbc::database
{
public:
	explicit database(odbc::environment& env) : odbc::database(env) { }
	database(database&& src) : odbc::database(std::forward<database>(src)) { }

	void open(const char* server, const char* db=NULL, const char* user=NULL, const char* password=NULL)
	{
		std::ostringstream oss;
		oss<<"DRIVER={SQL Server};SERVER="<<server<<";";
		if(user==NULL)
			oss<<"UID=;PWD=;Trusted_Connection=yes;";
		else
		{
			oss<<"UID="<<user<<";PWD=";
			if(password) oss<<password;
			oss<<";Trusted_Connection=no;";
		}
		oss<<"DATABASE="<<db;
		odbc::database::open(oss.str());
	}
};

} //mssql

namespace msaccess
{

class database : public odbc::database
{
public:
	explicit database(odbc::environment& env) : odbc::database(env) { }
	database(database&& src) : odbc::database(std::forward<database>(src)) { }

	void open(const char* filename, const char* user=NULL, const char* password=NULL)
	{
		std::ostringstream oss;
		oss<<"DRIVER={Microsoft Access Driver};DBQ="<<filename;
		if(user) oss<<";UID:"<<user;
		if(password) oss<<";PWD="<<password;
		odbc::database::open(oss.str());
	}
};

} //msaccess

#endif //_WIN32

}

#endif //_QTL_ODCB_H_
