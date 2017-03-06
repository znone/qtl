#ifndef _QTL_SQLITE_H_
#define _QTL_SQLITE_H_

#include "sqlite3.h"
#include <algorithm>
#include <array>
#include "qtl_common.hpp"

namespace qtl
{

namespace sqlite
{

class error : public std::exception
{
public:
	explicit error(int error_code) : m_errno(error_code)
	{
		m_errmsg=sqlite3_errstr(error_code);
	}
	explicit error(sqlite3* db)
	{
		m_errno=sqlite3_errcode(db);
		m_errmsg=sqlite3_errmsg(db);
	}
	error(const error& src) = default;
	virtual ~error() throw() { }
	virtual const char* what() const throw() override { return m_errmsg; }
	int code() const throw() { return m_errno; }

private:
	int m_errno;
	const char* m_errmsg;
};

class statement final
{
public:
	statement() : m_stmt(NULL), m_fetch_result(SQLITE_OK) { }
	statement(const statement&) = delete;
	statement(statement&& src) 
		: m_stmt(src.m_stmt), m_fetch_result(src.m_fetch_result)
	{
		src.m_stmt=NULL;
		src.m_fetch_result=SQLITE_OK;
	}
	statement& operator=(const statement&) = delete;
	statement& operator=(statement&& src)
	{
		if(this!=&src)
		{
			m_stmt=src.m_stmt;
			m_fetch_result=src.m_fetch_result;
			src.m_stmt=NULL;
			src.m_fetch_result=SQLITE_OK;
		}
		return *this;
	}
	~statement() 
	{
		close();
	}

	void open(sqlite3* db, const char* query_text, size_t text_length=-1)
	{
		close();
		verify_error(sqlite3_prepare_v2(db, query_text, (int)text_length, &m_stmt, NULL));
	}

	void close()
	{
		if(m_stmt)
		{
			sqlite3_finalize(m_stmt);
			m_stmt=NULL;
		}
	}

	void bind_param(int index, int value)
	{
		verify_error(sqlite3_bind_int(m_stmt, index+1, value));
	}
	void bind_param(int index, int64_t value)
	{
		verify_error(sqlite3_bind_int64(m_stmt, index+1, value));
	}
	void bind_param(int index, double value)
	{
		verify_error(sqlite3_bind_double(m_stmt, index+1, value));
	}
	void bind_param(int index, const char* value, size_t n=-1)
	{
		if(value)
			verify_error(sqlite3_bind_text(m_stmt, index+1, value, (int)n, NULL));
		else
			verify_error(sqlite3_bind_null(m_stmt, index+1));
	}
	void bind_param(int index, const wchar_t* value, size_t n=-1)
	{
		if(value)
			verify_error(sqlite3_bind_text16(m_stmt, index+1, value, (int)n, NULL));
		else
			verify_error(sqlite3_bind_null(m_stmt, index+1));
	}
	void bind_param(int index, const std::string& value)
	{
		bind_param(index, value.data(), value.size());
	}
	void bind_param(int index, const std::wstring& value)
	{
		bind_param(index, value.data(), value.size());
	}
	void bind_param(int index, const const_blob_data& value)
	{
		if(value.size)
		{
			if(value.data)
				verify_error(sqlite3_bind_blob(m_stmt, index+1, value.data, (int)value.size, NULL));
			else
				verify_error(sqlite3_bind_zeroblob(m_stmt, index+1, (int)value.size));
		}
		else
			verify_error(sqlite3_bind_null(m_stmt, index+1));
	}
	void bind_zero_blob(int index, int n)
	{
		verify_error(sqlite3_bind_zeroblob(m_stmt, index+1, (int)n));
	}
	void bind_param(int index, qtl::null)
	{
		verify_error(sqlite3_bind_null(m_stmt, index+1));
	}
	void bind_param(int index, std::nullptr_t)
	{
		verify_error(sqlite3_bind_null(m_stmt, index+1));
	}
	int get_parameter_count() const
	{
		return sqlite3_bind_parameter_count(m_stmt);
	}
	const char* get_parameter_name(int i) const
	{
		return sqlite3_bind_parameter_name(m_stmt, i);
	}
	int get_parameter_index(const char* param_name) const
	{
		return sqlite3_bind_parameter_index(m_stmt, param_name);
	}

	template<class Type>
	void bind_field(size_t index, Type&& value)
	{
		get_value((int)index, std::forward<Type>(value));
	}
	template<class Type>
	void bind_field(size_t index, qtl::indicator<Type>&& value)
	{
		int type=get_column_type(index);
		value.length=0;
		value.is_truncated=false;
		qtl::bind_field(*this, index, value.data);
		if(type==SQLITE_NULL)
		{
			value.is_null=true;
		}
		else
		{
			value.is_null=false;
			if(type==SQLITE_TEXT || type==SQLITE_BLOB)
				value.length=sqlite3_column_bytes(m_stmt, index);
		}
	}
	void bind_field(size_t index, char* value, size_t length)
	{
		size_t col_length=get_column_length((int)index);
		if(col_length>0)
			strncpy(value, (const char*)sqlite3_column_text(m_stmt, (int)index), std::min(length, col_length+1));
		else
			memset(value, 0, length*sizeof(char));
	}
	void bind_field(size_t index, wchar_t* value, size_t length)
	{
		size_t col_length=sqlite3_column_bytes16(m_stmt, (int)index);
		if(col_length>0)
			wcsncpy(value, (const wchar_t*)sqlite3_column_text16(m_stmt, (int)index), std::min(length, col_length+1));
		else
			memset(value, 0, length*sizeof(wchar_t));
	}
	template<size_t N>
	void bind_field(size_t index, char (&&value)[N])
	{
		bind_field(index, value, N);
	}
	template<size_t N>
	void bind_field(size_t index, wchar_t (&&value)[N])
	{
		bind_field(index, value, N);
	}
	template<size_t N>
	void bind_field(size_t index, std::array<char, N>&& value)
	{
		bind_field(index, value.data(), value.size());
	}
	template<size_t N>
	void bind_field(size_t index, std::array<wchar_t, N>&& value)
	{
		bind_field(index, value.data(), value.size());
	}

	bool fetch()
	{
		m_fetch_result=sqlite3_step(m_stmt);
		switch(m_fetch_result)
		{
		case SQLITE_ROW:
			return true;
		case SQLITE_DONE:
			return false;
		default:
			throw sqlite::error(m_fetch_result);
		}
	}
	int get_column_count() const
	{
		return sqlite3_column_count(m_stmt);
	}
	const char* get_column_name(int col) const
	{
		return sqlite3_column_name(m_stmt, col);
	}
	void get_value(int col, int&& value) const
	{
		value=sqlite3_column_int(m_stmt, col);
	}
	void get_value(int col, int64_t&& value) const
	{
		value=sqlite3_column_int64(m_stmt, col);
	}
	void get_value(int col, double&& value) const
	{
		value=sqlite3_column_double(m_stmt, col);
	}
	const unsigned char* get_value(int col) const
	{
		return sqlite3_column_text(m_stmt, col);
	}
	void get_value(int col, std::string&& value) const
	{
		int bytes=sqlite3_column_bytes(m_stmt, col);
		if(bytes>0)
			value.assign((const char*)sqlite3_column_text(m_stmt, col), bytes/sizeof(char));
		else
			value.clear();
	}
	void get_value(int col, std::wstring&& value) const
	{
		int bytes=sqlite3_column_bytes16(m_stmt, col);
		if(bytes>0)
			value.assign((const wchar_t*)sqlite3_column_text16(m_stmt, col), bytes/sizeof(wchar_t));
		else
			value.clear();
	}
	void get_value(int col, const_blob_data&& value) const
	{
		value.data=sqlite3_column_blob(m_stmt, col);
		value.size=sqlite3_column_bytes(m_stmt, col);
	}
	void get_value(int col, blob_data&& value) const
	{
		const void* data=sqlite3_column_blob(m_stmt, col);
		size_t size=sqlite3_column_bytes(m_stmt, col);
		if(value.size<size)
			throw std::out_of_range("no enough buffer to receive blob data.");
		memcpy(value.data, data, size);
		value.size=size;
	}
	void get_value(int col, std::ostream&& value) const
	{
		const void* data=sqlite3_column_blob(m_stmt, col);
		size_t size=sqlite3_column_bytes(m_stmt, col);
		if(size>0)
			value.write((const char*)data, size);
	}

	int get_column_length(int col) const
	{
		return sqlite3_column_bytes(m_stmt, col);
	}
	int get_column_type(int col) const
	{
		return sqlite3_column_type(m_stmt, col);
	}
	void clear_bindings()
	{
		sqlite3_clear_bindings(m_stmt);
	}
	void reset()
	{
		sqlite3_reset(m_stmt);
	}

	template<typename Types>
	void execute(const Types& params)
	{
		unsigned long count=get_parameter_count();
		if(count>0)
		{
			qtl::bind_params(*this, params);
		}
		fetch();
	}

	template<typename Types>
	bool fetch(Types&& values)
	{
		bool result=false;
		if(m_fetch_result==SQLITE_OK)
			fetch();
		if(m_fetch_result==SQLITE_ROW)
		{
			result=true;
			qtl::bind_record(*this, std::forward<Types>(values));
			m_fetch_result=SQLITE_OK;
		}
		return result;
	}

	int affetced_rows()
	{
		sqlite3* db=sqlite3_db_handle(m_stmt);
		return db ? sqlite3_changes(db) : 0;
	}

	int64_t insert_id()
	{
		sqlite3* db=sqlite3_db_handle(m_stmt);
		return db ? sqlite3_last_insert_rowid(db) : 0;
	}

protected:
	sqlite3_stmt* m_stmt;
	int m_fetch_result;
	void verify_error(int e)
	{
		if(e!=SQLITE_OK) throw error(e);
	}
};

class database final : public qtl::base_database<database, statement>
{
public:	
	database() : m_db(NULL) { }
	~database() { close(); }
	database(const database&) = delete;
	database(database&& src)
	{
		m_db=src.m_db;
		src.m_db=NULL;
	}
	database& operator=(const database&) = delete;
	database& operator=(database&& src)
	{
		if(this!=&src)
		{
			close();
			m_db=src.m_db;
			src.m_db=NULL;
		}
	}

	void open(const char *filename, int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)
	{
		int result=sqlite3_open_v2(filename, &m_db, flags, NULL);
		if(result!=SQLITE_OK)
			throw sqlite::error(result);
	}
	void open(const wchar_t *filename)
	{
		int result=sqlite3_open16(filename, &m_db);
		if(result!=SQLITE_OK)
			throw sqlite::error(result);
	}
	void close()
	{
		if(m_db)
		{
			sqlite3_close_v2(m_db);
			m_db=NULL;
		}
	}

	statement open_command(const char* query_text, size_t text_length)
	{
		statement stmt;
		stmt.open(handle(), query_text, text_length);
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

	void simple_execute(const char* lpszSql)
	{
		int result=sqlite3_exec(m_db, lpszSql, NULL, NULL, NULL);
		if(result!=SQLITE_OK)
			throw sqlite::error(result);
	}

	void begin_transaction()
	{
		simple_execute("BEGIN TRANSACTION");
	}
	void commit()
	{
		simple_execute("COMMIT TRANSACTION");
	}
	void rollback()
	{
		simple_execute("ROLLBACK TRANSACTION");
	}

	bool is_alive()
	{
#ifdef _WIN32
		return true;
#else
		int has_moved=0;
		int result=sqlite3_file_control(m_db, NULL, SQLITE_FCNTL_HAS_MOVED, &has_moved);
		if(result!=SQLITE_OK)
			throw sqlite::error(result);
		return has_moved==0;
#endif //_WIN32
	}
	const char* errmsg() const { return sqlite3_errmsg(m_db); }
	int error() const { return sqlite3_errcode(m_db); }
	uint64_t insert_id() { return sqlite3_last_insert_rowid(m_db); }
	sqlite3* handle() { return m_db; }

protected:
	sqlite3* m_db;
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

}

}

#endif //_QTL_SQLITE_H_
