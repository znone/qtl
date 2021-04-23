#ifndef _QTL_SQLITE_H_
#define _QTL_SQLITE_H_

#include "sqlite3.h"
#include <algorithm>
#include <array>
#include <sstream>
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
		: m_stmt(src.m_stmt), m_fetch_result(src.m_fetch_result),
		m_tail_text(std::forward<std::string>(src.m_tail_text))
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
			m_tail_text=std::forward<std::string>(src.m_tail_text);
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
		const char* tail=NULL;
		close();
		verify_error(sqlite3_prepare_v2(db, query_text, (int)text_length, &m_stmt, &tail));
		if(tail!=NULL)
		{
			if(text_length==-1)
				m_tail_text.assign(tail);
			else
				m_tail_text.assign(tail, query_text+text_length);
		}
		else
			m_tail_text.clear();
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
	//void bind_zero_blob(int index, sqlite3_uint64 n)
	//{
	//	verify_error(sqlite3_bind_zeroblob64(m_stmt, index+1, (int)n));
	//}
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

#ifdef _QTL_ENABLE_CPP17

	template<typename T>
	inline void bind_field(size_t index, std::optional<T>&& value)
	{
		int type = get_column_type(index);
		if (type == SQLITE_NULL)
		{
			value.reset();
		}
		else
		{
			qtl::bind_field(*this, index, *value);
		}
	}

	inline void bind_field(size_t index, std::any&& value)
	{
		int type = get_column_type(index);
		switch(type)
		{
		case SQLITE_NULL:
			value.reset();
		case SQLITE_INTEGER:
			value = sqlite3_column_int64(m_stmt, index);
			break;
		case SQLITE_FLOAT:
			value = sqlite3_column_double(m_stmt, index);
			break;
		case SQLITE_TEXT:
			value.emplace<std::string_view>((const char*)sqlite3_column_text(m_stmt, index), sqlite3_column_bytes(m_stmt, index));
			break;
		case SQLITE_BLOB:
			value.emplace<const_blob_data>(sqlite3_column_text(m_stmt, index), sqlite3_column_bytes(m_stmt, index));
			break;
		default:
			throw sqlite::error(SQLITE_MISMATCH);
		}
	}

#endif // C++17

	size_t find_field(const char* name) const
	{
		size_t count=get_column_count();
		for(size_t i=0; i!=count; i++)
		{
			if(strcmp(get_column_name(i), name)==0)
				return i;
		}
		return -1;
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

	template<typename CharT>
	const CharT* get_text_value(int col) const;

	template<typename T>
	void get_value(int col, qtl::bind_string_helper<T>&& value) const
	{
		typedef typename qtl::bind_string_helper<T>::char_type char_type;
		int bytes=sqlite3_column_bytes(m_stmt, col);
		if(bytes>0)
			value.assign(get_text_value<char_type>(col), bytes/sizeof(char_type));
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

	bool next_result()
	{
		sqlite3* db=sqlite3_db_handle(m_stmt);
		int count=0;
		do
		{
			trim_string(m_tail_text, " \t\r\n");
			if(!m_tail_text.empty())
			{
				open(db, m_tail_text.data(), m_tail_text.size());
				count=sqlite3_column_count(m_stmt);
				m_fetch_result=SQLITE_OK;
			}
		}while(!m_tail_text.empty() && count==0);
		return count>0;;
	}

	int affetced_rows() const
	{
		sqlite3* db=sqlite3_db_handle(m_stmt);
		return db ? sqlite3_changes(db) : 0;
	}

	int64_t insert_id() const
	{
		sqlite3* db=sqlite3_db_handle(m_stmt);
		return db ? sqlite3_last_insert_rowid(db) : 0;
	}

protected:
	sqlite3_stmt* m_stmt;
	std::string m_tail_text;
	
	int m_fetch_result;
	void verify_error(int e)
	{
		if(e!=SQLITE_OK) throw error(e);
	}
};

class database final : public qtl::base_database<database, statement>
{
public:	
	typedef sqlite::error exception_type;

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
		return *this;
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

// stream for blob field

class blobbuf : public std::streambuf
{
public:
	blobbuf() 
	{
		init();
	}
	blobbuf(const blobbuf&) = delete;
	blobbuf(blobbuf&& src) : std::streambuf(std::move(src))
	{
		init();
		swap(src);
	}
	virtual ~blobbuf()
	{
		if(m_blob)
		{
			close();
		}
	}

	blobbuf& operator=(const blobbuf&) = delete;
	blobbuf& operator=(blobbuf&& src)
	{
		if(this!=&src)
		{
			reset_back();
			close();
			swap(src);
		}
		return *this;
	}

	void swap( blobbuf& other )
	{
		std::swap(m_blob, other.m_blob);
		std::swap(m_inbuf, other.m_inbuf);
		std::swap(m_outbuf, other.m_outbuf);
		std::swap(m_size, other.m_size);
		std::swap(m_inpos, other.m_inpos);
		std::swap(m_outpos, other.m_outpos);

		std::streambuf::swap(other);
		std::swap(m_back_char, other.m_back_char);
		if(eback() == &other.m_back_char)
			set_back();
		else
			reset_back();

		if(other.eback()==&m_back_char)
			other.set_back();
		else
			other.reset_back();
	}

	static void init_blob(database& db, const char* table, const char* column, int64_t row, int length)
	{
		statement stmt;
		std::ostringstream oss;
		oss<< "UPDATE " << table << " SET " << column << "=? WHERE rowid=?";
		stmt.open(db.handle(), oss.str().data());
		stmt.bind_zero_blob(0, length);
		stmt.bind_param(1, row);
		stmt.fetch();
	}
	static void init_blob(database& db, const std::string& table, const std::string& column, int64_t row, int length)
	{
		return init_blob(db, table.c_str(), column.c_str(), row, length);
	}

	bool is_open() const { return m_blob!=nullptr; }

	blobbuf* open(database& db, const char* table, const char* column, sqlite3_int64 row, 
		std::ios_base::openmode mode, const char* dbname="main")
	{
		int flags=0;
		if(mode&std::ios_base::out) flags=1;
		if(sqlite3_blob_open(db.handle(), dbname, table, column, row, flags, &m_blob)==SQLITE_OK)
		{
			m_size=sqlite3_blob_bytes(m_blob)/sizeof(char);
			// prepare buffer
			size_t bufsize=std::min<size_t>(default_buffer_size, m_size);
			if(mode&std::ios_base::in)
			{
				m_inbuf.resize(bufsize);
				m_inpos=0;
				setg(m_inbuf.data(), m_inbuf.data(), m_inbuf.data());
			}
			if(mode&std::ios_base::out)
			{
				m_outbuf.resize(bufsize);
				m_outpos=0;
				setp(m_outbuf.data(), m_outbuf.data()+bufsize);
			}
		}
		return this;
	}
	blobbuf* open(database& db, const std::string& table, const std::string& column, sqlite3_int64 row, 
		std::ios_base::openmode mode, const char* dbname="main")
	{
		return open(db, table.c_str(), column.c_str(), row, mode, dbname);
	}

	blobbuf* close()
	{
		if(m_blob==nullptr) 
			return nullptr;

		overflow();
		sqlite3_blob_close(m_blob);
		init();
		return this;
	}

	std::streamoff size() const { return std::streamoff(m_size); }

	void flush() 
	{
		if(m_blob) 
			overflow();
	}

protected:
	enum { default_buffer_size = 4096 };

	virtual pos_type seekoff( off_type off, std::ios_base::seekdir dir,
		std::ios_base::openmode which = std::ios_base::in | std::ios_base::out ) override
	{
		if(!is_open())
			return pos_type(off_type(-1));

		pos_type pos=0;
		if(which&std::ios_base::out)
		{
			pos=seekoff(m_outpos, off, dir);
		}
		else if(which&std::ios_base::in)
		{
			pos=seekoff(m_inpos, off, dir);
		}
		return seekpos(pos, which);
	}

	virtual pos_type seekpos( pos_type pos,
		std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override
	{
		if(!is_open())
			return pos_type(off_type(-1));
		if(pos>=m_size)
			return pos_type(off_type(-1));

		if(which&std::ios_base::out)
		{
			if(pos<m_outpos || pos>=m_outpos+off_type(egptr()-pbase()))
			{
				overflow();
				m_outpos=pos;
				setp(m_outbuf.data(), m_outbuf.data()+m_outbuf.size());
			}
			else
			{
				pbump(off_type(pos-pabs()));
			}
		}
		else if(which&std::ios_base::in)
		{
			if(pos<m_inpos || pos>=m_inpos+off_type(epptr()-eback()))
			{
				m_inpos=pos;
				setg(m_inbuf.data(), m_inbuf.data(), m_inbuf.data());
			}
			else
			{
				gbump(off_type(pos-gabs()));
			}
		}
		return pos;
	}

	virtual std::streamsize showmanyc() override
	{
		return m_size-pabs();
	}

	//reads characters from the associated input sequence to the get area 
	virtual int_type underflow() override
	{
		if(!is_open()) 
			return traits_type::eof();

		if(pptr()>pbase())
			overflow();

		off_type count=egptr()-eback();
		pos_type next_pos=0;
		if(count==0 && eback()==m_inbuf.data())
		{
			setg(m_inbuf.data(), m_inbuf.data(), m_inbuf.data()+m_inbuf.size());
			count=m_inbuf.size();
		}
		else
		{
			next_pos=m_inpos+pos_type(count);
		}
		if(next_pos>=m_size)
			return traits_type::eof();

		count=std::min(count, m_size-next_pos);
		m_inpos = next_pos;
		if(sqlite3_blob_read(m_blob, eback(), count, m_inpos)!=SQLITE_OK)
			return traits_type::eof();
		setg(eback(), eback(), eback()+count);
		return traits_type::to_int_type(*gptr());
	}

	/*//reads characters from the associated input sequence to the get area and advances the next pointer 
	virtual int_type uflow() override
	{

	}*/

	//writes characters to the associated output sequence from the put area 
	virtual int_type overflow( int_type ch = traits_type::eof() ) override
	{
		if(!is_open()) 
			return traits_type::eof();

		if(pptr()!=pbase())
		{
			size_t count = pptr()-pbase();
			if(sqlite3_blob_write(m_blob, pbase(), count, m_outpos)!=SQLITE_OK)
				return traits_type::eof();

			auto intersection = interval_intersection(m_inpos, egptr()-eback(), m_outpos, epptr()-pbase());
			if(intersection.first!=intersection.second)
			{
				commit(intersection.first, intersection.second);
			}

			m_outpos+=count;
			setp(pbase(), epptr());
		}
		if(!traits_type::eq_int_type(ch, traits_type::eof()))
		{
			char_type c = traits_type::to_char_type(ch);
			if(m_outpos>=m_size)
				return traits_type::eof();
			if(sqlite3_blob_write(m_blob, &c, 1, m_outpos)!=SQLITE_OK)
				return traits_type::eof();
			auto intersection = interval_intersection(m_inpos, egptr()-eback(), m_outpos, 1);
			if(intersection.first!=intersection.second)
			{
				eback()[intersection.first-m_inpos]=c;
			}
			m_outpos+=1;
			
		}
		return ch;
	}

	virtual int_type pbackfail( int_type c = traits_type::eof() ) override
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
	sqlite3_blob* m_blob;
	std::vector<char> m_inbuf;
	std::vector<char> m_outbuf;
	pos_type m_size;
	pos_type m_inpos;	//position in the input sequence
	pos_type m_outpos;	//position in the output sequence

	void init()
	{
		m_blob=nullptr;
		m_size=0;
		m_inpos=m_outpos=0;
		m_set_eback=m_set_egptr=nullptr;
		m_back_char=0;
		setg(nullptr, nullptr, nullptr);
		setp(nullptr, nullptr);
	}

	off_type seekoff(off_type position, off_type off, std::ios_base::seekdir dir)
	{
		off_type result=0;
		switch(dir)
		{
		case std::ios_base::beg:
			result=off;
			break;
		case std::ios_base::cur:
			result=position+off;
			break;
		case std::ios_base::end:
			result=m_size-off;
			break;
		}
		if(result>m_size)
			result=m_size;
		return result;
	}

	void reset_back()
	{	// restore buffer after putback
		if (this->eback() == &m_back_char)
			this->setg(m_set_eback, m_set_eback, m_set_egptr);
	}

	void set_back()
	{	// set up putback area
		if (this->eback() != &m_back_char)
		{	// save current get buffer
			m_set_eback = this->eback();
			m_set_egptr = this->egptr();
		}
		this->setg(&m_back_char, &m_back_char, &m_back_char + 1);
	}

	char_type *m_set_eback { nullptr };	// saves eback() during one-element putback
	char_type *m_set_egptr { nullptr };	// saves egptr()
	char_type m_back_char { 0 };

	void move_data(blobbuf& other)
	{
		m_blob=other.m_blob;
		other.m_blob=nullptr;
		m_size=other.m_size;
		other.m_size=0;
		m_inpos=other.m_inpos;
		other.m_inpos=0;
		m_outpos=other.m_outpos;
		other.m_outpos=0;
	}

	static std::pair<pos_type, pos_type> interval_intersection(pos_type first1, pos_type last1, pos_type first2, pos_type last2)
	{
		if(first1>first2)
		{
			std::swap(first1, first2);
			std::swap(last1, last2);
		}

		if(first2<last1)
			return std::make_pair(first2, std::min(last1, last2));
		else
			return std::make_pair(0, 0);
	}

	static std::pair<pos_type, pos_type> interval_intersection(pos_type first1, off_type count1, pos_type first2, off_type count2)
	{
		return interval_intersection(first1, first1+count1, first2, first2+count2);
	}

	void commit(off_type first, off_type last)
	{
		char_type* src= pbase()+(first-m_outpos);
		char_type* dest = eback()+(first-m_inpos);
		memmove(dest, src, last-first);
	}

	pos_type gabs() const // absolute offset of input pointer in blob field
	{
		return m_inpos+off_type(gptr()-eback());
	}

	pos_type pabs() const // absolute offset of output pointer in blob field
	{
		return m_outpos+off_type(pptr()-pbase());
	}
};

class iblobstream : public std::istream
{
public:
	iblobstream() : std::istream(&m_buffer) { }
	iblobstream(database& db, const char* table, const char* column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::in, const char* dbname="main")
		: std::istream(&m_buffer)
	{
		open(db, table, column, row, mode, dbname);
	}
	iblobstream(database& db, const std::string& table, const std::string& column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::in, const char* dbname="main")
		: std::istream(&m_buffer)
	{
		open(db, table, column, row, mode, dbname);
	}
	iblobstream(const iblobstream&) = delete;
	iblobstream(iblobstream&& src) : std::istream(&m_buffer), m_buffer(std::move(src.m_buffer)) { }

	iblobstream& operator=(const iblobstream&) = delete;
	iblobstream& operator=(iblobstream&& src) 
	{
		m_buffer.operator =(std::move(src.m_buffer));
	}

	bool is_open() const { return m_buffer.is_open(); }

	void open(database& db, const char* table, const char* column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::in, const char* dbname="main")
	{
		if(m_buffer.open(db, table, column, row, mode|std::ios_base::in, dbname)==nullptr)
			this->setstate(std::ios_base::failbit);
		else
			this->clear();
	}
	void open(database& db, const std::string& table, const std::string& column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::in, const char* dbname="main")
	{
		open(db, table.c_str(), column.c_str(), row, mode, dbname);
	}

	void close()
	{
		if(m_buffer.close()==nullptr)
			this->setstate(std::ios_base::failbit);
	}

	blobbuf* rdbuf() const
	{
		return const_cast<blobbuf*>(&m_buffer);
	}

	std::streamoff blob_size() const { return m_buffer.size(); }

private:
	blobbuf m_buffer;
};

class oblobstream : public std::ostream
{
public:
	oblobstream() : std::ostream(&m_buffer) { }
	oblobstream(database& db, const char* table, const char* column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::in, const char* dbname="main")
		: std::ostream(&m_buffer)
	{
		open(db, table, column, row, mode, dbname);
	}
	oblobstream(database& db, const std::string& table, const std::string& column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::in, const char* dbname="main")
		: std::ostream(&m_buffer)
	{
		open(db, table, column, row, mode, dbname);
	}
	oblobstream(const oblobstream&) = delete;
	oblobstream(oblobstream&& src) : std::ostream(&m_buffer), m_buffer(std::move(src.m_buffer)) { }

	oblobstream& operator=(const oblobstream&) = delete;
	oblobstream& operator=(oblobstream&& src) 
	{
		m_buffer.operator =(std::move(src.m_buffer));
	}

	bool is_open() const { return m_buffer.is_open(); }

	void open(database& db, const char* table, const char* column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::out, const char* dbname="main")
	{
		if(m_buffer.open(db, table, column, row, mode|std::ios_base::out, dbname)==nullptr)
			this->setstate(std::ios_base::failbit);
		else
			this->clear();
	}
	void open(database& db, const std::string& table, const std::string& column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::out, const char* dbname="main")
	{
		open(db, table.c_str(), column.c_str(), row, mode, dbname);
	}

	void close()
	{
		if(m_buffer.close()==nullptr)
			this->setstate(std::ios_base::failbit);
	}

	blobbuf* rdbuf() const
	{
		return const_cast<blobbuf*>(&m_buffer);
	}

	std::streamoff blob_size() const { return m_buffer.size(); }

private:
	blobbuf m_buffer;
};

class blobstream : public std::iostream
{
public:
	blobstream() : std::iostream(&m_buffer) { }
	blobstream(database& db, const char* table, const char* column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::in, const char* dbname="main")
		: std::iostream(&m_buffer) 
	{
		open(db, table, column, row, mode, dbname);
	}
	blobstream(database& db, const std::string& table, const std::string& column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::in, const char* dbname="main")
		: std::iostream(&m_buffer) 
	{
		open(db, table, column, row, mode, dbname);
	}
	blobstream(const blobstream&) = delete;
	blobstream(blobstream&& src) : std::iostream(&m_buffer), m_buffer(std::move(src.m_buffer)) { }

	blobstream& operator=(const blobstream&) = delete;
	blobstream& operator=(blobstream&& src) 
	{
		m_buffer.operator =(std::move(src.m_buffer));
	}

	bool is_open() const { return m_buffer.is_open(); }

	void open(database& db, const char* table, const char* column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::in|std::ios_base::out, const char* dbname="main")
	{
		if(m_buffer.open(db, table, column, row, mode|std::ios_base::in|std::ios_base::out, dbname)==nullptr)
			this->setstate(std::ios_base::failbit);
		else
			this->clear();
	}
	void open(database& db, const std::string& table, const std::string& column, sqlite3_int64 row, 
		std::ios_base::openmode mode=std::ios_base::in|std::ios_base::out, const char* dbname="main")
	{
		open(db, table.c_str(), column.c_str(), row, mode, dbname);
	}

	void close()
	{
		if(m_buffer.close()==nullptr)
			this->setstate(std::ios_base::failbit);
	}

	blobbuf* rdbuf() const
	{
		return const_cast<blobbuf*>(&m_buffer);
	}

	std::streamoff blob_size() const { return m_buffer.size(); }

private:
	blobbuf m_buffer;
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

template<>
inline const char* statement::get_text_value<char>(int col) const
{
	return (const char*)sqlite3_column_text(m_stmt, col);
}
template<>
inline const wchar_t* statement::get_text_value<wchar_t>(int col) const
{
	return (const wchar_t*)sqlite3_column_text16(m_stmt, col);
}

}

}

#endif //_QTL_SQLITE_H_
