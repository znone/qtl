# QTL
QTL is a C ++ library for accessing SQL databases and currently supports MySQL, SQLite and ODBC. QTL is a lightweight library that consists of only header files and does not require separate compilation and installation. QTL is a thin encapsulation of the database's native client interface. It can provide a friendly way of using and has performance close to using the native interface.
Using QTL requires a compiler that supports C++11.

The project [db2qtl](https://github.com/znone/db2qtl) can generate QTL code.

## Usage

### Open database

```C++
qtl::mysql::database db;
db.open("localhost", "root", "", "test");
```

### Execute SQL


#### 1. Insert

```C++
uint64_t id=db.insert("insert into test(Name, CreateTime) values(?, now())", "test_user");
```

#### 2. Update

```C++
db.execute_direct("update test set Name=? WHERE ID=?",  NULL, "other_user", id);
```
#### 3. Update multiple records

```C++
uint64_t affected=0;
auto stmt=db.open_command("insert into test(Name, CreateTime) values(?, now())");
qtl::execute(stmt, &affected, "second_user", "third_user");

```
or
```C++
stmt<<"second_user"<<"third_user";

```

#### 4. Query data and process data in callback function
The program will traverse the data set until the callback function returns false. If the callback function has no return value, it is equivalent to returning true.

```C++
db.query("select * from test where id=?", id, 
	[](uint32_t id, const std::string& name, const qtl::mysql::time& create_time) {
		printf("ID=\"%d\", Name=\"%s\"\n", id, name.data());
});
```
When the field type cannot be inferred based on the parameters of the callback function, please use query_explicit instead of query and manually specify the data type for query.

#### 5. Bind data to structures

```C++
struct TestMysqlRecord
{
	uint32_t id;
	char name[33];
	qtl::mysql::time create_time;

	TestMysqlRecord()
	{
		memset(this, 0, sizeof(TestMysqlRecord));
	}
};

namespace qtl
{
	template<>
	inline void bind_record<qtl::mysql::statement, TestMysqlRecord>(qtl::mysql::statement& command, TestMysqlRecord&& v)
	{
		qtl::bind_fields(command, v.id, v.name, v.create_time);
	}
}

db.query("select * from test where id=?", id, 
	[](const TestMysqlRecord& record) {
		printf("ID=\"%d\", Name=\"%s\"\n", record.id, record.name);
});
```
#### 6. Use member functions as query callback functions
When the record class has a member function without parameters, it can be used directly as a query callback function
```C++
struct TestMysqlRecord
{
	void print();
};

db.query("select * from test where id=?", id,
	&TestMysqlRecord::print);
```

#### 7. Accessing data using iterator

```C++
for(auto& record : db.result<TestMysqlRecord>("select * from test"))
{
	printf("ID=\"%d\", Name=\"%s\"\n", record.id, record.name);
}
```
#### 8. Indicator
You can use the indicator to get more information about the query results. The indicator contains the following members:
- data Store field data
- is_null Whether the field is empty
- length The actual length of the data
- is_truncated Whether the data is truncated
 
#### 9. std::optional and std::any
You can bind fields to std::optional and std::any in C ++ 17. When fields are null, they contain nothing, otherwise they contain the value of the field.

#### 10. Support for string types other than the standard library
In addition to the std::string provided by the standard library, other libraries also provide their own string classes, such as QT's QString and MFC/ATL's CString. qtl can also bind character fields to these types. The extension method is:
1. Implement a specialization for qtl::bind_string_helper for your string type. If this string type has the following member functions that conform to the standard library string semantics, you can skip this step: assign, clear, resize, data, size;
2. Implement a specialization for qtl::bind_field for your string type;

Because QT's QByteArray has member functions compatible with the standard library, binding to QByteArray requires only one step:
Generally, the database does not provide binding to QChar/QString, so you can only use QByteArray to receive data, and then convert it to QString.

```C++
namespace qtl
{
	template<typename Command>
	inline void bind_field(Command& command, size_t index, QByteArray&& value)
	{
		command.bind_field(index, bind_string(std::forward<QByteArray>(value)));
	}
}

```

#### 11. Reuse the same data structure in different queries
Usually you want to reuse the structure and bind it to the result set of multiple different queries. At this time qtl::bind_record is not enough. You need to implement different binding functions with qtl::custom_bind to achieve this requirement. There are the following binding functions:

```C++
void my_bind(TestMysqlRecord&& v, qtl::sqlite::statement& command)
{
	qtl::bind_field(command, "id", v.id);
	qtl::bind_field(command, 1, v.name);
	qtl::bind_field(command, 2, v.create_time);
}
```
The following code shows how to use it for queries:
```C++
db->query_explicit("select * from test where id=?", id, 
	qtl::custom_bind(TestMysqlRecord(), &my_bind),
	[](const TestMysqlRecord& record) {
		printf("ID=\"%d\", Name=\"%s\"\n", record.id, record.name);
	});
```
qtl::bind_record is not the only method. A similar requirement can be achieved through derived classes (qtl::record_with_tag).

#### 12.Execute queries that return multiple result sets
Some query statements return multiple result sets. Executing these queries using the function query will only get the first result set. To process all result sets you need to use query_multi or query_multi_with_params. query_multi does not call callback functions for queries without a result set. E.g:
```SQL
CREATE PROCEDURE test_proc()
BEGIN
	select 0, 'hello world' from dual;
	select now() from dual;
END
```
```C++
db.query_multi("call test_proc", 
	[](uint32_t i, const std::string& str) {
		printf("0=\"%d\", 'hello world'=\"%s\"\n", i, str.data());
}, [](const qtl::mysql::time& time) {
	struct tm tm;
	time.as_tm(tm);
	printf("current time is: %s\n", asctime(&tm));
});

```

#### 13. Access the database asynchronously

The database can be called asynchronously through the class async_connection. All asynchronous functions need to provide a callback function to accept the result after the operation is completed. If an error occurs during an asynchronous call, the error is returned to the caller as a parameter to the callback function.
```
qtl::mysql::async_connection connection;
connection.open(ev, [&connection](const qtl::mysql::error& e) {
	...
});

```

Asynchronous calls are done in the event loop. ev is an event loop object. QTL only proposes its requirements for the event loop and does not implement the event loop. QTL requires the event loop to provide the following interface, which is implemented by user code:
```
class EventLoop
{
public:
	// Adding a database connection to the event loop
	template<typename Connection>
	qtl::event_handler* add(Connection* connection);
	
	// Add a timeout task to the event loop
	template<typename Handler>
	qtl::event* set_timeout(const timeval& timeout, Handler&& handler);
};
```

qtl::event is an event item interface defined in QTL, and user code should also implement it:
```
struct event
{
	// IO event flag
	enum io_flags
	{
		ef_read = 0x1,
		ef_write = 0x2,
		ef_exception = 0x4,
		ef_timeout =0x8,
		ev_all = ef_read | ef_write | ef_exception
	};

	virtual ~event() { }
	// Setting up the IO processor
	virtual void set_io_handler(int flags, long timeout, std::function<void(int)>&&) = 0;
	// Remove event items from the event loop
	virtual void remove() = 0;
	// Determine if the event item is waiting for IO
	virtual bool is_busying() = 0;
};

```
Database connections are usually not thread-safe. User code should guarantee that a connection can only be used by one thread at a time.
For use this feature, GCC requires version 5 or higher.

## About MySQL

When accessing MySQL, include the header file qtl_mysql.hpp.

### MySQL parameter data binding

| Parameter Type | C++ Types |
| ------- | ------ |
| tinyint | int8_t<br/>uint8_t |
| smallint | int16_t<br/>uint16_t |
| int | int32_t<br/>uint32_t |
| bigint | int64_t<br/>uint64_t |
| float | float |
| double | double |
| char<br>varchar | const char*<br>std::string |
| blob<br>binary<br>text | qtl::const_blob_data<br>std::istream<br>qtl::blob_writer |
| date<br>time<br>datetime<br/>timestamp | qtl::mysql::time |

blob_writer is a function, which is defined as follows:
```C++
typedef std::function<void(std::ostream&)> blob_writer;
```
This function writes data to the BLOB field with a parameter of type std::ostream. Due to the limitations of the MySQL API, the stream can basically only move forward, and it is not recommended to adjust the write position at will for this stream.

### MySQL field data binding

| Field Type | C++ Types |
| ------- | ------ |
| tinyint | int8_t<br/>uint8_t |
| smallint | int16_t<br/>uint16_t |
| int | int32_t<br/>uint32_t |
| bigint | int64_t<br/>uint64_t |
| float | float |
| double | double |
| char<br>varchar | char[N]<br>std::array&lt;char, N&gt;<br>std::string<br>std::istream
| blob<br>binary<br>text | qtl::blob_data<br>std::ostream<br>qtl::blobbuf
| date<br>time<br>datetime<br>timestamp | qtl::mysql::time |

Data from BLOB fields can be read via qtl::mysql::blobbuf：
```C++
void read_blob(qtl::blobbuf& buf) {
	istream s(&buf);
	...
};
```
Because of the limitations of the MySQL API, the stream can only move forward, and it is not recommended to adjust the read position at will for this stream.

### MySQL related C++ classes
- qtl::mysql::database
Represents a MySQL database connection. The program mainly manipulates the database through this class.
- qtl::mysql::statement
Represents a MySQL query statement to implement query-related operations.
- qtl::mysql::error
Represents a MySQL error. When an operation error occurs, an exception of this type is thrown, including an error message.
- qtl::mysql::transaction
Represents a MySQL transaction operation.
- qtl::mysql::query_result
Represents a MySQL query result set, used to iterate over query results in an iterator manner.

## About SQLite

When accessing SQLite, include the header file qtl_sqlite.hpp.

### SQLite parameter data binding

| Parameter Type | C++ Types |
| ------- | ------ |
| integer | int</br>int64_t |
| real | double |
| text | const char*<br>std::string<br>std::wstring |
| blob | qtl::const_blob_data |


### SQLite field data binding

| Field Type | C++ Types |
| ------- | ------ |
| integer | int</br>int64_t |
| real | double |
| text | char[N]<br>std::array&lt;char, N&gt;<br>std::string<br>std::wstring |
| blob | qtl::const_blob_data<br>qtl::blob_data<br>std::ostream |

When receiving blob data with qtl::const_blob_data, it directly returns the data address given by SQLite. When receiving blob data with qtl::blob_data, the data is copied to the address specified by qtl::blob_data.

### C ++ classes related to SQLite
- qtl::sqlite::database
Represents a SQLite database connection. The program mainly manipulates the database through this class.
- qtl::sqlite::statement
Represents a SQLite query statement to implement query-related operations.
- qtl::sqlite::error
Represents a SQLite error. When an operation error occurs, an exception of this type is thrown, including the error information.
- qtl::sqlite::transaction
Represents a SQLite transaction operation.
- qtl::sqlite::query_result
Represents a SQLite query result set, used to iterate over the query results in an iterator manner.

### Blob field in SQLite

Through QTL, you can access the SQLite BLOB field through the standard stream.
The following code first fills the BLOB field with the numbers 0-9, then reads the field content again and displays it to the screen.

```C++
int64_t id=db->insert("INSERT INTO test_blob (Filename, Content, MD5) values(?, ?, ?)",
	forward_as_tuple("sample", qtl::const_blob_data(nullptr, 1024), nullptr));

qtl::sqlite::blobstream bs(*db, "test_blob", "Content", id);
generate_n(ostreambuf_iterator<char>(bs), bs.blob_size()/sizeof(char), [i=0]() mutable { 
	return char('0'+(i++)%10);
});
copy(istream_iterator<char>(bs), istream_iterator<char>(), ostream_iterator<char>(cout, nullptr));
cout<<endl;

```

## About ODBC

When accessing the database through ODBC, include the header file qtl_odbc.hpp.
QTL does not support ODBC output parameters.

### ODBC parameter data binding

| Parameter Type | C++ Types |
| ------- | ------ |
| TINYINT | int8_t<br>uint8_t |
| SMALLINT | int16_t<br>uint16_t |
| INTEGER | int32_t<br>uint32_t |
| BIGINT | int64_t<br>uint64_t |
| FLOAT | float |
| DOUBLE | double |
| NUMERIC | SQL_NUMERIC_STRUCT |
| BIT | bool |
| CHAR<br>VARCHAR | const char*<br>std::string |
| WCHAR<br>WVARCHAR | const wchar_t*<br>std::wstring |
| BINARY | qtl::const_blob_data |
| LONGVARBINARY | std::istream<br>qtl::blob_writer |
| DATE | qtl::odbc::date |
| TIME<br>UTCTIME | qtl::odbc::time |
| TIMESTAMP<br>UTCDATETIME | qtl::odbc::datetime |
| GUID | SQLGUID |

### ODBC field data binding

| Field Type | C++ Types |
| ------- | ------ |
| TINYINT | int8_t<br>uint8_t |
| SMALLINT | int16_t<br>uint16_t |
| INTEGER | int32_t<br>uint32_t |
| BIGINT | int64_t<br>uint64_t |
| FLOAT | float |
| DOUBLE | double |
| NUMERIC | SQL_NUMERIC_STRUCT |
| BIT | bool |
| CHAR<br>VARCHAR | char[N]<br>std::array&lt;char, N&gt;<br>std::string |
| WCHAR<br>WVARCHAR | wchar_t[N]<br>std::array&lt;wchar_t, N&gt;<br>std::string |
| BINARY | qtl::blob_data |
| LONGVARBINARY | std::ostream<br>qtl::blobbuf |
| DATE | qtl::odbc::date |
| TIME<br>UTCTIME | qtl::odbc::time |
| TIMESTAMP<br>UTCDATETIME | qtl::odbc::datetime |
| GUID | SQLGUID |

### ODBC related C ++ classes
- qtl::odbc::database
Represents an ODBC database connection. The program mainly manipulates the database through this class.
- qtl::odbc::statement
Represents an ODBC query statement to implement query-related operations.
- qtl::odbc::error
Represents an ODBC error. When an operation error occurs, an exception of this type is thrown, including an error message.
- qtl::odbc::transaction
Represents an ODBC transaction operation.
- qtl::odbc::query_result
Represents an ODBC query result set, used to iterate through the query results in an iterator manner.

## About testing

Third-party libraries for compiling test cases need to be downloaded separately. In addition to database-related libraries, test cases use a test framework[CppTest](https://sourceforge.net/projects/cpptest/ "CppTest")。

The MySQL database used in the test case is as follows:
```SQL
CREATE TABLE test (
  ID int NOT NULL AUTO_INCREMENT,
  Name varchar(32) NOT NULL,
  CreateTime timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID)
);

CREATE TABLE test_blob (
  ID int unsigned NOT NULL AUTO_INCREMENT,
  Filename varchar(255) NOT NULL,
  Content longblob,
  MD5 binary(16) DEFAULT NULL,
  PRIMARY KEY (ID)
);
```
