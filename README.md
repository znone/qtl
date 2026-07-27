# QTL
QTL is a C ++ library for accessing SQL databases and currently supports MySQL, SQLite, PostgreSQL and ODBC. QTL is a lightweight library that consists of only header files and does not require separate compilation and installation. QTL is a thin encapsulation of the database's native client interface. It can provide a friendly way of using and has performance close to using the native interface.
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
or
```C++
db.execute("update test set Name=? WHERE ID=?", std::forward_as_tuple("other_user", id));
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

#### 11. Reuse the same data structure in different queries
Usually you want to reuse the structure and bind it to the result set of multiple different queries. At this time qtl::bind_record is not enough. You need to implement different binding functions with qtl::custom_bind to achieve this requirement. There are the following binding functions:

```C++
void my_bind(qtl::sqlite::statement& command, TestSqliteRecord&& v)
{
	qtl::bind_fields(command, v.id, v.name, v.create_time);		
}
```
The following code shows how to use it for queries:
```C++
db->query_explicit("select * from test where id=?", id, 
	qtl::custom_bind<TestMysqlRecord>(&my_bind),
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

### Access the database asynchronously

The database can be called asynchronously through the class async_connection. All asynchronous functions need to provide a callback function to accept the result after the operation is completed. If an error occurs during an asynchronous call, the error is returned to the caller as a parameter to the callback function.
```C++
qtl::mysql::async_connection connection;
connection.open(ev, [&connection](const qtl::mysql::error& e) {
	...
});

```

Asynchronous calls are done in the event loop. ev is an event loop object. QTL only proposes its requirements for the event loop and does not implement the event loop. QTL requires the event loop to provide the following interface, which is implemented by user code:
```C++
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
```C++
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

## Brief Description of Common Functions

### database

#### execute and execute_direct
Execute the SQL statement.
```C++
template<typename Params>
base_database& execute(const char* query_text, size_t text_length, const Params& params, uint64_t* affected=NULL);
template<typename Params>
database& execute(const char* query_text, const Params& params, uint64_t* affected=NULL);
template<typename Params>
database& execute(const std::string& query_text, const Params& params, uint64_t* affected=NULL);
template<typename... Params>
database& execute_direct(const char* query_text, size_t text_length, uint64_t* affected, const Params&... params);
template<typename... Params>
database& execute_direct(const char* query_text, uint64_t* affected, const Params&... params);
template<typename... Params>
database& execute_direct(const std::string& query_text, uint64_t* affected, const Params&... params);
```
Parameters:
query_text and text_length：SQL statement and statement length. When text_length=0, the default SQL statement ends with '\0'.
params：The parameters for executing SQL statements can be one parameter or a group of parameters (tuples).
affected：Returns the number of record rows affected by executing SQL statements. When the parameter is nullptr, the value is not returned.

Any acceptable data type can be used as a parameter to pass to the function. Parameters are bound through the function qtl::bind_param.
```C++
template<typename Command, typename T>
inline void bind_param(Command& command, size_t index, const T& param);
```
The following table lists the parameter types that can be accepted by different DBMSs. The function bind_param can be specialized to pass any data type as a parameter.

_QTL has specialized this function for std::optional. When this parameter does not contain data, null is passed to the database._

#### insert and insert_direct
Perform the insert operation.
```C++
template<typename Params>
uint64_t insert(const char* query_text, size_t text_length, const Params& params);
template<typename Params>
uint64_t insert(const char* query_text, const Params& params);
template<typename Params>
uint64_t insert(const std::string& query_text, const Params& params);
template<typename... Params>
uint64_t insert_direct(const char* query_text, size_t text_length, const Params&... params);
template<typename... Params>
uint64_t insert_direct(const char* query_text, const Params&... params);
template<typename... Params>
uint64_t insert_direct(const std::string& query_text, const Params&... params);
```
Each parameter has the same meaning as execute. After successfully inserting data, return the record ID.

#### result
Execute the query statement and return the query results.
```C++
template<typename Record, typename Params>
query_result<Command, Record> result(const char* query_text, size_t text_length, const Params& params);
template<typename Record, typename Params>
query_result<Command, Record> result(const char* query_text, const Params& params);
template<typename Record, typename Params>
query_result<Command, Record> result(const std::string& query_text, const Params& params);
template<typename Record>
query_result<Command, Record> result(const char* query_text, size_t text_length);
template<typename Record>
query_result<Command, Record> result(const char* query_text);
template<typename Record>
query_result<Command, Record> result(const std::string& query_text);
```
The template parameter _Record_ is a data type that can receive query result sets. QTL binds this data type to the result set through qtl::bind_record.
Query results can be regarded as containers, which support forward iterators.
```C++
template<typename Command, typename T>
inline void bind_record(Command& command, T&& value);
```
The function invoke qtl::bind_field to bind data to the field.
```C++
template<typename Command, typename T>
inline void bind_field(Command& command, size_t index, T& value)
```
Both functions can be specialized as needed. The following table lists the parameter types that can be accepted by different DBMSs.

_QTL has specialized the function bind_field for std::optional. When this parameter does not contain data, null is passed to the database._
#### query and query_explicit
Execute the query statement and process the query results in the callback function.
```C++
template<typename Params, typename Values, typename ValueProc>
database& query_explicit(const char* query_text, size_t text_length, const Params& params, Values&& values, ValueProc&& proc);
template<typename Params, typename Values, typename ValueProc>
database& query_explicit(const char* query_text, const Params& params, Values&& values, ValueProc&& proc);
template<typename Params, typename Values, typename ValueProc>
database& query_explicit(const std::string& query_text, const Params& params, Values&& values, ValueProc&& proc);
template<typename Values, typename ValueProc>
database& query_explicit(const char* query_text, size_t text_length, Values&& values, ValueProc&& proc);
template<typename Values, typename ValueProc>
database& query_explicit(const char* query_text, Values&& values, ValueProc&& proc);
template<typename Values, typename ValueProc>
database& query_explicit(const std::string& query_text, Values&& values, ValueProc&& proc);
template<typename Params, typename ValueProc>
database& query(const char* query_text, size_t text_length, const Params& params, ValueProc&& proc);
template<typename Params, typename ValueProc>
database& query(const char* query_text, const Params& params, ValueProc&& proc);
template<typename Params, typename ValueProc>
database& query(const std::string& query_text, const Params& params, ValueProc&& proc);
template<typename ValueProc>
database& query(const char* query_text, size_t text_length, ValueProc&& proc);
template<typename ValueProc>
database& query(const char* query_text, ValueProc&& proc);
template<typename ValueProc>
database& query(const std::string& query_text, ValueProc&& proc);
```
Parameters:
proc：The callback function for processing the result set, which is called once for each record.
You can not pass the data type of the bound result set. This function can bind the result set through the parameters of the callback function. If the data type passed is inconsistent with the parameters of the callback function, you need to ensure that the data type can be implicitly converted to the parameters of the callback function.
#### query_multi_with_params and query_multi
Execute query statements to process multiple result sets.
```C++
template<typename Params, typename... ValueProc>
database& query_multi_with_params(const char* query_text, size_t text_length, const Params& params, ValueProc&&... proc);
template<typename Params, typename... ValueProc>
databasw& query_multi_with_params(const char* query_text, const Params& params, ValueProc&&... proc);
template<typename Params, typename... ValueProc>
database& query_multi_with_params(const std::string& query_text, const Params& params, ValueProc&&... proc)；
template<typename... ValueProc>
database& query_multi(const char* query_text, size_t text_length, ValueProc&&... proc);
template<typename... ValueProc>
database& query_multi(const char* query_text, ValueProc&&... proc);
template<typename... ValueProc>
databased& query_multi(const std::string& query_text, ValueProc&&... proc);
```
Parameter package:
proc：Provide a callback function for each result set.
#### query_first and query_first_direct
Execute the query statement to obtain the first row of data in the result set.
```C++
template<typename Params, typename Values>
bool query_first(const char* query_text, size_t text_length, const Params& params, Values&& values);
template<typename Params, typename Values>
bool query_first(const char* query_text, const Params& params, Values&& values);
template<typename Params, typename Values>
bool query_first(const std::string& query_text, const Params& params, Values&& values);
template<typename Values>
bool query_first(const char* query_text, size_t text_length, Values&& values);
template<typename Values>
bool query_first(const char* query_text, Values&& values);
template<typename Values>
bool query_first(const std::string& query_text, Values&& values);
template<typename... Values>
bool query_first_direct(const char* query_text, size_t text_length, Values&... values);
template<typename... Values>
bool query_first_direct(const char* query_text, Values&... values);
template<typename... Values>
bool query_first_direct(const std::string& query_text, Values&... values);
```
Parameters:
values：Receive result data, which can be one data or a group of data (tuple or parameter package).


### Summary of various methods of binding data to structure

| Method | Features |
| ---- | ---- |
| struct qtl::record_binder<br/>qtl::bind_record() | Classes and functions binding data agreed by QTL. Each structure needs to specialize this class or function, and bind fields to query results one by one |
| qtl::custom_bind() | You can use any function to bind the structure to the query result, and bind the fields to the query result one by one |
| qtl::pfr::all_bind<br/>qtl::all_bind | Bind all fields in the structure to the query results in order. The following methods do not need to write specialized functions for structures:<br/>qtl::pfr::all_bind requires the boost.pfr library, and qtl::all_bind requires C++26, the same below. |
| struct qtl::pfr::partition_bind<br/>qtl::pfr::bind_some()<br/>struct qtl::partition_bind<br/>qtl::bind_some() | Bind the fields with the specified serial number in the structure to the query results in the listed order |
| qtl::pfr::bind_front()<br/>qtl::bind_front() | Bind the first few fields in the structure to the query results |
| qtl::pfr::auto_bind()<br/>qtl::auto_bind() | It is automatically bound to the query results according to the field names in the structure, which is simple to use but slow<br/>C++20 or C++26 is required |
| Micro QTL::BIND_STRUCT<br/>Micro QTL_BIND_OBJECT | Bind the fields specified in the structure to the query results in turn<br/>The boost.processor library is required |

|C++ Standard| Method |
| ---- | ---- |
|C++11| qtl::custom_bind()<br/>宏QTL::BIND_STRUCT<br/>宏QTL_BIND_OBJECT |
|C++17| struct qtl::pfr::all_bind<br/>struct qtl::pfr::partition_bind<br/> |
|C++20| qtl::pfr::auto_bind() |
|C++26| struct qtl::all_bind<br/>struct qtl::partition_bind<br/>qtl::auto_bind() |


### async_connection

#### bind
Before performing an asynchronous operation, it is associated with an event loop.
```C++
template<typename EventLoop>
bool bind(EventLoop& ev);
bool unbind();
```
#### unbind
Remove from the event loop.
```C++
bool unbind();
```

#### execute and execute_direct
Execute SQL statements asynchronously.
```C++
template<typename Params, typename ResultHandler>
void execute(ResultHandler handler, const char* query_text, size_t text_length, const Params& params);
template<typename Params, typename ResultHandler>
void execute(ResultHandler handler, const char* query_text, const Params& params);
template<typename Params, typename ResultHandler>
void execute(ResultHandler handler, const std::string& query_text, const Params& params);
template<typename... Params, typename ResultHandler>
void execute_direct(ResultHandler handler, const char* query_text, size_t text_length, const Params&... params);
template<typename... Params, typename ResultHandler>
void execute_direct(ResultHandler handler, const char* query_text, const Params&... params);
template<typename... Params, typename ResultHandler>
void execute_direct(ResultHandler handler, const std::string& query_text, const Params&... params);
```
Parameters:
handler：The callback function when the execution is completed.
```C++
void handler(const exception_type& e, uint64_t affected=0);
```
Parameters:
e：An error occurred while executing.
affected：Number of records affected by the SQL statement.

#### insert and insert_direct
Execute the insert statements asynchronously.
```C++
template<typename Params, typename ResultHandler>
void insert(ResultHandler handler, const char* query_text, size_t text_length, const Params& params);
template<typename Params, typename ResultHandler>
void insert(ResultHandler&& handler, const char* query_text, const Params& params);
template<typename Params, typename ResultHandler>
void insert(ResultHandler&& handler, const std::string& query_text, const Params& params);
template<typename... Params, typename ResultHandler>
void insert_direct(ResultHandler&& handler, const char* query_text, size_t text_length, const Params&... params);
template<typename... Params, typename ResultHandler>
void insert_direct(ResultHandler&& handler, const char* query_text, const Params&... params);
template<typename... Params, typename ResultHandler>
void insert_direct(ResultHandler&& handler, const std::string& query_text, const Params&... params);
```
Parameters:
handler：The callback function when the execution is completed.
```C++
void handler(const exception_type& e, uint64_t insert_id=0);
```
#### query_explicit and query
Execute the query statement asynchronously.
```C++
template<typename Params, typename Values, typename RowHandler, typename FinishHandler>
void query_explicit(const char* query_text, size_t text_length, const Params& params, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename Params, typename Values, typename RowHandler, typename FinishHandler>
void query_explicit(const char* query_text, const Params& params, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename Params, typename Values, typename RowHandler, typename FinishHandler>
void query_explicit(const std::string& query_text, const Params& params, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename Values, typename RowHandler, typename FinishHandler>
void query_explicit(const char* query_text, size_t text_length, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename Values, typename RowHandler, typename FinishHandler>
void query_explicit(const char* query_text, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename Values, typename RowHandler, typename FinishHandler>
void query_explicit(const std::string& query_text, Values&& values, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename Params, typename RowHandler, typename FinishHandler>
void query(const char* query_text, size_t text_length, const Params& params, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename Params, typename RowHandler, typename FinishHandler>
void query(const char* query_text, const Params& params, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename Params, typename RowHandler, typename FinishHandler>
void query(const std::string& query_text, const Params& params, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename RowHandler, typename FinishHandler>
void query(const char* query_text, size_t text_length, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename RowHandler, typename FinishHandler>
void query(const char* query_text, RowHandler&& row_handler, FinishHandler&& finish_handler);
template<typename RowHandler, typename FinishHandler>
void query(const std::string& query_text, RowHandler&& row_handler, FinishHandler&& finish_handler);
```
Parameters:
row_handler：The callback function for processing the result set, which is called once per line of records.
finish_handler：The callback function when the query is completed.

#### query_multi_with_params and query_multi
Execute query statements asynchronously and process multiple result sets.
```C++
template<typename Params, typename FinishHandler, typename... RowHandlers>
void query_multi_with_params(const char* query_text, size_t text_length, const Params& params, FinishHandler&& finish_handler, RowHandlers&&... row_handlers);
template<typename Params, typename FinishHandler, typename... RowHandlers>
void query_multi_with_params(const char* query_text, const Params& params, FinishHandler&& finish_handler, RowHandlers&&... row_handlers);
template<typename Params, typename FinishHandler, typename... RowHandlers>
void query_multi_with_params(const std::string& query_text, const Params& params, FinishHandler&& finish_handler, RowHandlers&&... row_handlers);
template<typename FinishHandler, typename... RowHandlers>
void query_multi(const char* query_text, size_t text_length, FinishHandler&& finish_handler, RowHandlers&&... row_handlers);
template<typename FinishHandler, typename... RowHandlers>
void query_multi(const char* query_text, FinishHandler&& finish_handler, RowHandlers&&... row_handlers);
template<typename FinishHandler, typename... RowHandlers>
void query_multi(const std::string& query_text, FinishHandler&& finish_handler, RowHandlers&&... row_handlers);
```
Parameter package:
row_handlers：Provide a callback function for each result set.

### Asio related

#### claass qtl::asio::service
Packaging asio::service，Provides an event loop for asynchronous database operations.
```C++
class service
{
public:
	typedef asio::io_context service_type;
	service() noexcept;
	explicit service(int concurrency_hint);
	void reset();
	void run();
	void stop();
	service_type& context() noexcept;
};
```

#### qtl::asio::async_open
Connect to the database asynchronously.
```C++
template<typename Connection, typename OpenHandler, typename... Args>
inline ASIO_INITFN_RESULT_TYPE(OpenHandler, void(typename Connection::exception_type)) 
async_open(service& service, Connection& db, OpenHandler&& handler, Args&&... args);
```
#### qtl::asio::async_close
Close the database connection asynchronously.
```C++
template<typename Connection, typename CloseHandler, typename... Args>
inline ASIO_INITFN_RESULT_TYPE(CloseHandler, void()) 
async_close(Connection& db, CloseHandler&& handler, Args&&... args);
```
#### qtl::asio::async_execute etc
Execute SQL statements asynchronously, etc.
```C++
template<typename Connection, typename ExecuteHandler, typename... Args>
ASIO_INITFN_RESULT_TYPE(ExecuteHandler, void(typename Connection::exception_type, uint64_t))  
async_execute(Connection& db, ExecuteHandler&& handler, Args&&... args);
template<typename Connection, typename ExecuteHandler, typename... Args>
ASIO_INITFN_RESULT_TYPE(ExecuteHandler, void(typename Connection::exception_type, uint64_t))  
async_execute_direct(Connection& db, ExecuteHandler&& handler, Args&&... args);

template<typename Connection, typename ExecuteHandler, typename... Args>
ASIO_INITFN_RESULT_TYPE(ExecuteHandler, void(typename Connection::exception_type, uint64_t)) 
async_insert(Connection& db, ExecuteHandler&& handler, Args&&... args);
template<typename Connection, typename ExecuteHandler, typename... Args>
ASIO_INITFN_RESULT_TYPE(ExecuteHandler, void(typename Connection::exception_type, uint64_t))； 
async_insert_direct(Connection& db, ExecuteHandler&& handler, Args&&... args);

template<typename Connection, typename FinishHandler, typename... Args>
ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type)) 
async_query(Connection& db, FinishHandler&& handler, Args&&... args);
template<typename Connection, typename FinishHandler, typename... Args>
ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type)) 
async_query_explicit(Connection& db, FinishHandler&& handler, Args&&... args);

template<typename Connection, typename A1, typename A2, typename FinishHandler, typename... RowHandlers>
ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type))
async_query_multi_with_params(Connection& db, A1&& a1, A2&& a2, FinishHandler&& handler, RowHandlers&&... row_handlers);
template<typename Connection, typename A1, typename FinishHandler, typename... RowHandlers>
ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type))；
async_query_multi_with_params(Connection& db, A1&& a1, FinishHandler&& handler, RowHandlers&&... row_handlers);

template<typename Connection, typename A1, typename A2, typename FinishHandler, typename... RowHandlers>
ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type))
async_query_multi(Connection& db, A1&& a1, A2&& a2, FinishHandler&& handler, RowHandlers&&... row_handlers);
template<typename Connection, typename A1, typename FinishHandler, typename... RowHandlers>
ASIO_INITFN_RESULT_TYPE(FinishHandler, void(typename Connection::exception_type))
async_query_multi(Connection& db, A1&& a1, FinishHandler&& handler, RowHandlers&&... row_handlers);

```

## About MySQL

When accessing MySQL, include the header file qtl_mysql.hpp.

### MySQL parameter data binding

| Parameter Types | C++ Types |
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

| Field Types | C++ Types |
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

| Parameter Types | C++ Types |
| ------- | ------ |
| integer | int</br>int64_t |
| real | double |
| text | const char*<br>std::string<br>std::wstring |
| blob | qtl::const_blob_data |


### SQLite field data binding

| Field Types | C++ Types |
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

| Parameter Types | C++ Types |
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

| Field Types | C++ Types |
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

## About PostgreSQL
When accessing PostgreSQL, include the header file qtl_postgres.hpp.
On Linux, you need to install libpq, libecpg, and PostgreSQL Server development libraries.

### PostgreSQL parameter data binding

| Parameter Types | C++ Types |
| ------- | ------ |
| bool | bool |
| integer | int32_t |
| smallint | int16_t |
| bigint | int64_t |
| real | float |
| double | double |
| text | const char*<br>std::string |
| bytea | qtl::const_blob_data<br>std::vector<uint8_t> |
| oid | qtl::postgres::large_object |
| date | qtl::postgres::date |
| timestamp | qtl::postgres::timestamp |
| interval | qtl::postgres::interval |
| array | std::vector<br>std::array<br>T[N] |
| composite types | std::tuple<br>std::pair |

### PostgreSQL field data binding

| Field Types | C++ Types |
| ------- | ------ |
| bool | bool |
| integer | int32_t |
| smallint | int16_t |
| bigint | int64_t |
| real | float |
| double | double |
| text | char[N]<br>std::array&lt;char, N&gt;<br>std::string |
| bytea | qtl::const_blob_data<br>qtl::blob_data<br>std::vector<uint8_t> |
| oid | qtl::postgres::large_object |
| date | qtl::postgres::date |
| timestamp | qtl::postgres::timestamp |
| interval | qtl::postgres::interval |
| array | std::vector<br>std::array<br>T[N] |
| composite types | std::tuple<br>std::pair |

### C ++ classes related to PostgreSQL
- qtl::postgres::database
Represents a PostgreSQL database connection. The program mainly manipulates the database through this class.
- qtl::postgres::statement
Represents a PostgreSQL query statement to implement query-related operations.
- qtl::postgres::error
Represents a PostgreSQL error. When an operation error occurs, an exception of this type is thrown, including the error information.
- qtl::postgres::transaction
Represents a PostgreSQL transaction operation.
- qtl::postgres::query_result
Represents a PostgreSQL query result set, used to iterate over the query results in an iterator manner.

## About testing

Third-party libraries for compiling test cases need to be downloaded separately. In addition to database-related libraries, test cases use a test framework[CppTest](https://sourceforge.net/projects/cpptest/ "CppTest")。

The database used in the test case is as follows:

### MySQL
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

### PostgreSQL
```SQL
DROP TABLE IF EXISTS test;
CREATE TABLE test (
  id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY (
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
),
  name varchar(255) COLLATE default,
  createtime timestamp(6)
)
;

ALTER TABLE test ADD CONSTRAINT test_pkey PRIMARY KEY ("id");

DROP TABLE IF EXISTS test_blob;
CREATE TABLE test_blob (
  id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY (
INCREMENT 1
MINVALUE  1
MAXVALUE 2147483647
START 1
),
  filename varchar(255) COLLATE default NOT NULL,
  md5 bytea,
  content oid
)
;

ALTER TABLE test_blob ADD CONSTRAINT test_blob_pkey PRIMARY KEY ("id");
```
