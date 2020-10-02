# QTL
QTL是一个访问SQL数据库的C++库，目前支持MySQL、SQLite和ODBC。QTL是一个轻量级的库，只由头文件组成，不需要单独编译安装。QTL是对数据库原生客户端接口的薄封装，能提供友好使用方式的同时拥有接近于使用原生接口的性能。
使用QTL需要支持C++11的编译器。

[![996.icu](https://img.shields.io/badge/link-996.icu-red.svg)](https://996.icu)

## 使用方式

### 打开数据库

```C++
qtl::mysql::database db;
db.open("localhost", "root", "", "test");
```

### 执行查询


#### 1. 插入记录

```C++
uint64_t id=db.insert("insert into test(Name, CreateTime) values(?, now())", "test_user");
```

#### 2. 更新记录

```C++
db.execute_direct("update test set Name=? WHERE ID=?",  NULL, "other_user", id);
```
#### 3. 更新多条记录：

```C++
uint64_t affected=0;
auto stmt=db.open_command("insert into test(Name, CreateTime) values(?, now())");
qtl::execute(stmt, &affected, "second_user", "third_user");

```
或者
```C++
stmt<<"second_user"<<"third_user";

```

#### 4. 查询数据，以回调函数方式处理数据
程序会一直遍历数据集，直到当回调函数返回false为止。如果回调函数无返回值，相当于返回true。

```C++
db.query("select * from test where id=?", id, 
	[](uint32_t id, const std::string& name, const qtl::mysql::time& create_time) {
		printf("ID=\"%d\", Name=\"%s\"\n", id, name.data());
});
```
当无法根据回调函数的参数推断字段类型时，请使用query_explicit代替query，手动指定数据类型进行查询。

#### 5. 也可以把数据绑定到结构上

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
#### 6. 用成员函数做为查询的回调函数
当记录类有不带参数的成员函数时，可以直接用作查询的回调函数
```C++
struct TestMysqlRecord
{
	void print();
};

db.query("select * from test where id=?", id,
	&TestMysqlRecord::print);
```

#### 7. 以迭代器方式访问数据

```C++
for(auto& record : db.result<TestMysqlRecord>("select * from test"))
{
	printf("ID=\"%d\", Name=\"%s\"\n", record.id, record.name);
}
```
#### 8. 指示器
可以用指示器（indicator）获取查询结果的更多信息。指示器包含以下成员：
- data 存储字段的数据
- is_null 字段是否为空
- length 数据的实际长度
- is_truncated 数据是否被截断
 
#### 9. std::optional和std::any
可以绑定字段到 C++17 中的 std::optional 和 std::any。当字段为null时，它们不包含任何内容，否则它们包含字段的值。

#### 10. 支持标准库以外的字符串类型
除了标准库提供的std::string，另外其他库也提供了自己的字符串类，比如QT的QString，MFC/ATL的CString等。qtl也可以将字符字段绑定到这些类型上。扩展方法是：
1. 为你的字符串类型，对 qtl::bind_string_helper 实现一个专门化。如果该字符串类型有符合标准库字符串语义的以下成员函数，可以跳过这一步：assign，clear，resize，data，size；
2. 为你的字符串类型，对 qtl::bind_field 实现一个专门化；

因为 QT 的 QByteArray 有兼容标准库的成员函数，所以绑定到 QByteArray 只需要一步：
一般数据库不提供到 QChar/QString 的绑定，所以只能先用 QByteArray 接收数据，然后转换为 QString。

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

#### 11. 在不同查询中复用同一数据结构
通常希望复用结构，将其绑定到多个不同的查询的结果集，这时候 qtl::bind_record就不够用了。需要利用 qtl::custom_bind 实现不同的绑定函数才能实现这一需求。有如下绑定函数：

```C++
void my_bind(TestMysqlRecord&& v, qtl::sqlite::statement& command)
{
	qtl::bind_field(command, "id", v.id);
	qtl::bind_field(command, 1, v.name);
	qtl::bind_field(command, 2, v.create_time);
}
```
以下代码说明如何将其用于查询：
```C++
db->query_explicit("select * from test where id=?", id, 
	qtl::custom_bind(TestMysqlRecord(), &my_bind),
	[](const TestMysqlRecord& record) {
		printf("ID=\"%d\", Name=\"%s\"\n", record.id, record.name);
	});
```
qtl::bind_record不是唯一的方法。通过派生类也能实现类似的需求（qtl::record_with_tag）。

#### 12.处理返回多个结果集的查询
有些查询语句会返回多个结果集。使用函数query执行这些查询只能得到第一个结果集。要处理所有结果集需要使用query_multi或query_multi_with_params。query_multi不会为没有结果集的查询调用回调函数。例如：
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

#### 13. 异步调用数据库

通过类async_connection可以异步调用数据库。所有的异步函数都需要提供一个回调函数接受操作完成后的结果。如果异步调用中发生错误，错误做为回调函数的参数返回给调用者。
```
qtl::mysql::async_connection connection;
connection.open(ev, [&connection](const qtl::mysql::error& e) {
	...
});

```

异步调用在事件循环中完成。ev是事件循环对象。QTL只提出它对事件循环的需求，并不实现事件循环。QTL要求事件循环提供如下接口，该接口由用户代码实现：
```
class EventLoop
{
public:
	// 把数据库连接添加到事件循环中
	template<typename Connection>
	qtl::event_handler* add(Connection* connection);
	
	// 在事件循环中添加一个超时任务
	template<typename Handler>
	qtl::event* set_timeout(const timeval& timeout, Handler&& handler);
};
```

qtl::event是QTL中定义的一个事件项接口，用户代码同样应该实现它：
```
struct event
{
	// IO事件标志
	enum io_flags
	{
		ef_read = 0x1,
		ef_write = 0x2,
		ef_exception = 0x4,
		ef_timeout =0x8,
		ev_all = ef_read | ef_write | ef_exception
	};

	virtual ~event() { }
	// 设置IO处理器
	virtual void set_io_handler(int flags, long timeout, std::function<void(int)>&&) = 0;
	// 从事件循环中移除事件项
	virtual void remove() = 0;
	// 判断该事件项是否在等待IO中
	virtual bool is_busying() = 0;
};

```
数据库连接通常不是线程安全的。用户代码应该保证，一个连接只能同时由一个线程使用。
使用这项功能，GCC需要 5 或更高版本才行。

## 有关MySQL的说明

访问MySQL时，包含头文件qtl_mysql.hpp。

### MySQL的参数数据绑定

| 参数类型 | C++类型 |
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

blob_writer是一个函数，它的定义如下：
```C++
typedef std::function<void(std::ostream&)> blob_writer;
```
该函数通过std::ostream类型的参数向BLOB字段写入数据。因为MySQL API的限制，该流基本只能向前移动，并不建议对该流随意调整写入位置。

### MySQL的字段数据绑定

| 字段类型 | C++类型 |
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

可以通过qtl::mysql::blobbuf读取BLOB字段的数据：
```C++
void read_blob(qtl::blobbuf& buf) {
	istream s(&buf);
	...
};
```
因为MySQL API的限制，该流只能向前移动，并不建议对该流随意调整读取位置。

### MySQL相关的C++类
- qtl::mysql::database
表示一个MySQL的数据库连接，程序主要通过这个类操纵数据库。
- qtl::mysql::statement
表示一个MySQL的查询语句，实现查询相关操作。
- qtl::mysql::error
表示一个MySQL的错误，当操作出错时，抛出该类型的异常，包含错误信息。
- qtl::mysql::transaction
表示一个MySQL的事务操作。
- qtl::mysql::query_result
表示一个MySQL的查询结果集，用于以迭代器方式遍历查询结果。

## 有关SQLite的说明

访问SQLite时，包含头文件qtl_sqlite.hpp。

### SQLite的参数数据绑定

| 参数类型 | C++类型 |
| ------- | ------ |
| integer | int</br>int64_t |
| real | double |
| text | const char*<br>std::string<br>std::wstring |
| blob | qtl::const_blob_data |


### SQLite的字段数据绑定

| 字段类型 | C++类型 |
| ------- | ------ |
| integer | int</br>int64_t |
| real | double |
| text | char[N]<br>std::array&lt;char, N&gt;<br>std::string<br>std::wstring |
| blob | qtl::const_blob_data<br>qtl::blob_data<br>std::ostream |

当以qtl::const_blob_data接收blob数据时，直接返回SQLite给出的数据地址；当以qtl::blob_data接收blob数据时，数据被复制到qtl::blob_data指定的地址。

### SQLite相关的C++类
- qtl::sqlite::database
表示一个SQLite的数据库连接，程序主要通过这个类操纵数据库。
- qtl::sqlite::statement
表示一个SQLite的查询语句，实现查询相关操作。
- qtl::sqlite::error
表示一个SQLite的错误，当操作出错时，抛出该类型的异常，包含错误信息。
- qtl::sqlite::transaction
表示一个SQLite的事务操作。
- qtl::sqlite::query_result
表示一个SQLite的查询结果集，用于以迭代器方式遍历查询结果。

### SQLite的Blob字段

通过QTL，可以通过标准流的方式访问SQLite的BLOB字段。
下面的代码，先用数字0-9向BLOB字段填充，然后再次读取字段内容并显示到屏幕。

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

## 有关ODBC的说明

通过ODBC访问数据库时，包含头文件qtl_odbc.hpp。
QTL不支持ODBC的输出参数。

### ODBC的参数数据绑定

| 参数类型 | C++类型 |
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

### ODBC的字段数据绑定

| 字段类型 | C++类型 |
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

### ODBC相关的C++类
- qtl::odbc::database
表示一个ODBC的数据库连接，程序主要通过这个类操纵数据库。
- qtl::odbc::statement
表示一个ODBC的查询语句，实现查询相关操作。
- qtl::odbc::error
表示一个ODBC的错误，当操作出错时，抛出该类型的异常，包含错误信息。
- qtl::odbc::transaction
表示一个ODBC的事务操作。
- qtl::odbc::query_result
表示一个ODBC的查询结果集，用于以迭代器方式遍历查询结果。

## 关于测试

编译测试用例的第三方库需要另外下载。除了数据库相关的库外，测试用例用到了测试框架[CppTest](https://sourceforge.net/projects/cpptest/ "CppTest")。

测试用例所用的MySQL数据库如下：
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

测试用例在 Visual Studio 2013 和 GCC 4.8 下测试通过。
