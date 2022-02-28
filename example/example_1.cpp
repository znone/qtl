#include "qtl_odbc.hpp"
#include <iostream>
#include <string>

using namespace std;

int main()
{
  string sql;

  qtl::odbc::environment env;
  qtl::odbc::database db(env);

  db.open("Driver={SQL Server};Server=localhost;Database=test_db;");
  cout << "DBMS: " << db.dbms_name() << endl;
  cout << "SERVER: " << db.server_name() << endl;
  cout << "USER: " << db.user_name() << endl;
  cout << "DATABASE: " << db.db_name() << endl;


  sql = "drop table if exists test_table_1;";
  db.simple_execute(sql);
  sql = "create table test_table_1 (a float, b varchar(10));";
  db.simple_execute(sql);
  sql = "insert into test_table_1 VALUES (1.4, 'foo');";
  db.simple_execute(sql);
  sql = "insert into test_table_1 VALUES (1.5, 'bar');";
  db.simple_execute(sql);

  sql = "select * from[test_table_1];";
  db.query(sql,
    [](float a, const std::string& b)
    {
      std::string as = std::to_string(a);
      cout << a << "(" << as << ")" << endl;
      cout << b.data() << endl;
    });

  return 0;
}