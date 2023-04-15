#include "postgres_db.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <string.h>

#include "core/db_factory.h"

using namespace std;

namespace {
const string PROP_POSTGRES_HOST = "pgsql.hostaddr";
const string PROP_POSTGRES_HOST_DEFAULT = "localhost";

const string PROP_POSTGRES_PORT = "pgsql.port";
const string PROP_POSTGRES_PORT_DEFAULT = "5432";

const string PROP_POSTGRES_DBNAME = "pgsql.dbname";
const string PROP_POSTGRES_DBNAME_DEFAULT = "postgres";

const string PRIMARY_KEY = "YCSB_KEY";
const string COLUMN_NAME = "YCSB_VALUE";

};  // namespace

namespace ycsbc {

void PostgresDB::Init() {
  auto &props = *props_;
  const string conninfo = "postgresql://" + props.GetProperty(PROP_POSTGRES_HOST, PROP_POSTGRES_HOST_DEFAULT) + ":" +
                          props.GetProperty(PROP_POSTGRES_PORT, PROP_POSTGRES_PORT_DEFAULT) + "/" +
                          props.GetProperty(PROP_POSTGRES_DBNAME, PROP_POSTGRES_DBNAME_DEFAULT);
  cout << "Connecting to " << conninfo << endl;
  conn_ = PQconnectdb(conninfo.c_str());

  if (PQstatus(conn_) != CONNECTION_OK) {
    cerr << "Failed to connect to Postgres server, error: " << PQerrorMessage(conn_) << endl;
    PQfinish(conn_);
    exit(1);
  }

  string table_name = props.GetProperty(CoreWorkload::TABLENAME_PROPERTY, CoreWorkload::TABLENAME_DEFAULT);
  string query("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '" + table_name + "')");
  PGresult *res = PQexec(conn_, query.c_str());
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    cerr << "Failed to query table existence, status: " << PQresStatus(PQresultStatus(res)) << ", error: " << PQresultErrorMessage(res) << endl;
    PQclear(res);
    PQfinish(conn_);
    exit(1);
  } else {
    char *exist = PQgetvalue(res, 0, 0);
    if (strcmp("t", exist) != 0) {
      query = "CREATE TABLE " + table_name + "(" + PRIMARY_KEY + " VARCHAR(255) PRIMARY KEY, " + COLUMN_NAME + " TEXT)";
      res = PQexec(conn_, query.c_str());
      if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        cerr << "Failed to create table, status: "<< PQresStatus(PQresultStatus(res)) <<", error: " << PQresultErrorMessage(res) << endl;
        PQclear(res);
        PQfinish(conn_);
        exit(1);
      }
    }
  }
  PQclear(res);

  cout << "Connected." << endl;
}

DB::Status PostgresDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                            std::vector<Field> &result) {
  const string query("SELECT " + COLUMN_NAME + " FROM " + table + " WHERE " + PRIMARY_KEY + " = '" + key + "'");
  PGresult *res = PQexec(conn_, query.c_str());
  ExecStatusType status = PQresultStatus(res);
  Status ret;

  if (status == PGRES_TUPLES_OK) {
    if (PQntuples(res) == 1) {
      assert(PQnfields(res) == 1);
      ret = Status::kOK;
      result.push_back(Field("", PQgetvalue(res, 0, 0)));
    } else {
      ret = Status::kNotFound;
    }
  } else {
    cerr << "[READ] Status: " << PQresStatus(status) << ", error: " << PQresultErrorMessage(res) << endl;
    ret = Status::kError;
  }
  PQclear(res);
  return ret;
}

DB::Status PostgresDB::Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
  string value;
  for (auto &f : values) {
    value.append(f.second);
  }
  const char *param[1] = {value.data()};
  const int param_len[1] = {static_cast<int>(value.size())};
  const int param_format[1] = {0};
  const string query("UPDATE " + table + " SET " + COLUMN_NAME + " = $1::text WHERE " + PRIMARY_KEY + " = '" + key + "'");
  
  PGresult *res = PQexecParams(conn_, query.c_str(), 1, NULL, param, param_len, param_format, 0);
  ExecStatusType status = PQresultStatus(res);
  Status ret;

  if (status == PGRES_COMMAND_OK) {
    if (stoi(PQcmdTuples(res)) == 0) {
      ret = Status::kNotFound;
    } else {
      ret = Status::kOK;
    }
  } else {
    cerr << "[UPDATE] Status: " << PQresStatus(status) << ", error: " << PQresultErrorMessage(res) << endl;
    ret = Status::kError;
  }
  PQclear(res);
  return ret;
}

DB::Status PostgresDB::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
  string value;
  for (auto &f : values) {
    value.append(f.second);
  }
  const char *param[1] = {value.data()};
  const int param_len[1] = {static_cast<int>(value.size())};
  const int param_format[1] = {0};
  const string query("INSERT INTO " + table + " (" + PRIMARY_KEY + "," + COLUMN_NAME + ") VALUES ('" + key + "', $1::varchar(255))");

  PGresult *res = PQexecParams(conn_, query.c_str(), 1, NULL, param, param_len, param_format, 0);
  ExecStatusType status = PQresultStatus(res);
  Status ret;

  if (status == PGRES_COMMAND_OK) {
    ret = Status::kOK;
  } else {
    cerr << "[INSERT] Status: " << PQresStatus(status) << ", error: " << PQresultErrorMessage(res) << endl;
    ret = Status::kError;
  }
  PQclear(res);
  return ret;
}

DB::Status PostgresDB::Delete(const std::string &table, const std::string &key) {
  const string query("DELETE FROM " + table + " WHERE " + PRIMARY_KEY + " = '" + key + "'");
  PGresult *res = PQexec(conn_, query.c_str());
  ExecStatusType status = PQresultStatus(res);
  Status ret;

  if (status == PGRES_COMMAND_OK) {
    ret = Status::kOK;
  } else {
    cerr << "[DELETE] Status: " << PQresStatus(status) << ", error: " << PQresultErrorMessage(res) << endl;
    ret = Status::kError;
  }
  PQclear(res);
  return ret;
}

const bool registered = DBFactory::RegisterDB("postgres", []() { return dynamic_cast<DB *>(new PostgresDB); });

};  // namespace ycsbc
