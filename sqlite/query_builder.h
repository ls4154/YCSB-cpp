//
//  query_builder.h
//  YCSB-cpp
//
//  Copyright (c) 2023 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_QUERY_BUILDER_H_
#define YCSB_C_QUERY_BUILDER_H_

#include <string>
#include <vector>

namespace ycsbc {

inline std::string BuildCreateTableQuery(std::string &table, std::string &key, const std::vector<std::string> &fields) {
  std::string stmt("CREATE TABLE");
  stmt += " IF NOT EXISTS ";
  stmt += table;

  stmt += " (";
  stmt += key;
  stmt += " TEXT";
  stmt += " PRIMARY KEY";
  for (size_t i = 0; i < fields.size(); i++) {
    stmt += ", ";
    stmt += fields[i];
    stmt += " TEXT";
  }
  stmt += ")";

  return stmt;
}

inline std::string BuildReadQuery(std::string &table, std::string &key, const std::vector<std::string> &fields) {
  std::string stmt("SELECT ");

  for (size_t i = 0; i < fields.size(); i++) {
    if (i > 0) {
      stmt += ", ";
    }
    stmt += fields[i];
  }

  stmt += " FROM ";
  stmt += table;

  stmt += " WHERE ";
  stmt += key;
  stmt += " = ?";

  return stmt;
}

inline std::string BuildInsertQuery(std::string &table, std::string &key, const std::vector<std::string> &fields) {
  std::string stmt("INSERT OR REPLACE INTO ");
  stmt += table;

  stmt += " (";
  stmt += key;
  for (size_t i = 0; i < fields.size(); i++) {
    stmt += ", ";
    stmt += fields[i];
  }
  stmt += ") ";

  stmt += "VALUES (?";
  for (size_t i = 0; i < fields.size(); i++) {
    stmt += ", ?";
  }
  stmt += ")";

  return stmt;
}

inline std::string BuildDeleteQuery(std::string &table, std::string &key) {
  std::string stmt("DELETE FROM ");
  stmt += table;

  stmt += " WHERE ";
  stmt += key;
  stmt += " = ?";

  return stmt;
}

inline std::string BuildUpdateQuery(std::string &table, std::string &key, const std::vector<std::string> &fields) {
  std::string stmt("UPDATE ");
  stmt += table;
  stmt += " SET ";

  for (size_t i = 0; i < fields.size(); i++) {
    if (i > 0) {
      stmt += ", ";
    }
    stmt += fields[i];
    stmt += " = ?";
  }

  stmt += " WHERE ";
  stmt += key;
  stmt += " = ?";

  return stmt;
}

inline std::string BuildScanQuery(std::string &table, std::string &key, const std::vector<std::string> &fields) {
  std::string stmt("SELECT ");

  stmt += key;
  for (size_t i = 0; i < fields.size(); i++) {
    stmt += ", ";
    stmt += fields[i];
  }

  stmt += " FROM ";
  stmt += table;

  stmt += " WHERE ";
  stmt += key;
  stmt += " >= ? ";
  stmt += "ORDER BY ";
  stmt += key;
  stmt += " LIMIT ?";

  return stmt;
}


} // ycsbc

#endif // YCSB_C_QUERY_BUILDER_H_
