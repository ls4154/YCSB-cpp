//
//  core_workload.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include <vector>
#include <string>
#include "db.h"
#include "properties.h"
#include "generator.h"
#include "discrete_generator.h"
#include "counter_generator.h"
#include "acknowledged_counter_generator.h"
#include "utils.h"

namespace ycsbc {

enum Operation {
  INSERT,
  READ,
  UPDATE,
  SCAN,
  READMODIFYWRITE,
  DELETE,
  MAXOPTYPE
};

extern const char *kOperationString[MAXOPTYPE];

class CoreWorkload {
 public:
  ///
  /// The name of the database table to run queries against.
  ///
  static const std::string TABLENAME_PROPERTY;
  static const std::string TABLENAME_DEFAULT;

  ///
  /// The name of the property for the number of fields in a record.
  ///
  static const std::string FIELD_COUNT_PROPERTY;
  static const std::string FIELD_COUNT_DEFAULT;

  ///
  /// The name of the property for the field length distribution.
  /// Options are "uniform", "zipfian" (favoring short records), and "constant".
  ///
  static const std::string FIELD_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string FIELD_LENGTH_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for the length of a field in bytes.
  ///
  static const std::string FIELD_LENGTH_PROPERTY;
  static const std::string FIELD_LENGTH_DEFAULT;

  ///
  /// The name of the property for deciding whether to read one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string READ_ALL_FIELDS_PROPERTY;
  static const std::string READ_ALL_FIELDS_DEFAULT;

  ///
  /// The name of the property for deciding whether to write one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string WRITE_ALL_FIELDS_PROPERTY;
  static const std::string WRITE_ALL_FIELDS_DEFAULT;

  ///
  /// The name of the property for the proportion of read transactions.
  ///
  static const std::string READ_PROPORTION_PROPERTY;
  static const std::string READ_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of update transactions.
  ///
  static const std::string UPDATE_PROPORTION_PROPERTY;
  static const std::string UPDATE_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of insert transactions.
  ///
  static const std::string INSERT_PROPORTION_PROPERTY;
  static const std::string INSERT_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of scan transactions.
  ///
  static const std::string SCAN_PROPORTION_PROPERTY;
  static const std::string SCAN_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of
  /// read-modify-write transactions.
  ///
  static const std::string READMODIFYWRITE_PROPORTION_PROPERTY;
  static const std::string READMODIFYWRITE_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the the distribution of request keys.
  /// Options are "uniform", "zipfian" and "latest".
  ///
  static const std::string REQUEST_DISTRIBUTION_PROPERTY;
  static const std::string REQUEST_DISTRIBUTION_DEFAULT;

  ///
  /// The default zero padding value. Matches integer sort order
  ///
  static const std::string ZERO_PADDING_PROPERTY;
  static const std::string ZERO_PADDING_DEFAULT;

  ///
  /// The name of the property for the min scan length (number of records).
  ///
  static const std::string MIN_SCAN_LENGTH_PROPERTY;
  static const std::string MIN_SCAN_LENGTH_DEFAULT;

  ///
  /// The name of the property for the max scan length (number of records).
  ///
  static const std::string MAX_SCAN_LENGTH_PROPERTY;
  static const std::string MAX_SCAN_LENGTH_DEFAULT;

  ///
  /// The name of the property for the scan length distribution.
  /// Options are "uniform" and "zipfian" (favoring short scans).
  ///
  static const std::string SCAN_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string SCAN_LENGTH_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for the order to insert records.
  /// Options are "ordered" or "hashed".
  ///
  static const std::string INSERT_ORDER_PROPERTY;
  static const std::string INSERT_ORDER_DEFAULT;

  static const std::string INSERT_START_PROPERTY;
  static const std::string INSERT_START_DEFAULT;

  static const std::string RECORD_COUNT_PROPERTY;
  static const std::string OPERATION_COUNT_PROPERTY;

  ///
  /// Field name prefix.
  ///
  static const std::string FIELD_NAME_PREFIX;
  static const std::string FIELD_NAME_PREFIX_DEFAULT;

  ///
  /// Initialize the scenario.
  /// Called once, in the main client thread, before any operations are started.
  ///
  virtual void Init(const utils::Properties &p);

  virtual bool DoInsert(DB &db);
  virtual bool DoTransaction(DB &db);

  bool read_all_fields() const { return read_all_fields_; }
  bool write_all_fields() const { return write_all_fields_; }

  CoreWorkload() :
      field_count_(0), read_all_fields_(false), write_all_fields_(false),
      field_len_generator_(nullptr), key_chooser_(nullptr), field_chooser_(nullptr),
      scan_len_chooser_(nullptr), insert_key_sequence_(nullptr),
      transaction_insert_key_sequence_(nullptr), ordered_inserts_(true), record_count_(0) {
  }

  virtual ~CoreWorkload() {
    delete field_len_generator_;
    delete key_chooser_;
    delete field_chooser_;
    delete scan_len_chooser_;
    delete insert_key_sequence_;
    delete transaction_insert_key_sequence_;
  }

 protected:
  static Generator<uint64_t> *GetFieldLenGenerator(const utils::Properties &p);
  std::string BuildKeyName(uint64_t key_num);
  void BuildValues(std::vector<DB::Field> &values);
  void BuildSingleValue(std::vector<DB::Field> &update);

  uint64_t NextTransactionKeyNum();
  std::string NextFieldName();

  int TransactionRead(DB &db);
  int TransactionReadModifyWrite(DB &db);
  int TransactionScan(DB &db);
  int TransactionUpdate(DB &db);
  int TransactionInsert(DB &db);

  std::string table_name_;
  int field_count_;
  std::string field_prefix_;
  bool read_all_fields_;
  bool write_all_fields_;
  Generator<uint64_t> *field_len_generator_;
  DiscreteGenerator<Operation> op_chooser_;
  Generator<uint64_t> *key_chooser_; // transaction key gen
  Generator<uint64_t> *field_chooser_;
  Generator<uint64_t> *scan_len_chooser_;
  CounterGenerator *insert_key_sequence_; // load insert key gen
  AcknowledgedCounterGenerator *transaction_insert_key_sequence_; // transaction insert key gen
  bool ordered_inserts_;
  size_t record_count_;
  int zero_padding_;
};

inline uint64_t CoreWorkload::NextTransactionKeyNum() {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > transaction_insert_key_sequence_->Last());
  return key_num;
}

inline std::string CoreWorkload::NextFieldName() {
  return std::string(field_prefix_).append(std::to_string(field_chooser_->Next()));
}

inline bool CoreWorkload::DoInsert(DB &db) {
  const std::string key = BuildKeyName(insert_key_sequence_->Next());
  std::vector<DB::Field> fields;
  BuildValues(fields);
  return db.Insert(table_name_, key, fields) == DB::kOK;
}

inline bool CoreWorkload::DoTransaction(DB &db) {
  int status = -1;
  switch (op_chooser_.Next()) {
    case READ:
      status = TransactionRead(db);
      break;
    case UPDATE:
      status = TransactionUpdate(db);
      break;
    case INSERT:
      status = TransactionInsert(db);
      break;
    case SCAN:
      status = TransactionScan(db);
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite(db);
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int CoreWorkload::TransactionRead(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  const std::string key = BuildKeyName(key_num);
  std::vector<DB::Field> result;
  if (!read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(NextFieldName());
    return db.Read(table_name_, key, &fields, result);
  } else {
    return db.Read(table_name_, key, NULL, result);
  }
}

inline int CoreWorkload::TransactionReadModifyWrite(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  const std::string key = BuildKeyName(key_num);
  std::vector<DB::Field> result;

  if (!read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(NextFieldName());
    db.Read(table_name_, key, &fields, result);
  } else {
    db.Read(table_name_, key, NULL, result);
  }

  std::vector<DB::Field> values;
  if (write_all_fields()) {
    BuildValues(values);
  } else {
    BuildSingleValue(values);
  }
  return db.Update(table_name_, key, values);
}

inline int CoreWorkload::TransactionScan(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  const std::string key = BuildKeyName(key_num);
  int len = scan_len_chooser_->Next();
  std::vector<std::vector<DB::Field>> result;
  if (!read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(NextFieldName());
    return db.Scan(table_name_, key, len, &fields, result);
  } else {
    return db.Scan(table_name_, key, len, NULL, result);
  }
}

inline int CoreWorkload::TransactionUpdate(DB &db) {
  uint64_t key_num = NextTransactionKeyNum();
  const std::string key = BuildKeyName(key_num);
  std::vector<DB::Field> values;
  if (write_all_fields()) {
    BuildValues(values);
  } else {
    BuildSingleValue(values);
  }
  return db.Update(table_name_, key, values);
}

inline int CoreWorkload::TransactionInsert(DB &db) {
  uint64_t key_num = transaction_insert_key_sequence_->Next();
  const std::string key = BuildKeyName(key_num);
  std::vector<DB::Field> values;
  BuildValues(values);
  int s = db.Insert(table_name_, key, values);
  transaction_insert_key_sequence_->Acknowledge(key_num);
  return s;
}

} // ycsbc

#endif // YCSB_C_CORE_WORKLOAD_H_
