//
//  core_workload.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include <vector>
#include <string>
#include "db.h"
#include "generator.h"
#include "discrete_generator.h"
#include "counter_generator.h"
#include "acknowledged_counter_generator.h"
#include "utils/properties.h"
#include "utils/utils.h"

namespace ycsbc {

enum Operation {
  INSERT = 0,
  READ,
  UPDATE,
  SCAN,
  READMODIFYWRITE,
  DELETE,
  INSERT_FAILED,
  READ_FAILED,
  UPDATE_FAILED,
  SCAN_FAILED,
  READMODIFYWRITE_FAILED,
  DELETE_FAILED,
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
  /// Zipfian constant for transaction key generation.
  ///
  static const std::string ZIPFIAN_CONST_PROPERTY;

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

  DB::Status TransactionRead(DB &db);
  DB::Status TransactionReadModifyWrite(DB &db);
  DB::Status TransactionScan(DB &db);
  DB::Status TransactionUpdate(DB &db);
  DB::Status TransactionInsert(DB &db);

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

} // ycsbc

#endif // YCSB_C_CORE_WORKLOAD_H_
