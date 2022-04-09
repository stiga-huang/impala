// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <orc/sargs/Literal.hh>

#include "gen-cpp/ImpalaInternalService_types.h"
#include "impala-ir/impala-ir-functions.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/string-buffer.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/types.h"

namespace impala {

typedef google::protobuf::RepeatedPtrField<ColumnValuePB> ColumnValueBatchPB;

class InListFilter {
 public:
  /// Upper bound of total length for the string set to avoid it explodes.
  /// TODO: find a better value based on the implementation of ORC lib, or make this
  /// configurable.
  const static uint32_t STRING_SET_MAX_TOTAL_LENGTH = 4 * 1024 * 1024;

  InListFilter(ColumnType type, uint32_t entry_limit, bool contains_null = false);
  virtual ~InListFilter() {}
  void Close() {}

  /// Add a new value to the list.
  virtual void Insert(const void* val) = 0;

  virtual std::string DebugString() const noexcept = 0;

  bool ContainsNull() { return contains_null_; }
  bool AlwaysTrue() { return always_true_; }
  virtual bool AlwaysFalse() = 0;
  static bool AlwaysFalse(const InListFilterPB& filter);

  /// Makes this filter always return true.
  void SetAlwaysTrue() { always_true_ = true; }

  virtual bool Find(void* val, const ColumnType& col_type) const noexcept = 0;
  virtual void InsertBatch(const ColumnValueBatchPB& batch) = 0;
  virtual int NumItems() const noexcept = 0;
  virtual void ToOrcLiteralList(std::vector<orc::Literal>* in_list) = 0;

  /// Returns a new InListFilter with the given type, allocated from 'mem_tracker'.
  static InListFilter* Create(ColumnType type, uint32_t entry_limit, ObjectPool* pool, bool contains_null = false);

  /// Returns a new InListFilter created from the protobuf representation, allocated from
  /// 'mem_tracker'.
  static InListFilter* Create(const InListFilterPB& protobuf, ColumnType type,
      uint32_t entry_limit, ObjectPool* pool);

  /// Converts 'filter' to its corresponding Protobuf representation.
  /// If the first argument is NULL, it is interpreted as a complete filter which
  /// contains all elements, i.e. always true.
  static void ToProtobuf(const InListFilter* filter, InListFilterPB* protobuf);

  /// Returns the LLVM_CLASS_NAME for this base class 'InListFilter'.
  static const char* LLVM_CLASS_NAME;

  /// Return a debug string for 'filter'
  static std::string DebugString(const InListFilterPB& filter);
  /// Return a debug string for the list of the 'filter'
  static std::string DebugStringOfList(const InListFilterPB& filter);

 protected:
  friend class HdfsOrcScanner;
  virtual void ToProtobuf(InListFilterPB* protobuf) const = 0;

  bool always_true_;
  bool contains_null_;
  PrimitiveType type_;
  // Type len for CHAR type.
  int type_len_;
  uint32_t str_total_size_ = 0;
  uint32_t entry_limit_;
};

template<typename T, PrimitiveType SLOT_TYPE>
class InListFilterImpl : public InListFilter {
 public:
  InListFilterImpl(ColumnType type, uint32_t entry_limit, bool contains_null = false):
      InListFilter(type, entry_limit, contains_null) {}
  ~InListFilterImpl() {}

  bool AlwaysFalse() override {
    return !always_true_ && !contains_null_ && set_values_.empty();
  }
  void Insert(const void* val) override;
  bool Find(void* val, const ColumnType& col_type) const noexcept override;
  void InsertBatch(const ColumnValueBatchPB& batch) override;
  int NumItems() const noexcept override {
    return set_values_.size() + (contains_null_ ? 1 : 0);
  }
  void ToProtobuf(InListFilterPB* protobuf) const override;
  void ToOrcLiteralList(std::vector<orc::Literal>* in_list) override {
    for (auto v : set_values_) in_list->emplace_back(static_cast<int64_t>(v));
  }

  std::string DebugString() const noexcept override;
 private:
  std::unordered_set<T> set_values_;
};
}
