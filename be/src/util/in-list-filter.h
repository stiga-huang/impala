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
#include <boost/unordered_set.hpp>

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
  virtual void Close() {}

  /// Add a new value to the list.
  virtual void Insert(const void* val) = 0;

  /// Materialize filter values by copying any values stored by filters into memory owned
  /// by the filter. Filters may assume that the memory for Insert()-ed values stays valid
  /// until this is called.
  virtual void MaterializeValues() {}

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
  static InListFilter* Create(ColumnType type, uint32_t entry_limit, ObjectPool* pool,
      MemTracker* mem_tracker, bool contains_null = false);

  /// Returns a new InListFilter created from the protobuf representation, allocated from
  /// 'mem_tracker'.
  static InListFilter* Create(const InListFilterPB& protobuf, ColumnType type,
      uint32_t entry_limit, ObjectPool* pool, MemTracker* mem_tracker);

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
  uint32_t entry_limit_;
};

template<typename T, PrimitiveType SLOT_TYPE>
class InListFilterImpl : public InListFilter {
 public:
  InListFilterImpl(ColumnType type, uint32_t entry_limit, bool contains_null = false):
      InListFilter(type, entry_limit, contains_null) {}
  ~InListFilterImpl() {}

  bool AlwaysFalse() override;
  int NumItems() const noexcept override;

  void Insert(const void* val) override;
  void InsertBatch(const ColumnValueBatchPB& batch) override;

  bool Find(void* val, const ColumnType& col_type) const noexcept override {
    if (always_true_) return true;
    if (val == nullptr) return contains_null_;
    DCHECK_EQ(type_, col_type.type);
    T v = *reinterpret_cast<const T*>(val);
    return set_values_.find(v) != set_values_.end();
  }

  void ToProtobuf(InListFilterPB* protobuf) const override;

  void ToOrcLiteralList(std::vector<orc::Literal>* in_list) override {
    for (auto v : set_values_) in_list->emplace_back(static_cast<int64_t>(v));
  }

  std::string DebugString() const noexcept override;
 private:
  std::unordered_set<T> set_values_;
};

template<PrimitiveType SLOT_TYPE>
class InListFilterImpl<StringValue, SLOT_TYPE> : public InListFilter {
 public:
  InListFilterImpl(ColumnType type, uint32_t entry_limit, MemTracker* mem_tracker,
      bool contains_null = false):
      InListFilter(type, entry_limit, contains_null), mem_pool_(mem_tracker) {
    if (SLOT_TYPE == TYPE_CHAR) type_len_ = type.len;
  }
  ~InListFilterImpl() {}
  void Close() override { mem_pool_.FreeAll(); }

  bool AlwaysFalse() override {
    return !always_true_ && !contains_null_ && set_values_.empty();
  }
  int NumItems() const noexcept override {
    return set_values_.size() + (contains_null_ ? 1 : 0);
  }

  void Insert(const void* val) override;
  void InsertBatch(const ColumnValueBatchPB& batch) override;
  void MaterializeValues() override {
    if (new_values_total_len_ == 0) {
      if (!new_values_.empty()) {
        set_values_.insert(new_values_.begin(), new_values_.end());
      }
      return;
    }
    uint8_t* buffer = mem_pool_.Allocate(new_values_total_len_);
    if (buffer == nullptr) {
      VLOG_QUERY << "Not enough memory in materializing string IN-list filters: "
                 << new_values_total_len_ << ", " << mem_pool_.DebugString();
      always_true_ = true;
      set_values_.clear();
      new_values_.clear();
      return;
    }
    for (const StringValue& s : new_values_) {
      std::memcpy(buffer, s.ptr, s.len);
      set_values_.insert(StringValue(reinterpret_cast<char*>(buffer), s.len));
      buffer += s.len;
    }
    new_values_.clear();
    new_values_total_len_ = 0;
  }
  bool Find(void* val, const ColumnType& col_type) const noexcept override;

  void ToProtobuf(InListFilterPB* protobuf) const override;
  void ToOrcLiteralList(std::vector<orc::Literal>* in_list) override;
  std::string DebugString() const noexcept override;
 private:
  MemPool mem_pool_;
  boost::unordered_set<StringValue> set_values_;
  boost::unordered_set<StringValue> new_values_;
  size_t new_values_total_len_ = 0;
  // Type len for CHAR type.
  int type_len_;
  uint32_t str_total_size_ = 0;
};
}
