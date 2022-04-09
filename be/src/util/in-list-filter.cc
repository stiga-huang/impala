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

#include "util/in-list-filter.h"

#include "common/object-pool.h"

namespace impala {

bool InListFilter::AlwaysFalse(const InListFilterPB& filter) {
  return !filter.always_true() && !filter.contains_null() && filter.value_size() == 0;
}

template<typename T, PrimitiveType SLOT_TYPE>
bool InListFilterImpl<T, SLOT_TYPE>::Find(void* val, const ColumnType& col_type) const noexcept {
  if (always_true_) return true;
  if (val == nullptr) return contains_null_;
  DCHECK_EQ(type_, col_type.type);
  T v = *reinterpret_cast<const T*>(val);
  return set_values_.find(v) != set_values_.end();
}

template<>
bool InListFilterImpl<int64_t, TYPE_DATE>::Find(void* val, const ColumnType& col_type)
    const noexcept {
  if (always_true_) return true;
  if (val == nullptr) return contains_null_;
  DCHECK_EQ(type_, col_type.type);
  int64 v = reinterpret_cast<const DateValue*>(val)->Value();
  return set_values_.find(v) != set_values_.end();
}

template<>
bool InListFilterImpl<string, TYPE_STRING>::Find(void* val, const ColumnType& col_type)
    const noexcept {
  if (always_true_) return true;
  if (val == nullptr) return contains_null_;
  DCHECK_EQ(type_, col_type.type);
  const StringValue* s = reinterpret_cast<const StringValue*>(val);
  return set_values_.find(string(s->ptr, s->len)) != set_values_.end();
}

template<>
bool InListFilterImpl<string, TYPE_VARCHAR>::Find(void* val, const ColumnType& col_type)
const noexcept {
  if (always_true_) return true;
  if (val == nullptr) return contains_null_;
  DCHECK_EQ(type_, col_type.type);
  return set_values_.find(string(reinterpret_cast<const char*>(val), col_type.len))
      != set_values_.end();
}

template<>
bool InListFilterImpl<string, TYPE_CHAR>::Find(void* val, const ColumnType& col_type)
const noexcept {
  if (always_true_) return true;
  if (val == nullptr) return contains_null_;
  DCHECK_EQ(type_, col_type.type);
  const StringValue* s = reinterpret_cast<const StringValue*>(val);
  return set_values_.find(string(s->ptr, s->len)) != set_values_.end();
}

#define IN_LIST_FILTER_INSERT_BATCH(TYPE, SLOT_TYPE, PB_VAL_METHOD)                      \
  template<>                                                                             \
  void InListFilterImpl<TYPE, SLOT_TYPE>::InsertBatch(const ColumnValueBatchPB& batch) { \
    for (const ColumnValuePB& v : batch) {                                    \
      DCHECK(v.has_##PB_VAL_METHOD());                                        \
      set_values_.insert(v.PB_VAL_METHOD());                                  \
    }                                                                         \
  }

IN_LIST_FILTER_INSERT_BATCH(int8_t, TYPE_TINYINT, byte_val)
IN_LIST_FILTER_INSERT_BATCH(int16_t, TYPE_SMALLINT, short_val)
IN_LIST_FILTER_INSERT_BATCH(int32_t, TYPE_INT, int_val)
IN_LIST_FILTER_INSERT_BATCH(int64_t, TYPE_BIGINT, long_val)
// We use int64_t for DATE type as well
IN_LIST_FILTER_INSERT_BATCH(int64_t, TYPE_DATE, long_val)
IN_LIST_FILTER_INSERT_BATCH(string, TYPE_STRING, string_val)
IN_LIST_FILTER_INSERT_BATCH(string, TYPE_VARCHAR, string_val)
IN_LIST_FILTER_INSERT_BATCH(string, TYPE_CHAR, string_val)

InListFilter::InListFilter(ColumnType type, uint32_t entry_limit, bool contains_null):
  always_true_(false), contains_null_(contains_null), type_(type.type),
  entry_limit_(entry_limit) {
  if (type.type == TYPE_CHAR) type_len_ = type.len;
}

InListFilter* InListFilter::Create(ColumnType type, uint32_t entry_limit,
    ObjectPool* pool, bool contains_null) {
  InListFilter* res;
  switch (type.type) {
    case TYPE_TINYINT:
      res = new InListFilterImpl<int8_t, TYPE_TINYINT>(type, entry_limit, contains_null);
      break;
    case TYPE_SMALLINT:
      res = new InListFilterImpl<int16_t, TYPE_SMALLINT>(type, entry_limit, contains_null);
      break;
    case TYPE_INT:
      res = new InListFilterImpl<int32_t, TYPE_INT>(type, entry_limit, contains_null);
      break;
    case TYPE_BIGINT:
      res = new InListFilterImpl<int64_t, TYPE_BIGINT>(type, entry_limit, contains_null);
      break;
    case TYPE_DATE:
      res = new InListFilterImpl<int64_t, TYPE_DATE>(type, entry_limit, contains_null);
      break;
    case TYPE_STRING:
      res = new InListFilterImpl<string, TYPE_STRING>(type, entry_limit, contains_null);
      break;
    case TYPE_VARCHAR:
      res = new InListFilterImpl<string, TYPE_VARCHAR>(type, entry_limit, contains_null);
      break;
    case TYPE_CHAR:
      res = new InListFilterImpl<string, TYPE_CHAR>(type, entry_limit, contains_null);
      break;
    default:
      DCHECK(false) << "Not support IN-list filter type: " << TypeToString(type.type);
  }
  return pool->Add(res);
}

InListFilter* InListFilter::Create(const InListFilterPB& protobuf, ColumnType type,
    uint32_t entry_limit, ObjectPool* pool) {
  InListFilter* filter = InListFilter::Create(type, entry_limit, pool,
      protobuf.contains_null());
  filter->always_true_ = protobuf.always_true();
  filter->InsertBatch(protobuf.value());
  return filter;
}

void InListFilter::ToProtobuf(const InListFilter* filter, InListFilterPB* protobuf) {
  DCHECK(protobuf != nullptr);
  if (filter == nullptr) {
    protobuf->set_always_true(true);
    return;
  }
  filter->ToProtobuf(protobuf);
}

#define NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(TYPE, SLOT_TYPE, PB_VAL_METHOD)             \
  template<>                                                                           \
  void InListFilterImpl<TYPE, SLOT_TYPE>::ToProtobuf(InListFilterPB* protobuf) const { \
    protobuf->set_always_true(always_true_);                                           \
    if (always_true_) return;                                                          \
    protobuf->set_contains_null(contains_null_);                                       \
    for (TYPE v : set_values_) {                                                       \
      ColumnValuePB* proto = protobuf->add_value();                                    \
      proto->set_##PB_VAL_METHOD(v);                                                   \
    }                                                                                  \
  }

NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int8_t, TYPE_TINYINT, byte_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int16_t, TYPE_SMALLINT, short_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int32_t, TYPE_INT, int_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int64_t, TYPE_BIGINT, long_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int64_t, TYPE_DATE, long_val)

#define STRING_IN_LIST_FILTER_TO_PROTOBUF(TYPE, SLOT_TYPE)                             \
  template<>                                                                           \
  void InListFilterImpl<TYPE, SLOT_TYPE>::ToProtobuf(InListFilterPB* protobuf) const { \
    protobuf->set_always_true(always_true_);                                           \
    if (always_true_) return;                                                          \
    protobuf->set_contains_null(contains_null_);                                       \
    for (const string& v : set_values_) {                                              \
      ColumnValuePB* proto = protobuf->add_value();                                    \
      proto->set_string_val(v);                                                        \
    }                                                                                  \
  }

STRING_IN_LIST_FILTER_TO_PROTOBUF(string, TYPE_STRING)
STRING_IN_LIST_FILTER_TO_PROTOBUF(string, TYPE_VARCHAR)
STRING_IN_LIST_FILTER_TO_PROTOBUF(string, TYPE_CHAR)

template<>
void InListFilterImpl<int64_t, TYPE_DATE>::ToOrcLiteralList(
    vector<orc::Literal>* in_list) {
  for (int64_t v : set_values_) {
    in_list->emplace_back(orc::PredicateDataType::DATE, v);
  }
}

#define STRING_IN_LIST_FILTER_TO_ORC_LITERAL_LIST(SLOT_TYPE)  \
  template<>                                                  \
  void InListFilterImpl<string, SLOT_TYPE>::ToOrcLiteralList( \
      vector<orc::Literal>* in_list) {                        \
    for (const string& str : set_values_) {                   \
      in_list->emplace_back(str.c_str(), str.length());       \
    }                                                         \
  }

STRING_IN_LIST_FILTER_TO_ORC_LITERAL_LIST(TYPE_STRING)
STRING_IN_LIST_FILTER_TO_ORC_LITERAL_LIST(TYPE_VARCHAR)
STRING_IN_LIST_FILTER_TO_ORC_LITERAL_LIST(TYPE_CHAR)

#define NUMERIC_IN_LIST_FILTER_DEBUG_STRING(TYPE, SLOT_TYPE) \
template<>\
string InListFilterImpl<TYPE, SLOT_TYPE>::DebugString() const noexcept { \
  std::stringstream ss; \
  bool first_value = true;\
  ss << "IN-list filter: [";\
    for (TYPE v : set_values_) { \
      if (first_value) {\
        first_value = false;\
      } else {\
        ss << ',';\
      }\
      ss << v;\
    }\
  if (contains_null_) {\
    if (!first_value) ss << ',';\
    ss << "NULL";\
  }\
  ss << ']';\
  return ss.str();\
}
NUMERIC_IN_LIST_FILTER_DEBUG_STRING(int8_t, TYPE_TINYINT)
NUMERIC_IN_LIST_FILTER_DEBUG_STRING(int16_t, TYPE_SMALLINT)
NUMERIC_IN_LIST_FILTER_DEBUG_STRING(int32_t, TYPE_INT)
NUMERIC_IN_LIST_FILTER_DEBUG_STRING(int64_t, TYPE_BIGINT)
NUMERIC_IN_LIST_FILTER_DEBUG_STRING(int64_t, TYPE_DATE)

#define STRING_IN_LIST_FILTER_DEBUG_STRING(SLOT_TYPE)   \
  template<>                                                  \
  string InListFilterImpl<string, SLOT_TYPE>::DebugString() const noexcept { \
    std::stringstream ss; \
  bool first_value = true;\
  ss << "IN-list filter: [";\
    for (const string &s : set_values_) {\
      if (first_value) {\
        first_value = false;\
      } else {\
        ss << ',';\
      }\
      ss << "\"" << s << "\""; \
    }\
  if (contains_null_) {\
    if (!first_value) ss << ',';\
    ss << "NULL";\
  }\
  ss << ']';\
  return ss.str();\
}
STRING_IN_LIST_FILTER_DEBUG_STRING(TYPE_STRING)
STRING_IN_LIST_FILTER_DEBUG_STRING(TYPE_VARCHAR)
STRING_IN_LIST_FILTER_DEBUG_STRING(TYPE_CHAR)

string InListFilter::DebugString(const InListFilterPB& filter) {
  std::stringstream ss;
  ss << "IN-list filter: " << DebugStringOfList(filter);
  return ss.str();
}

string InListFilter::DebugStringOfList(const InListFilterPB& filter) {
  std::stringstream ss;
  ss << "[";
  bool first_value = true;
  for (const ColumnValuePB& v : filter.value()) {
    if (first_value) {
      first_value = false;
    } else {
      ss << ',';
    }
    if (v.has_byte_val()) {
      ss << v.byte_val();
    } else if (v.has_short_val()) {
      ss << v.short_val();
    } else if (v.has_int_val()) {
      ss << v.int_val();
    } else if (v.has_long_val()) {
      ss << v.long_val();
    } else if (v.has_date_val()) {
      ss << v.date_val();
    } else if (v.has_string_val()) {
      ss << "\"" << v.string_val() << "\"";
    }
  }
  ss << ']';
  return ss.str();
}

} // namespace impala
