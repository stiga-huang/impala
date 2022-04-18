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
#include "runtime/string-value.inline.h"

namespace impala {

InListFilter::InListFilter(ColumnType type, uint32_t entry_limit, bool contains_null):
  always_true_(false), contains_null_(contains_null), type_(type.type),
  entry_limit_(entry_limit) {
}

bool InListFilter::AlwaysFalse(const InListFilterPB& filter) {
  return !filter.always_true() && !filter.contains_null() && filter.value_size() == 0;
}

InListFilter* InListFilter::Create(ColumnType type, uint32_t entry_limit,
    ObjectPool* pool, MemTracker* mem_tracker, bool contains_null) {
  InListFilter* res;
  switch (type.type) {
    case TYPE_TINYINT:
      res = new InListFilterImpl<int8_t, TYPE_TINYINT>(type, entry_limit, contains_null);
      break;
    case TYPE_SMALLINT:
      res = new InListFilterImpl<int16_t, TYPE_SMALLINT>(type, entry_limit,
          contains_null);
      break;
    case TYPE_INT:
      res = new InListFilterImpl<int32_t, TYPE_INT>(type, entry_limit, contains_null);
      break;
    case TYPE_BIGINT:
      res = new InListFilterImpl<int64_t, TYPE_BIGINT>(type, entry_limit, contains_null);
      break;
    case TYPE_DATE:
      // We use int64_t for DATE type as well
      res = new InListFilterImpl<int64_t, TYPE_DATE>(type, entry_limit, contains_null);
      break;
    case TYPE_STRING:
      res = new InListFilterImpl<StringValue, TYPE_STRING>(type, entry_limit,
          mem_tracker, contains_null);
      break;
    case TYPE_VARCHAR:
      res = new InListFilterImpl<StringValue, TYPE_VARCHAR>(type, entry_limit,
          mem_tracker, contains_null);
      break;
    case TYPE_CHAR:
      res = new InListFilterImpl<StringValue, TYPE_CHAR>(type, entry_limit,
          mem_tracker, contains_null);
      break;
    default:
      DCHECK(false) << "Not support IN-list filter type: " << TypeToString(type.type);
  }
  return pool->Add(res);
}

InListFilter* InListFilter::Create(const InListFilterPB& protobuf, ColumnType type,
    uint32_t entry_limit, ObjectPool* pool, MemTracker* mem_tracker) {
  InListFilter* filter = InListFilter::Create(type, entry_limit, pool, mem_tracker,
      protobuf.contains_null());
  filter->always_true_ = protobuf.always_true();
  filter->InsertBatch(protobuf.value());
  filter->MaterializeValues();
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

template<PrimitiveType SLOT_TYPE>
void InListFilterImpl<StringValue, SLOT_TYPE>::MaterializeValues() {
  if (new_values_total_len_ == 0) {
    if (!new_values_.empty()) {
      values_.insert(new_values_.begin(), new_values_.end());
    }
    return;
  }
  uint8_t* buffer = mem_pool_.Allocate(new_values_total_len_);
  if (buffer == nullptr) {
    VLOG_QUERY << "Not enough memory in materializing string IN-list filters: "
               << new_values_total_len_ << ", " << mem_pool_.DebugString();
    always_true_ = true;
    values_.clear();
    new_values_.clear();
    return;
  }
  for (const StringValue& s : new_values_) {
    std::memcpy(buffer, s.ptr, s.len);
    values_.insert(StringValue(reinterpret_cast<char*>(buffer), s.len));
    buffer += s.len;
  }
  new_values_.clear();
  new_values_total_len_ = 0;
}

#define IN_LIST_FILTER_INSERT_BATCH(TYPE, SLOT_TYPE, PB_VAL_METHOD)                      \
  template<>                                                                             \
  void InListFilterImpl<TYPE, SLOT_TYPE>::InsertBatch(const ColumnValueBatchPB& batch) { \
    for (const ColumnValuePB& v : batch) {                                               \
      DCHECK(v.has_##PB_VAL_METHOD());                                                   \
      values_.insert(v.PB_VAL_METHOD());                                                 \
    }                                                                                    \
  }

IN_LIST_FILTER_INSERT_BATCH(int8_t, TYPE_TINYINT, byte_val)
IN_LIST_FILTER_INSERT_BATCH(int16_t, TYPE_SMALLINT, short_val)
IN_LIST_FILTER_INSERT_BATCH(int32_t, TYPE_INT, int_val)
IN_LIST_FILTER_INSERT_BATCH(int64_t, TYPE_BIGINT, long_val)
IN_LIST_FILTER_INSERT_BATCH(int64_t, TYPE_DATE, long_val)

#define IN_LIST_FILTER_INSERT_STRING_BATCH(SLOT_TYPE)                                 \
  template<>                                                                          \
  void InListFilterImpl<StringValue, SLOT_TYPE>::InsertBatch(                         \
      const ColumnValueBatchPB& batch) {                                              \
    for (const ColumnValuePB& v : batch) {                                            \
      DCHECK(v.has_string_val());                                                     \
      values_.insert(StringValue(v.string_val()));                                    \
    }                                                                                 \
  }
IN_LIST_FILTER_INSERT_STRING_BATCH(TYPE_STRING)
IN_LIST_FILTER_INSERT_STRING_BATCH(TYPE_VARCHAR)
IN_LIST_FILTER_INSERT_STRING_BATCH(TYPE_CHAR)


#define NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(TYPE, SLOT_TYPE, PB_VAL_METHOD)             \
  template<>                                                                           \
  void InListFilterImpl<TYPE, SLOT_TYPE>::ToProtobuf(InListFilterPB* protobuf) const { \
    protobuf->set_always_true(always_true_);                                           \
    if (always_true_) return;                                                          \
    protobuf->set_contains_null(contains_null_);                                       \
    for (TYPE v : values_) {                                                           \
      ColumnValuePB* proto = protobuf->add_value();                                    \
      proto->set_##PB_VAL_METHOD(v);                                                   \
    }                                                                                  \
  }

NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int8_t, TYPE_TINYINT, byte_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int16_t, TYPE_SMALLINT, short_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int32_t, TYPE_INT, int_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int64_t, TYPE_BIGINT, long_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int64_t, TYPE_DATE, long_val)

#define STRING_IN_LIST_FILTER_TO_PROTOBUF(SLOT_TYPE)                                   \
  template<>                                                                           \
  void InListFilterImpl<StringValue, SLOT_TYPE>::ToProtobuf(InListFilterPB* protobuf)  \
      const {                                                                          \
    protobuf->set_always_true(always_true_);                                           \
    if (always_true_) return;                                                          \
    protobuf->set_contains_null(contains_null_);                                       \
    for (const StringValue& v : values_) {                                             \
      ColumnValuePB* proto = protobuf->add_value();                                    \
      proto->set_string_val(v.ptr, v.len);                                             \
    }                                                                                  \
  }

STRING_IN_LIST_FILTER_TO_PROTOBUF(TYPE_STRING)
STRING_IN_LIST_FILTER_TO_PROTOBUF(TYPE_VARCHAR)
STRING_IN_LIST_FILTER_TO_PROTOBUF(TYPE_CHAR)

template<>
void InListFilterImpl<int64_t, TYPE_DATE>::ToOrcLiteralList(
    vector<orc::Literal>* in_list) {
  for (int64_t v : values_) {
    in_list->emplace_back(orc::PredicateDataType::DATE, v);
  }
}

#define STRING_IN_LIST_FILTER_TO_ORC_LITERAL_LIST(SLOT_TYPE)            \
  template<>                                                            \
  void InListFilterImpl<StringValue, SLOT_TYPE>::ToOrcLiteralList(      \
      vector<orc::Literal>* in_list) {                                  \
    for (const StringValue& s : values_) {                              \
      in_list->emplace_back(s.ptr, s.len);                              \
    }                                                                   \
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
    for (TYPE v : values_) { \
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
  string InListFilterImpl<StringValue, SLOT_TYPE>::DebugString() const noexcept { \
    std::stringstream ss; \
  bool first_value = true;\
  ss << "IN-list filter: [";\
    for (const StringValue &s : values_) {\
      if (first_value) {\
        first_value = false;\
      } else {\
        ss << ',';\
      }\
      ss << "\"" << s.DebugString() << "\""; \
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
