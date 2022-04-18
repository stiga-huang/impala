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

#include "common/object-pool.h"
#include "runtime/string-value.inline.h"
#include "util/in-list-filter.h"

namespace impala {

template<>
int64_t InListFilterImpl<int64_t, TYPE_DATE>::GetValue(const void* val) {
  return reinterpret_cast<const DateValue*>(val)->Value();
}

#define NUMERIC_IN_LIST_FILTER_INSERT(TYPE, SLOT_TYPE)              \
  template<>                                                        \
  void InListFilterImpl<TYPE, SLOT_TYPE>::Insert(const void* val) { \
    if (always_true_) return;                                       \
    if (UNLIKELY(val == nullptr)) {                                 \
      contains_null_ = true;                                        \
      return;                                                       \
    }                                                               \
    const auto& res = values_.insert(GetValue(val));                \
    if (res.second) {                                               \
      ++total_entries_;                                             \
      if (UNLIKELY(total_entries_ > entry_limit_)) {                \
        always_true_ = true;                                        \
        values_.clear();                                            \
      }                                                             \
    }                                                               \
  }

NUMERIC_IN_LIST_FILTER_INSERT(int8_t, TYPE_TINYINT)
NUMERIC_IN_LIST_FILTER_INSERT(int16_t, TYPE_SMALLINT)
NUMERIC_IN_LIST_FILTER_INSERT(int32_t, TYPE_INT)
NUMERIC_IN_LIST_FILTER_INSERT(int64_t, TYPE_BIGINT)
NUMERIC_IN_LIST_FILTER_INSERT(int64_t, TYPE_DATE)

template<>
StringValue InListFilterImpl<StringValue, TYPE_CHAR>::GetValue(const void* val,
    int char_type_len) {
  return {const_cast<char*>(reinterpret_cast<const char*>(val)), char_type_len};
}

#define STRING_IN_LIST_FILTER_INSERT(SLOT_TYPE)                              \
  template<>                                                                 \
  void InListFilterImpl<StringValue, SLOT_TYPE>::Insert(const void* val) {   \
    if (always_true_) return;                                                \
    if (UNLIKELY(val == nullptr)) {                                          \
      contains_null_ = true;                                                 \
      return;                                                                \
    }                                                                        \
    StringValue s = GetValue(val, type_len_);                                \
    if (values_.find(s) == values_.end()) {                                  \
      const auto& res = new_values_.insert(s);                               \
      if (res.second) {                                                      \
        str_total_size_ += s.len;                                            \
        new_values_total_len_ += s.len;                                      \
        ++total_entries_;                                                    \
        if (UNLIKELY(total_entries_ > entry_limit_                           \
            || str_total_size_ >= STRING_SET_MAX_TOTAL_LENGTH)) {            \
          always_true_ = true;                                               \
          values_.clear();                                                   \
          new_values_.clear();                                               \
          return;                                                            \
        }                                                                    \
      }                                                                      \
    }                                                                        \
  }

STRING_IN_LIST_FILTER_INSERT(TYPE_STRING)
STRING_IN_LIST_FILTER_INSERT(TYPE_VARCHAR)
STRING_IN_LIST_FILTER_INSERT(TYPE_CHAR)

} // namespace impala
