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

#ifndef IMPALA_UTIL_DEBUG_UTIL_H
#define IMPALA_UTIL_DEBUG_UTIL_H

#include <ostream>
#include <string>
#include <sstream>

#include <thrift/protocol/TDebugProtocol.h>

#include "gen-cpp/JniCatalog_types.h"
#include "gen-cpp/Descriptors_types.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/RuntimeProfile_types.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/parquet_types.h"

#include "runtime/descriptors.h" // for SchemaPath

namespace impala {

class RowDescriptor;
class TableDescriptor;
class TupleDescriptor;
class Tuple;
class TupleRow;
class RowBatch;

// TODO: remove these functions and use operator << after upgrading to Thrift 0.11.0 or
// higher.
std::string PrintThriftEnum(const beeswax::QueryState::type& value);
std::string PrintThriftEnum(const parquet::Encoding::type& value);
std::string PrintThriftEnum(const TCatalogObjectType::type& value);
std::string PrintThriftEnum(const TCatalogOpType::type& value);
std::string PrintThriftEnum(const TDdlType::type& value);
std::string PrintThriftEnum(const TExplainLevel::type& value);
std::string PrintThriftEnum(const THdfsCompression::type& value);
std::string PrintThriftEnum(const THdfsFileFormat::type& value);
std::string PrintThriftEnum(const THdfsSeqCompressionMode::type& value);
std::string PrintThriftEnum(const TImpalaQueryOptions::type& value);
std::string PrintThriftEnum(const TJoinDistributionMode::type& value);
std::string PrintThriftEnum(const TKuduReadMode::type& value);
std::string PrintThriftEnum(const TMetricKind::type& value);
std::string PrintThriftEnum(const TParquetArrayResolution::type& value);
std::string PrintThriftEnum(const TParquetFallbackSchemaResolution::type& value);
std::string PrintThriftEnum(const TPlanNodeType::type& value);
std::string PrintThriftEnum(const TPrefetchMode::type& value);
std::string PrintThriftEnum(const TReplicaPreference::type& value);
std::string PrintThriftEnum(const TRuntimeFilterMode::type& value);
std::string PrintThriftEnum(const TSessionType::type& value);
std::string PrintThriftEnum(const TStmtType::type& value);
std::string PrintThriftEnum(const TUnit::type& value);

std::string PrintTuple(const Tuple* t, const TupleDescriptor& d);
std::string PrintRow(TupleRow* row, const RowDescriptor& d);
std::string PrintBatch(RowBatch* batch);
std::string PrintId(const TUniqueId& id, const std::string& separator = ":");

/// Returns the fully qualified path, e.g. "database.table.array_col.item.field"
std::string PrintPath(const TableDescriptor& tbl_desc, const SchemaPath& path);
/// Same as PrintPath(), but truncates the path after the given 'end_path_idx'.
std::string PrintSubPath(const TableDescriptor& tbl_desc, const SchemaPath& path,
    int end_path_idx);
/// Returns the numeric path without column/field names, e.g. "[0,1,2]"
std::string PrintNumericPath(const SchemaPath& path);

// Convenience wrapper around Thrift's debug string function
template<typename ThriftStruct> std::string PrintThrift(const ThriftStruct& t) {
  return apache::thrift::ThriftDebugString(t);
}

/// Parse 's' into a TUniqueId object.  The format of s needs to be the output format
/// from PrintId.  (<hi_part>:<low_part>)
/// Returns true if parse succeeded.
bool ParseId(const std::string& s, TUniqueId* id);

/// Returns a string "<product version number> (build <build hash>)"
/// If compact == false, this string is appended: "\nBuilt on <build time>"
/// This is used to set gflags build version
std::string GetBuildVersion(bool compact = false);

/// Returns "<program short name> version <GetBuildVersion(compact)>"
std::string GetVersionString(bool compact = false);

/// Returns the stack trace as a string from the current location.
/// Note: there is a libc bug that causes this not to work on 64 bit machines
/// for recursive calls.
std::string GetStackTrace();

/// Returns the backend name in "host:port" form suitable for human consumption.
std::string GetBackendString();

/// Slow path implementing SleepIfSetInDebugOptions() for the case where 'query_options'
/// is non-empty.
void SleepIfSetInDebugOptionsImpl(
    const TQueryOptions& query_options, const string& sleep_label);

/// If sleep time is specified in the debug_action query option in the format
/// <sleep_label>:<sleep_time_ms>, where <sleep_label> is a string and <sleep_time_ms>
/// is an integer, and 'sleep_label' matches the one specified in the query option, then
/// this methods extracts the corresponding <sleep_time_ms> and initiates a sleep for that
/// many milliseconds.
static inline void SleepIfSetInDebugOptions(
    const TQueryOptions& query_options, const string& sleep_label) {
  // Make sure this is very cheap if debug actions are not in use.
  if (LIKELY(query_options.debug_action.empty())) return;
  return SleepIfSetInDebugOptionsImpl(query_options, sleep_label);
}

// FILE_CHECKs are conditions that we expect to be true but could fail due to a malformed
// input file. They differentiate these cases from DCHECKs, which indicate conditions that
// are true unless there's a bug in Impala. We would ideally always return a bad Status
// instead of failing a FILE_CHECK, but in many cases we use FILE_CHECK instead because
// there's a performance cost to doing the check in a release build, or just due to legacy
// code.
#define FILE_CHECK(a) DCHECK(a)
#define FILE_CHECK_EQ(a, b) DCHECK_EQ(a, b)
#define FILE_CHECK_NE(a, b) DCHECK_NE(a, b)
#define FILE_CHECK_GT(a, b) DCHECK_GT(a, b)
#define FILE_CHECK_LT(a, b) DCHECK_LT(a, b)
#define FILE_CHECK_GE(a, b) DCHECK_GE(a, b)
#define FILE_CHECK_LE(a, b) DCHECK_LE(a, b)

}

#endif
