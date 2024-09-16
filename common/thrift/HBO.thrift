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

//
// This file contains all structs, enums, etc., that uses in HBO.

namespace cpp impala
namespace java org.apache.impala.thrift

include "Exprs.thrift"

// Execution stats extracted from a query
struct THistoryStatsUpdate {
  1: optional list<TScanNodeCardinality> scan_node_cards
}

struct TScanNodeCardinality {
  // Key
  1: required string table_name
  2: required i64 catalog_version
  3: optional string conjuncts_string
  // Value
  4: required i64 num_rows
}
