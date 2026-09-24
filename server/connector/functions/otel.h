////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <array>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/sql_statement.hpp>
#include <string_view>

#include "otel/model.h"

namespace sdb::connector {

// OTLP payloads as table functions: each takes one ProtoJSON
// Export<Signal>ServiceRequest and emits rows shaped exactly like its target
// table, so a captured payload loads with one
// `INSERT INTO <table> SELECT * FROM otel_parse_*(<payload>)`.
//
//   otel_parse_logs(json)                          -> otel_logs rows
//   otel_parse_traces(json)                        -> otel_traces rows
//   otel_parse_metrics_gauge(json)                 -> otel_metrics_gauge rows
//   otel_parse_metrics_sum(json)                   -> otel_metrics_sum rows
//   otel_parse_metrics_histogram(json)             -> otel_metrics_histogram
//   rows otel_parse_metrics_exponential_histogram(json) ->
//   ..._exponential_histogram otel_parse_metrics_summary(json)               ->
//   otel_metrics_summary rows
//
// Columns the mapping does not produce are left NULL, so a deployment may add
// promoted columns to its own DDL without breaking the INSERT.
void RegisterOtelFunctions(duckdb::DatabaseInstance& db);

template<typename Request>
struct OtelRequestBox final : duckdb::TableFunctionInfo {
  const Request* request = nullptr;
};

using OtelLogsBox = OtelRequestBox<otel::ExportLogsRequest>;
using OtelTracesBox = OtelRequestBox<otel::ExportTracesRequest>;
using OtelMetricsBox = OtelRequestBox<otel::ExportMetricsRequest>;

duckdb::unique_ptr<duckdb::SQLStatement> OtelLogsInsert(
  duckdb::shared_ptr<OtelLogsBox> box);
duckdb::unique_ptr<duckdb::SQLStatement> OtelTracesInsert(
  duckdb::shared_ptr<OtelTracesBox> box);
duckdb::unique_ptr<duckdb::SQLStatement> OtelMetricsInsert(
  size_t table, duckdb::shared_ptr<OtelMetricsBox> box);

inline constexpr std::string_view kOtelSchema = "public";
inline constexpr std::string_view kOtelLogsTable = "otel_logs";
inline constexpr std::string_view kOtelTracesTable = "otel_traces";

inline constexpr std::array<std::string_view, 5> kOtelMetricTables{
  "otel_metrics_gauge",     "otel_metrics_sum",
  "otel_metrics_histogram", "otel_metrics_exponential_histogram",
  "otel_metrics_summary",
};

}  // namespace sdb::connector
