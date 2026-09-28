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
#include <duckdb/main/database.hpp>
#include <string_view>

#include "otel/model.h"

namespace sdb::connector {

// OTLP payloads as table functions: each takes one
// Export{SignalName}ServiceRequest, as ProtoJSON or, with 'protobuf', base64
// protobuf, and emits rows shaped exactly like its target table, so a captured
// payload loads with one `INSERT INTO <table> SELECT * FROM
// otel_parse_*(<payload>)`.
//
// clang-format off
//   otel_parse_logs(payload [, encoding])                          -> otel_logs
//   otel_parse_traces(payload [, encoding])                        -> otel_traces
//   otel_parse_metrics_gauge(payload [, encoding])                 -> otel_metrics_gauge
//   otel_parse_metrics_sum(payload [, encoding])                   -> otel_metrics_sum
//   otel_parse_metrics_histogram(payload [, encoding])             -> otel_metrics_histogram
//   otel_parse_metrics_exponential_histogram(payload [, encoding]) -> otel_metrics_exponential_histogram
//   otel_parse_metrics_summary(payload [, encoding])               -> otel_metrics_summary
// clang-format on
//
// Columns the schema does not know are left NULL, so a deployment may add
// promoted columns to its own DDL without breaking the INSERT.
void RegisterOtelFunctions(duckdb::DatabaseInstance& db);

inline constexpr std::string_view kOtelSourceLogsFunction = "otel_source_logs";
inline constexpr std::string_view kOtelSourceTracesFunction =
  "otel_source_traces";
inline constexpr std::array<std::string_view, 5> kOtelSourceMetricsFunctions{
  "otel_source_metrics_gauge",     "otel_source_metrics_sum",
  "otel_source_metrics_histogram", "otel_source_metrics_exponential_histogram",
  "otel_source_metrics_summary",
};

inline constexpr std::string_view kOtelSchema = "public";
inline constexpr std::string_view kOtelLogsTable = "otel_logs";
inline constexpr std::string_view kOtelTracesTable = "otel_traces";

inline constexpr std::array<std::string_view, 5> kOtelMetricTables{
  "otel_metrics_gauge",     "otel_metrics_sum",
  "otel_metrics_histogram", "otel_metrics_exponential_histogram",
  "otel_metrics_summary",
};

}  // namespace sdb::connector
