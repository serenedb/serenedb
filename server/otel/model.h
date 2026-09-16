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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace sdb::otel {

struct AnyValue;

struct KeyValue {
  std::string key;
  AnyValue* value = nullptr;
};

using KeyValueList = std::vector<KeyValue>;

struct ArrayValue {
  std::vector<AnyValue*> values;
};

struct KvlistValue {
  KeyValueList values;
};

struct BytesValue {
  std::string data;
};

struct AnyValue {
  std::variant<std::monostate, std::string, bool, int64_t, double, ArrayValue,
               KvlistValue, BytesValue>
    value;
};

class ValueArena {
 public:
  AnyValue* Make() {
    return _values.emplace_back(std::make_unique<AnyValue>()).get();
  }

 private:
  std::vector<std::unique_ptr<AnyValue>> _values;
};

struct Resource {
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
};

struct InstrumentationScope {
  std::string name;
  std::string version;
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
};

enum class SeverityNumber : int32_t {
  Unspecified = 0,
  Trace = 1,
  Debug = 5,
  Info = 9,
  Warn = 13,
  Error = 17,
  Fatal = 21,
};

struct LogRecord {
  uint64_t time_unix_nano = 0;
  uint64_t observed_time_unix_nano = 0;
  int32_t severity_number = 0;
  std::string severity_text;
  std::string event_name;
  AnyValue* body = nullptr;
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
  uint32_t flags = 0;
  std::string trace_id;
  std::string span_id;
};

enum class SpanKind : int32_t {
  Unspecified = 0,
  Internal = 1,
  Server = 2,
  Client = 3,
  Producer = 4,
  Consumer = 5,
};

enum class StatusCode : int32_t {
  Unset = 0,
  Ok = 1,
  Error = 2,
};

struct Status {
  StatusCode code = StatusCode::Unset;
  std::string message;
};

struct SpanEvent {
  uint64_t time_unix_nano = 0;
  std::string name;
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
};

struct SpanLink {
  std::string trace_id;
  std::string span_id;
  std::string trace_state;
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
  uint32_t flags = 0;
};

struct Span {
  std::string trace_id;
  std::string span_id;
  std::string trace_state;
  std::string parent_span_id;
  uint32_t flags = 0;
  std::string name;
  SpanKind kind = SpanKind::Unspecified;
  uint64_t start_time_unix_nano = 0;
  uint64_t end_time_unix_nano = 0;
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
  std::vector<SpanEvent> events;
  uint32_t dropped_events_count = 0;
  std::vector<SpanLink> links;
  uint32_t dropped_links_count = 0;
  Status status;
};

enum class AggregationTemporality : int32_t {
  Unspecified = 0,
  Delta = 1,
  Cumulative = 2,
};

struct Exemplar {
  KeyValueList filtered_attributes;
  uint64_t time_unix_nano = 0;
  std::variant<std::monostate, int64_t, double> value;
  std::string span_id;
  std::string trace_id;
};

struct NumberDataPoint {
  KeyValueList attributes;
  uint64_t start_time_unix_nano = 0;
  uint64_t time_unix_nano = 0;
  std::variant<std::monostate, int64_t, double> value;
  std::vector<Exemplar> exemplars;
  uint32_t flags = 0;
};

struct HistogramDataPoint {
  KeyValueList attributes;
  uint64_t start_time_unix_nano = 0;
  uint64_t time_unix_nano = 0;
  uint64_t count = 0;
  std::optional<double> sum;
  std::vector<uint64_t> bucket_counts;
  std::vector<double> explicit_bounds;
  std::vector<Exemplar> exemplars;
  uint32_t flags = 0;
  std::optional<double> min;
  std::optional<double> max;
};

struct ExponentialHistogramBuckets {
  int32_t offset = 0;
  std::vector<uint64_t> bucket_counts;
};

struct ExponentialHistogramDataPoint {
  KeyValueList attributes;
  uint64_t start_time_unix_nano = 0;
  uint64_t time_unix_nano = 0;
  uint64_t count = 0;
  std::optional<double> sum;
  int32_t scale = 0;
  uint64_t zero_count = 0;
  ExponentialHistogramBuckets positive;
  ExponentialHistogramBuckets negative;
  uint32_t flags = 0;
  std::vector<Exemplar> exemplars;
  std::optional<double> min;
  std::optional<double> max;
  double zero_threshold = 0;
};

struct SummaryQuantileValue {
  double quantile = 0;
  double value = 0;
};

struct SummaryDataPoint {
  KeyValueList attributes;
  uint64_t start_time_unix_nano = 0;
  uint64_t time_unix_nano = 0;
  uint64_t count = 0;
  double sum = 0;
  std::vector<SummaryQuantileValue> quantile_values;
  uint32_t flags = 0;
};

struct Gauge {
  std::vector<NumberDataPoint> data_points;
};

struct Sum {
  std::vector<NumberDataPoint> data_points;
  AggregationTemporality aggregation_temporality =
    AggregationTemporality::Unspecified;
  bool is_monotonic = false;
};

struct Histogram {
  std::vector<HistogramDataPoint> data_points;
  AggregationTemporality aggregation_temporality =
    AggregationTemporality::Unspecified;
};

struct ExponentialHistogram {
  std::vector<ExponentialHistogramDataPoint> data_points;
  AggregationTemporality aggregation_temporality =
    AggregationTemporality::Unspecified;
};

struct Summary {
  std::vector<SummaryDataPoint> data_points;
};

struct Metric {
  std::string name;
  std::string description;
  std::string unit;
  std::variant<std::monostate, Gauge, Sum, Histogram, ExponentialHistogram,
               Summary>
    data;
  KeyValueList metadata;
};

template<typename Record>
struct ScopeRecords {
  InstrumentationScope scope;
  std::vector<Record> records;
  std::string schema_url;
};

template<typename Record>
struct ResourceRecords {
  Resource resource;
  std::vector<ScopeRecords<Record>> scopes;
  std::string schema_url;
};

template<typename Record>
struct ExportRequest {
  std::vector<ResourceRecords<Record>> resources;
  ValueArena arena;
};

using ExportLogsRequest = ExportRequest<LogRecord>;
using ExportTracesRequest = ExportRequest<Span>;
using ExportMetricsRequest = ExportRequest<Metric>;

}  // namespace sdb::otel
