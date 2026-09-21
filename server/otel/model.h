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

// The OTLP data model, transcribed from opentelemetry-proto. One in-memory
// form, filled by either decoder (protojson.cpp, protobuf.cpp) and consumed by
// mapper.cpp, so every ingestion route produces the same rows.
//
// https://github.com/open-telemetry/opentelemetry-proto
//   common/v1/common.proto      AnyValue, KeyValue, InstrumentationScope
//   resource/v1/resource.proto  Resource
//   logs/v1/logs.proto          LogRecord
//   trace/v1/trace.proto        Span, Event, Link, Status
//   metrics/v1/metrics.proto    Gauge, Sum, Histogram, ExponentialHistogram,
//                               Summary, Exemplar
namespace sdb::otel {

struct AnyValue;

struct KeyValue {
  std::string key;
  AnyValue* value = nullptr;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/common/v1/common.proto
// KeyValue
enum class KeyValueTag : uint32_t {
  Key = 1,
  Value = 2,
};

using KeyValueList = std::vector<KeyValue>;

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/common/v1/common.proto
// KeyValueList
enum class KeyValueListTag : uint32_t {
  Values = 1,
};

struct ArrayValue {
  std::vector<AnyValue*> values;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/common/v1/common.proto
// ArrayValue
enum class ArrayValueTag : uint32_t {
  Values = 1,
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

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/common/v1/common.proto
// AnyValue
enum class AnyValueTag : uint32_t {
  StringValue = 1,
  BoolValue = 2,
  IntValue = 3,
  DoubleValue = 4,
  ArrayValue = 5,
  KvlistValue = 6,
  BytesValue = 7,
};

class ValueArena {
 public:
  AnyValue* Make() {
    return _values.emplace_back(std::make_unique<AnyValue>()).get();
  }

 private:
  std::vector<std::unique_ptr<AnyValue>> _values;
};

template<size_t HexLength>
struct HexId {
  std::string hex;
};

using TraceId = HexId<32>;
using SpanId = HexId<16>;

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto
enum class SeverityNumber : int32_t {
  Unspecified = 0,
  Trace = 1,
  Trace2 = 2,
  Trace3 = 3,
  Trace4 = 4,
  Debug = 5,
  Debug2 = 6,
  Debug3 = 7,
  Debug4 = 8,
  Info = 9,
  Info2 = 10,
  Info3 = 11,
  Info4 = 12,
  Warn = 13,
  Warn2 = 14,
  Warn3 = 15,
  Warn4 = 16,
  Error = 17,
  Error2 = 18,
  Error3 = 19,
  Error4 = 20,
  Fatal = 21,
  Fatal2 = 22,
  Fatal3 = 23,
  Fatal4 = 24,
};

struct Resource {
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/resource/v1/resource.proto
// Resource
enum class ResourceTag : uint32_t {
  Attributes = 1,
  DroppedAttributesCount = 2,
};

struct InstrumentationScope {
  std::string name;
  std::string version;
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/common/v1/common.proto
// InstrumentationScope
enum class InstrumentationScopeTag : uint32_t {
  Name = 1,
  Version = 2,
  Attributes = 3,
  DroppedAttributesCount = 4,
};

struct LogRecord {
  uint64_t time_unix_nano = 0;
  uint64_t observed_time_unix_nano = 0;
  SeverityNumber severity_number = SeverityNumber::Unspecified;
  std::string severity_text;
  std::string event_name;
  AnyValue* body = nullptr;
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
  uint32_t flags = 0;
  TraceId trace_id;
  SpanId span_id;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto
// LogRecord
enum class LogRecordTag : uint32_t {
  TimeUnixNano = 1,
  SeverityNumber = 2,
  SeverityText = 3,
  Body = 5,
  Attributes = 6,
  DroppedAttributesCount = 7,
  Flags = 8,
  TraceId = 9,
  SpanId = 10,
  ObservedTimeUnixNano = 11,
  EventName = 12,
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

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto
// Status
enum class StatusTag : uint32_t {
  Message = 2,
  Code = 3,
};

struct SpanEvent {
  uint64_t time_unix_nano = 0;
  std::string name;
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto
// Span.Event
enum class SpanEventTag : uint32_t {
  TimeUnixNano = 1,
  Name = 2,
  Attributes = 3,
  DroppedAttributesCount = 4,
};

struct SpanLink {
  TraceId trace_id;
  SpanId span_id;
  std::string trace_state;
  KeyValueList attributes;
  uint32_t dropped_attributes_count = 0;
  uint32_t flags = 0;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto
// Span.Link
enum class SpanLinkTag : uint32_t {
  TraceId = 1,
  SpanId = 2,
  TraceState = 3,
  Attributes = 4,
  DroppedAttributesCount = 5,
  Flags = 6,
};

struct Span {
  TraceId trace_id;
  SpanId span_id;
  std::string trace_state;
  SpanId parent_span_id;
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

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto
// Span
enum class SpanTag : uint32_t {
  TraceId = 1,
  SpanId = 2,
  TraceState = 3,
  ParentSpanId = 4,
  Name = 5,
  Kind = 6,
  StartTimeUnixNano = 7,
  EndTimeUnixNano = 8,
  Attributes = 9,
  DroppedAttributesCount = 10,
  Events = 11,
  DroppedEventsCount = 12,
  Links = 13,
  DroppedLinksCount = 14,
  Status = 15,
  Flags = 16,
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
  SpanId span_id;
  TraceId trace_id;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
// Exemplar
enum class ExemplarTag : uint32_t {
  TimeUnixNano = 2,
  AsDouble = 3,
  SpanId = 4,
  TraceId = 5,
  AsInt = 6,
  FilteredAttributes = 7,
};

struct NumberDataPoint {
  KeyValueList attributes;
  uint64_t start_time_unix_nano = 0;
  uint64_t time_unix_nano = 0;
  std::variant<std::monostate, int64_t, double> value;
  std::vector<Exemplar> exemplars;
  uint32_t flags = 0;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
// NumberDataPoint
enum class NumberDataPointTag : uint32_t {
  StartTimeUnixNano = 2,
  TimeUnixNano = 3,
  AsDouble = 4,
  Exemplars = 5,
  AsInt = 6,
  Attributes = 7,
  Flags = 8,
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

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
// HistogramDataPoint
enum class HistogramDataPointTag : uint32_t {
  StartTimeUnixNano = 2,
  TimeUnixNano = 3,
  Count = 4,
  Sum = 5,
  BucketCounts = 6,
  ExplicitBounds = 7,
  Exemplars = 8,
  Attributes = 9,
  Flags = 10,
  Min = 11,
  Max = 12,
};

struct ExponentialHistogramBuckets {
  int32_t offset = 0;
  std::vector<uint64_t> bucket_counts;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
// ExponentialHistogramDataPoint.Buckets
enum class ExponentialHistogramBucketsTag : uint32_t {
  Offset = 1,
  BucketCounts = 2,
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

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
// ExponentialHistogramDataPoint
enum class ExponentialHistogramDataPointTag : uint32_t {
  Attributes = 1,
  StartTimeUnixNano = 2,
  TimeUnixNano = 3,
  Count = 4,
  Sum = 5,
  Scale = 6,
  ZeroCount = 7,
  Positive = 8,
  Negative = 9,
  Flags = 10,
  Exemplars = 11,
  Min = 12,
  Max = 13,
  ZeroThreshold = 14,
};

struct SummaryQuantileValue {
  double quantile = 0;
  double value = 0;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
// SummaryDataPoint.ValueAtQuantile
enum class SummaryQuantileValueTag : uint32_t {
  Quantile = 1,
  Value = 2,
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

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
// SummaryDataPoint
enum class SummaryDataPointTag : uint32_t {
  StartTimeUnixNano = 2,
  TimeUnixNano = 3,
  Count = 4,
  Sum = 5,
  QuantileValues = 6,
  Attributes = 7,
  Flags = 8,
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

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
// Gauge, Sum, Histogram, ExponentialHistogram and Summary share this
// layout; only Sum carries is_monotonic.
enum class MetricShapeTag : uint32_t {
  DataPoints = 1,
  AggregationTemporality = 2,
  IsMonotonic = 3,
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

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
// Metric
enum class MetricTag : uint32_t {
  Name = 1,
  Description = 2,
  Unit = 3,
  Gauge = 5,
  Sum = 7,
  Histogram = 9,
  ExponentialHistogram = 10,
  Summary = 11,
  Metadata = 12,
};

template<typename Record>
struct ScopeRecords {
  InstrumentationScope scope;
  std::vector<Record> records;
  std::string schema_url;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto
// ScopeLogs, and the ScopeSpans / ScopeMetrics of the other two signals.
enum class ScopeRecordsTag : uint32_t {
  Scope = 1,
  Records = 2,
  SchemaUrl = 3,
};

template<typename Record>
struct ResourceRecords {
  Resource resource;
  std::vector<ScopeRecords<Record>> scopes;
  std::string schema_url;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto
// ResourceLogs, and the ResourceSpans / ResourceMetrics of the other two.
enum class ResourceRecordsTag : uint32_t {
  Resource = 1,
  ScopeRecords = 2,
  SchemaUrl = 3,
};

template<typename Record>
struct ExportRequest {
  std::vector<ResourceRecords<Record>> resources;
  ValueArena arena;
};

// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/collector/logs/v1/logs_service.proto
// ExportLogsServiceRequest, and the trace / metrics service requests.
enum class ExportRequestTag : uint32_t {
  ResourceRecords = 1,
};

// One decoded metrics payload, shared by the five otlp_metrics_* binds it
// fans out into.
struct DecodedMetrics;

using ExportLogsRequest = ExportRequest<LogRecord>;
using ExportTracesRequest = ExportRequest<Span>;
using ExportMetricsRequest = ExportRequest<Metric>;

struct DecodedMetrics {
  ExportMetricsRequest request;
};

}  // namespace sdb::otel
