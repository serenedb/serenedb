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

#include "connector/functions/otel.h"

#include <absl/strings/escaping.h>

#include <array>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>
#include <utility>
#include <vector>

#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "otel/mapper.h"
#include "otel/protobuf.h"
#include "otel/protojson.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

struct Cell {
  std::string_view column;
  duckdb::Value value;
};

using Row = std::vector<Cell>;

duckdb::Value TimestampNs(uint64_t unix_nano) {
  return duckdb::Value::TIMESTAMPNS(
    duckdb::timestamp_ns_t{static_cast<int64_t>(unix_nano)});
}

duckdb::Value NullableText(std::string_view text) {
  if (text.empty()) {
    return duckdb::Value{};
  }
  return duckdb::Value{std::string{text}};
}

duckdb::Value TextList(const std::vector<std::string>& items) {
  duckdb::vector<duckdb::Value> values;
  values.reserve(items.size());
  for (const auto& item : items) {
    values.emplace_back(item);
  }
  return duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(values));
}

duckdb::Value BigintList(const std::vector<uint64_t>& items) {
  duckdb::vector<duckdb::Value> values;
  values.reserve(items.size());
  for (const auto item : items) {
    values.emplace_back(duckdb::Value::BIGINT(static_cast<int64_t>(item)));
  }
  return duckdb::Value::LIST(duckdb::LogicalType::BIGINT, std::move(values));
}

duckdb::Value DoubleList(const std::vector<double>& items) {
  duckdb::vector<duckdb::Value> values;
  values.reserve(items.size());
  for (const auto item : items) {
    values.emplace_back(duckdb::Value::DOUBLE(item));
  }
  return duckdb::Value::LIST(duckdb::LogicalType::DOUBLE, std::move(values));
}

duckdb::Value OptionalDouble(const std::optional<double>& number) {
  if (!number) {
    return duckdb::Value{};
  }
  return duckdb::Value::DOUBLE(*number);
}

duckdb::Value NumberValue(
  const std::variant<std::monostate, int64_t, double>& number) {
  if (const auto* integer = std::get_if<int64_t>(&number)) {
    return duckdb::Value::DOUBLE(static_cast<double>(*integer));
  }
  if (const auto* real = std::get_if<double>(&number)) {
    return duckdb::Value::DOUBLE(*real);
  }
  return duckdb::Value{};
}

struct OtelBindData final : duckdb::TableFunctionData {
  std::string body;
  std::vector<Row> rows;
  irs::containers::FlatHashMap<std::string, size_t> columns;
  duckdb::vector<duckdb::LogicalType> types;
};

struct OtelState final : duckdb::GlobalTableFunctionState {
  size_t pos = 0;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<OtelState>();
  }
};

void BindTarget(duckdb::ClientContext& context, std::string_view table_name,
                OtelBindData& data,
                duckdb::vector<duckdb::LogicalType>& return_types,
                duckdb::vector<duckdb::string>& names) {
  auto& conn_ctx = GetSereneDBContext(context);
  const auto* table = catalog::FindTableEntry(
    &context, conn_ctx.GetDatabaseId(), kOtelSchema, table_name);
  if (table == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", kOtelSchema, ".", table_name,
                            "\" does not exist; create the OpenTelemetry "
                            "schema first"));
  }
  size_t index = 0;
  for (const auto& column : table->GetColumns().Logical()) {
    return_types.push_back(column.Type());
    names.emplace_back(column.Name().GetIdentifierName());
    data.columns.emplace(column.Name().GetIdentifierName(), index);
    data.types.push_back(column.Type());
    ++index;
  }
}

std::string ReadBodyArgument(const duckdb::Value& argument) {
  if (argument.IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("OTLP payload cannot be NULL"));
  }
  return argument.GetValue<std::string>();
}

// `protobuf` payloads are base64 so the binary body survives the SQL literal.
bool ReadProtobufArgument(const duckdb::vector<duckdb::Value>& inputs) {
  if (inputs.size() < 2 || inputs[1].IsNull()) {
    return false;
  }
  const auto encoding = inputs[1].GetValue<std::string>();
  if (encoding == "json") {
    return false;
  }
  if (encoding == "protobuf") {
    return true;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("unknown OTLP encoding [", encoding,
                          "], expected json or protobuf"));
}

std::string DecodeBase64Payload(const std::string& payload) {
  std::string wire;
  if (!absl::Base64Unescape(payload, &wire)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("OTLP payload is not valid base64"));
  }
  return wire;
}

// Resource and scope columns are identical across every signal.
void AppendCommon(Row& row, const otel::Resource& resource,
                  const otel::InstrumentationScope& scope,
                  std::string_view resource_schema_url,
                  std::string_view scope_schema_url) {
  const auto* service =
    otel::FindAttribute(resource.attributes, otel::kServiceNameKey);
  row.push_back({"service_name", service == nullptr
                                   ? duckdb::Value{}
                                   : NullableText(otel::BodyToText(service))});
  row.push_back({"resource_schema_url", NullableText(resource_schema_url)});
  row.push_back({"scope_schema_url", NullableText(scope_schema_url)});
  row.push_back({"scope_name", NullableText(scope.name)});
  row.push_back({"scope_version", NullableText(scope.version)});
  row.push_back({"resource_attributes",
                 duckdb::Value{otel::AttributesToJson(resource.attributes)}});
  row.push_back({"scope_attributes",
                 duckdb::Value{otel::AttributesToJson(scope.attributes)}});
}

void BuildLogRows(const otel::ExportLogsRequest& request,
                  std::vector<Row>& rows) {
  for (const auto& resource_records : request.resources) {
    for (const auto& scope_records : resource_records.scopes) {
      for (const auto& record : scope_records.records) {
        Row row;
        const uint64_t observed = record.observed_time_unix_nano;
        const uint64_t timestamp =
          record.time_unix_nano != 0 ? record.time_unix_nano : observed;
        row.push_back({"timestamp", TimestampNs(timestamp)});
        row.push_back({"observed_timestamp", observed == 0
                                               ? duckdb::Value{}
                                               : TimestampNs(observed)});
        row.push_back({"trace_id", NullableText(record.trace_id.hex)});
        row.push_back({"span_id", NullableText(record.span_id.hex)});
        row.push_back({"trace_flags", duckdb::Value::INTEGER(
                                        static_cast<int32_t>(record.flags))});
        row.push_back({"severity_text", NullableText(record.severity_text)});
        row.push_back(
          {"severity_number",
           record.severity_number == otel::SeverityNumber::Unspecified
             ? duckdb::Value{}
             : duckdb::Value::SMALLINT(static_cast<int16_t>(
                 std::to_underlying(record.severity_number)))});
        std::string event_name{record.event_name};
        if (event_name.empty()) {
          if (const auto* attribute =
                otel::FindAttribute(record.attributes, otel::kEventNameKey)) {
            event_name = otel::BodyToText(attribute);
          }
        }
        row.push_back({"event_name", NullableText(event_name)});
        row.push_back({"body", NullableText(otel::BodyToText(record.body))});
        AppendCommon(row, resource_records.resource, scope_records.scope,
                     resource_records.schema_url, scope_records.schema_url);
        row.push_back({"log_attributes", duckdb::Value{otel::AttributesToJson(
                                           record.attributes)}});
        rows.push_back(std::move(row));
      }
    }
  }
}

void BuildSpanRows(const otel::ExportTracesRequest& request,
                   std::vector<Row>& rows) {
  for (const auto& resource_records : request.resources) {
    for (const auto& scope_records : resource_records.scopes) {
      for (const auto& span : scope_records.records) {
        Row row;
        row.push_back({"timestamp", TimestampNs(span.start_time_unix_nano)});
        row.push_back(
          {"end_timestamp", span.end_time_unix_nano == 0
                              ? duckdb::Value{}
                              : TimestampNs(span.end_time_unix_nano)});
        row.push_back({"trace_id", NullableText(span.trace_id.hex)});
        row.push_back({"span_id", NullableText(span.span_id.hex)});
        row.push_back(
          {"parent_span_id", NullableText(span.parent_span_id.hex)});
        row.push_back({"trace_state", NullableText(span.trace_state)});
        row.push_back({"span_name", NullableText(span.name)});
        row.push_back({"span_kind", duckdb::Value{std::string{
                                      otel::SpanKindName(span.kind)}}});
        const bool has_duration =
          span.end_time_unix_nano >= span.start_time_unix_nano &&
          span.end_time_unix_nano != 0;
        row.push_back(
          {"duration_ns",
           has_duration
             ? duckdb::Value::BIGINT(static_cast<int64_t>(
                 span.end_time_unix_nano - span.start_time_unix_nano))
             : duckdb::Value{}});
        row.push_back(
          {"status_code",
           duckdb::Value{std::string{otel::StatusCodeName(span.status.code)}}});
        row.push_back({"status_message", NullableText(span.status.message)});
        AppendCommon(row, resource_records.resource, scope_records.scope,
                     resource_records.schema_url, scope_records.schema_url);
        row.push_back({"span_attributes",
                       duckdb::Value{otel::AttributesToJson(span.attributes)}});
        row.push_back(
          {"events", duckdb::Value{otel::EventsToJson(span.events)}});
        row.push_back({"links", duckdb::Value{otel::LinksToJson(span.links)}});
        std::vector<std::string> event_names;
        event_names.reserve(span.events.size());
        for (const auto& event : span.events) {
          event_names.emplace_back(event.name);
        }
        std::vector<std::string> link_trace_ids;
        link_trace_ids.reserve(span.links.size());
        for (const auto& link : span.links) {
          link_trace_ids.push_back(link.trace_id.hex);
        }
        row.push_back({"event_names", TextList(event_names)});
        row.push_back({"link_trace_ids", TextList(link_trace_ids)});
        rows.push_back(std::move(row));
      }
    }
  }
}

template<typename Point>
void AppendMetricCommon(Row& row, const otel::Metric& metric,
                        const Point& point, const otel::Resource& resource,
                        const otel::InstrumentationScope& scope,
                        std::string_view resource_schema_url,
                        std::string_view scope_schema_url) {
  row.push_back({"timestamp", TimestampNs(point.time_unix_nano)});
  row.push_back(
    {"start_timestamp", point.start_time_unix_nano == 0
                          ? duckdb::Value{}
                          : TimestampNs(point.start_time_unix_nano)});
  row.push_back({"metric_name", duckdb::Value{metric.name}});
  row.push_back({"metric_description", NullableText(metric.description)});
  row.push_back({"metric_unit", NullableText(metric.unit)});
  AppendCommon(row, resource, scope, resource_schema_url, scope_schema_url);
  row.push_back(
    {"attributes", duckdb::Value{otel::AttributesToJson(point.attributes)}});
  row.push_back(
    {"flags", duckdb::Value::INTEGER(static_cast<int32_t>(point.flags))});
}

void AppendTemporality(Row& row, otel::AggregationTemporality temporality) {
  row.push_back(
    {"aggregation_temporality",
     duckdb::Value{std::string{otel::TemporalityName(temporality)}}});
}

void AppendExemplars(Row& row, const std::vector<otel::Exemplar>& exemplars) {
  row.push_back({"exemplars", duckdb::Value{otel::ExemplarsToJson(exemplars)}});
}

void AppendCountAndSum(Row& row, uint64_t count, duckdb::Value sum) {
  row.push_back({"count", duckdb::Value::BIGINT(static_cast<int64_t>(count))});
  row.push_back({"sum", std::move(sum)});
}

void AppendMinAndMax(Row& row, const std::optional<double>& min,
                     const std::optional<double>& max) {
  row.push_back({"min", OptionalDouble(min)});
  row.push_back({"max", OptionalDouble(max)});
}

struct GaugeShape {
  using Data = otel::Gauge;

  static void Append(Row& row, const Data&,
                     const otel::NumberDataPoint& point) {
    row.push_back({"value", NumberValue(point.value)});
    AppendExemplars(row, point.exemplars);
  }
};

struct SumShape {
  using Data = otel::Sum;

  static void Append(Row& row, const Data& sum,
                     const otel::NumberDataPoint& point) {
    row.push_back({"value", NumberValue(point.value)});
    AppendTemporality(row, sum.aggregation_temporality);
    row.push_back({"is_monotonic", duckdb::Value::BOOLEAN(sum.is_monotonic)});
    AppendExemplars(row, point.exemplars);
  }
};

struct HistogramShape {
  using Data = otel::Histogram;

  static void Append(Row& row, const Data& histogram,
                     const otel::HistogramDataPoint& point) {
    AppendCountAndSum(row, point.count, OptionalDouble(point.sum));
    row.push_back({"bucket_counts", BigintList(point.bucket_counts)});
    row.push_back({"explicit_bounds", DoubleList(point.explicit_bounds)});
    AppendMinAndMax(row, point.min, point.max);
    AppendTemporality(row, histogram.aggregation_temporality);
    AppendExemplars(row, point.exemplars);
  }
};

struct ExponentialHistogramShape {
  using Data = otel::ExponentialHistogram;

  static void Append(Row& row, const Data& exponential,
                     const otel::ExponentialHistogramDataPoint& point) {
    AppendCountAndSum(row, point.count, OptionalDouble(point.sum));
    row.push_back({"scale", duckdb::Value::INTEGER(point.scale)});
    row.push_back({"zero_count", duckdb::Value::BIGINT(
                                   static_cast<int64_t>(point.zero_count))});
    row.push_back(
      {"positive_offset", duckdb::Value::INTEGER(point.positive.offset)});
    row.push_back(
      {"positive_bucket_counts", BigintList(point.positive.bucket_counts)});
    row.push_back(
      {"negative_offset", duckdb::Value::INTEGER(point.negative.offset)});
    row.push_back(
      {"negative_bucket_counts", BigintList(point.negative.bucket_counts)});
    AppendMinAndMax(row, point.min, point.max);
    AppendTemporality(row, exponential.aggregation_temporality);
    AppendExemplars(row, point.exemplars);
  }
};

struct SummaryShape {
  using Data = otel::Summary;

  static void Append(Row& row, const Data&,
                     const otel::SummaryDataPoint& point) {
    AppendCountAndSum(row, point.count, duckdb::Value::DOUBLE(point.sum));
    std::vector<double> quantiles;
    std::vector<double> values;
    quantiles.reserve(point.quantile_values.size());
    values.reserve(point.quantile_values.size());
    for (const auto& quantile : point.quantile_values) {
      quantiles.push_back(quantile.quantile);
      values.push_back(quantile.value);
    }
    row.push_back({"quantiles", DoubleList(quantiles)});
    row.push_back({"values", DoubleList(values)});
  }
};

template<typename MetricShape>
void BuildMetricRows(const otel::ExportMetricsRequest& request,
                     std::vector<Row>& rows) {
  for (const auto& resource_records : request.resources) {
    const auto& resource = resource_records.resource;
    for (const auto& scope_records : resource_records.scopes) {
      const auto& scope = scope_records.scope;
      for (const auto& metric : scope_records.records) {
        const auto* data =
          std::get_if<typename MetricShape::Data>(&metric.data);
        if (data == nullptr) {
          continue;
        }
        for (const auto& point : data->data_points) {
          Row row;
          AppendMetricCommon(row, metric, point, resource, scope,
                             resource_records.schema_url,
                             scope_records.schema_url);
          MetricShape::Append(row, *data, point);
          rows.push_back(std::move(row));
        }
      }
    }
  }
}

void Emit(duckdb::DataChunk& output, const OtelBindData& data,
          const std::vector<Row>& rows, size_t& pos) {
  duckdb::idx_t emitted = 0;
  while (emitted < STANDARD_VECTOR_SIZE && pos < rows.size()) {
    for (duckdb::idx_t column = 0; column < output.ColumnCount(); ++column) {
      output.SetValue(column, emitted, duckdb::Value{});
    }
    for (const auto& cell : rows[pos]) {
      const auto it = data.columns.find(std::string{cell.column});
      if (it == data.columns.end()) {
        continue;
      }
      const auto& type = data.types[it->second];
      if (cell.value.IsNull()) {
        continue;
      }
      output.SetValue(it->second, emitted, cell.value.DefaultCastAs(type));
    }
    ++pos;
    ++emitted;
  }
  output.SetCardinality(emitted);
}

template<const std::string_view& Table, auto Build>
duckdb::unique_ptr<duckdb::FunctionData> OtelBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = duckdb::make_uniq<OtelBindData>();
  BindTarget(context, Table, *data, return_types, names);
  data->body = ReadBodyArgument(input.inputs[0]);
  Build(context, data->body, ReadProtobufArgument(input.inputs), data->rows);
  return data;
}

void OtelExecute(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
                 duckdb::DataChunk& output) {
  const auto& data = input.bind_data->Cast<OtelBindData>();
  Emit(output, data, data.rows, input.global_state->Cast<OtelState>().pos);
}

// otel_source_logs(): the rows of the logs request the HTTP handler left on
// the connection. Bind carries no data -- only the target's columns -- so the
// statement can be prepared once and re-executed for every request. The scan
// writes the decoded model straight into the output vectors; a schema whose
// column types differ from the shipped DDL takes the generic row path instead.
enum class LogColumn : uint8_t {
  Timestamp,
  ObservedTimestamp,
  TraceId,
  SpanId,
  TraceFlags,
  SeverityText,
  SeverityNumber,
  ServiceName,
  EventName,
  Body,
  ResourceSchemaUrl,
  ScopeSchemaUrl,
  ScopeName,
  ScopeVersion,
  ResourceAttributes,
  ScopeAttributes,
  LogAttributes,
};

struct LogColumnSpec {
  std::string_view name;
  duckdb::LogicalTypeId type;
};

// resources/otel/otel_schema.sql, otel_logs; JSON is VARCHAR underneath.
constexpr std::array<LogColumnSpec, 17> kLogColumns{{
  {"timestamp", duckdb::LogicalTypeId::TIMESTAMP_NS},
  {"observed_timestamp", duckdb::LogicalTypeId::TIMESTAMP_NS},
  {"trace_id", duckdb::LogicalTypeId::VARCHAR},
  {"span_id", duckdb::LogicalTypeId::VARCHAR},
  {"trace_flags", duckdb::LogicalTypeId::INTEGER},
  {"severity_text", duckdb::LogicalTypeId::VARCHAR},
  {"severity_number", duckdb::LogicalTypeId::SMALLINT},
  {"service_name", duckdb::LogicalTypeId::VARCHAR},
  {"event_name", duckdb::LogicalTypeId::VARCHAR},
  {"body", duckdb::LogicalTypeId::VARCHAR},
  {"resource_schema_url", duckdb::LogicalTypeId::VARCHAR},
  {"scope_schema_url", duckdb::LogicalTypeId::VARCHAR},
  {"scope_name", duckdb::LogicalTypeId::VARCHAR},
  {"scope_version", duckdb::LogicalTypeId::VARCHAR},
  {"resource_attributes", duckdb::LogicalTypeId::VARCHAR},
  {"scope_attributes", duckdb::LogicalTypeId::VARCHAR},
  {"log_attributes", duckdb::LogicalTypeId::VARCHAR},
}};

inline constexpr duckdb::idx_t kAbsent = duckdb::DConstants::INVALID_INDEX;

struct OtelSourceBindData final : duckdb::TableFunctionData {
  OtelBindData generic;
  std::array<duckdb::idx_t, kLogColumns.size()> slots;
  bool direct = true;
};

// The columns shared by every record of one scope, computed once per scope.
struct ScopeColumns {
  std::string service_name;
  bool has_service = false;
  std::string resource_attributes;
  std::string scope_attributes;
};

struct OtelSourceState final : duckdb::GlobalTableFunctionState {
  const otel::ExportLogsRequest* request = nullptr;
  size_t resource = 0;
  size_t scope = 0;
  size_t record = 0;
  bool scope_ready = false;
  ScopeColumns columns;
  std::vector<Row> rows;
  size_t pos = 0;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
    auto state = duckdb::make_uniq<OtelSourceState>();
    const auto* logs = GetSereneDBContext(context).GetOtelLogs();
    if (logs == nullptr) {
      return state;
    }
    if (input.bind_data->Cast<OtelSourceBindData>().direct) {
      state->request = &logs->request;
    } else {
      BuildLogRows(logs->request, state->rows);
    }
    return state;
  }

  // The next record, or nullptr when the request is exhausted.
  const otel::LogRecord* Next() {
    while (request != nullptr && resource < request->resources.size()) {
      const auto& scopes = request->resources[resource].scopes;
      if (scope >= scopes.size()) {
        ++resource;
        scope = 0;
        continue;
      }
      if (record >= scopes[scope].records.size()) {
        ++scope;
        record = 0;
        scope_ready = false;
        continue;
      }
      if (!scope_ready) {
        const auto& res = request->resources[resource].resource;
        const auto* service =
          otel::FindAttribute(res.attributes, otel::kServiceNameKey);
        columns.has_service = service != nullptr;
        columns.service_name =
          service == nullptr ? std::string{} : otel::BodyToText(service);
        columns.resource_attributes = otel::AttributesToJson(res.attributes);
        columns.scope_attributes =
          otel::AttributesToJson(scopes[scope].scope.attributes);
        scope_ready = true;
      }
      return &scopes[scope].records[record++];
    }
    return nullptr;
  }
};

duckdb::unique_ptr<duckdb::FunctionData> OtelSourceBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = duckdb::make_uniq<OtelSourceBindData>();
  BindTarget(context, kOtelLogsTable, data->generic, return_types, names);
  for (size_t i = 0; i < kLogColumns.size(); ++i) {
    const auto it =
      data->generic.columns.find(std::string{kLogColumns[i].name});
    if (it == data->generic.columns.end()) {
      data->slots[i] = kAbsent;
      continue;
    }
    data->slots[i] = it->second;
    if (return_types[it->second].id() != kLogColumns[i].type) {
      data->direct = false;
    }
  }
  return data;
}

class LogRowWriter {
 public:
  LogRowWriter(duckdb::DataChunk& output, const OtelSourceBindData& data)
    : _output{output}, _data{data} {}

  void Null(LogColumn column, duckdb::idx_t row) {
    if (auto* vector = Slot(column)) {
      duckdb::FlatVector::SetNull(*vector, row, true);
    }
  }

  // Empty text is NULL, as NullableText maps it.
  void Text(LogColumn column, duckdb::idx_t row, std::string_view text) {
    if (text.empty()) {
      Null(column, row);
    } else {
      Json(column, row, text);
    }
  }

  void Json(LogColumn column, duckdb::idx_t row, std::string_view text) {
    if (auto* vector = Slot(column)) {
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(*vector)[row] =
        duckdb::StringVector::AddString(*vector, text.data(), text.size());
    }
  }

  template<typename T>
  void Number(LogColumn column, duckdb::idx_t row, T value) {
    if (auto* vector = Slot(column)) {
      duckdb::FlatVector::GetDataMutable<T>(*vector)[row] = value;
    }
  }

  void Timestamp(LogColumn column, duckdb::idx_t row, uint64_t unix_nano) {
    Number(column, row, static_cast<int64_t>(unix_nano));
  }

 private:
  duckdb::Vector* Slot(LogColumn column) {
    const auto slot = _data.slots[std::to_underlying(column)];
    return slot == kAbsent ? nullptr : &_output.data[slot];
  }

  duckdb::DataChunk& _output;
  const OtelSourceBindData& _data;
};

void WriteLogRow(LogRowWriter& out, duckdb::idx_t row,
                 const otel::LogRecord& record, const ScopeColumns& shared,
                 const otel::ScopeRecords<otel::LogRecord>& scope,
                 std::string_view resource_schema_url) {
  const uint64_t observed = record.observed_time_unix_nano;
  out.Timestamp(LogColumn::Timestamp, row,
                record.time_unix_nano != 0 ? record.time_unix_nano : observed);
  if (observed == 0) {
    out.Null(LogColumn::ObservedTimestamp, row);
  } else {
    out.Timestamp(LogColumn::ObservedTimestamp, row, observed);
  }
  out.Text(LogColumn::TraceId, row, record.trace_id.hex);
  out.Text(LogColumn::SpanId, row, record.span_id.hex);
  out.Number(LogColumn::TraceFlags, row, static_cast<int32_t>(record.flags));
  out.Text(LogColumn::SeverityText, row, record.severity_text);
  if (record.severity_number == otel::SeverityNumber::Unspecified) {
    out.Null(LogColumn::SeverityNumber, row);
  } else {
    out.Number(
      LogColumn::SeverityNumber, row,
      static_cast<int16_t>(std::to_underlying(record.severity_number)));
  }
  if (!record.event_name.empty()) {
    out.Text(LogColumn::EventName, row, record.event_name);
  } else if (const auto* attribute =
               otel::FindAttribute(record.attributes, otel::kEventNameKey)) {
    out.Text(LogColumn::EventName, row, otel::BodyToText(attribute));
  } else {
    out.Null(LogColumn::EventName, row);
  }
  out.Text(LogColumn::Body, row, otel::BodyToText(record.body));
  if (shared.has_service) {
    out.Text(LogColumn::ServiceName, row, shared.service_name);
  } else {
    out.Null(LogColumn::ServiceName, row);
  }
  out.Text(LogColumn::ResourceSchemaUrl, row, resource_schema_url);
  out.Text(LogColumn::ScopeSchemaUrl, row, scope.schema_url);
  out.Text(LogColumn::ScopeName, row, scope.scope.name);
  out.Text(LogColumn::ScopeVersion, row, scope.scope.version);
  out.Json(LogColumn::ResourceAttributes, row, shared.resource_attributes);
  out.Json(LogColumn::ScopeAttributes, row, shared.scope_attributes);
  out.Json(LogColumn::LogAttributes, row,
           otel::AttributesToJson(record.attributes));
}

void OtelSourceExecute(duckdb::ClientContext&,
                       duckdb::TableFunctionInput& input,
                       duckdb::DataChunk& output) {
  const auto& data = input.bind_data->Cast<OtelSourceBindData>();
  auto& state = input.global_state->Cast<OtelSourceState>();
  if (!data.direct) {
    Emit(output, data.generic, state.rows, state.pos);
    return;
  }
  LogRowWriter out{output, data};
  duckdb::idx_t row = 0;
  while (row < STANDARD_VECTOR_SIZE) {
    const auto* record = state.Next();
    if (record == nullptr) {
      break;
    }
    const auto& resource = state.request->resources[state.resource];
    WriteLogRow(out, row, *record, state.columns, resource.scopes[state.scope],
                resource.schema_url);
    ++row;
  }
  // Columns the mapping does not produce (a deployment's own additions).
  std::array<bool, 256> produced{};
  for (const auto slot : data.slots) {
    if (slot != kAbsent && slot < produced.size()) {
      produced[slot] = true;
    }
  }
  for (duckdb::idx_t column = 0; column < output.ColumnCount(); ++column) {
    if (column >= produced.size() || !produced[column]) {
      duckdb::FlatVector::ValidityMutable(output.data[column])
        .SetAllInvalid(row);
    }
  }
  output.SetCardinality(row);
}

void BuildLogs(duckdb::ClientContext&, const std::string& body, bool protobuf,
               std::vector<Row>& rows) {
  otel::ExportLogsRequest request;
  std::string wire;
  if (protobuf) {
    wire = DecodeBase64Payload(body);
    otel::DecodeLogsRequest(wire, request);
  } else {
    otel::ParseLogsRequest(body, request);
  }
  BuildLogRows(request, rows);
}

void BuildTraces(duckdb::ClientContext&, const std::string& body, bool protobuf,
                 std::vector<Row>& rows) {
  otel::ExportTracesRequest request;
  std::string wire;
  if (protobuf) {
    wire = DecodeBase64Payload(body);
    otel::DecodeTracesRequest(wire, request);
  } else {
    otel::ParseTracesRequest(body, request);
  }
  BuildSpanRows(request, rows);
}

template<typename MetricShape>
void BuildMetrics(duckdb::ClientContext& context, const std::string& body,
                  bool protobuf, std::vector<Row>& rows) {
  if (const auto* decoded = GetSereneDBContext(context).GetOtelMetrics()) {
    BuildMetricRows<MetricShape>(decoded->request, rows);
    return;
  }
  otel::ExportMetricsRequest request;
  std::string wire;
  if (protobuf) {
    wire = DecodeBase64Payload(body);
    otel::DecodeMetricsRequest(wire, request);
  } else {
    otel::ParseMetricsRequest(body, request);
  }
  BuildMetricRows<MetricShape>(request, rows);
}

constexpr std::string_view kLogsTable = kOtelLogsTable;
constexpr std::string_view kTracesTable = kOtelTracesTable;
constexpr std::string_view kGaugeTable = kOtelMetricTables[0];
constexpr std::string_view kSumTable = kOtelMetricTables[1];
constexpr std::string_view kHistogramTable = kOtelMetricTables[2];
constexpr std::string_view kExponentialTable = kOtelMetricTables[3];
constexpr std::string_view kSummaryTable = kOtelMetricTables[4];

}  // namespace

void RegisterOtelFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  const auto add = [&](const char* name, auto bind) {
    loader.RegisterFunction(
      duckdb::TableFunction{name,
                            {duckdb::LogicalType::VARCHAR},
                            OtelExecute,
                            bind,
                            OtelState::Init});
    loader.RegisterFunction(duckdb::TableFunction{
      name,
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      OtelExecute,
      bind,
      OtelState::Init});
  };

  add("otel_parse_logs", OtelBind<kLogsTable, BuildLogs>);
  add("otel_parse_traces", OtelBind<kTracesTable, BuildTraces>);
  add("otel_parse_metrics_gauge",
      OtelBind<kGaugeTable, BuildMetrics<GaugeShape>>);
  add("otel_parse_metrics_sum", OtelBind<kSumTable, BuildMetrics<SumShape>>);
  add("otel_parse_metrics_histogram",
      OtelBind<kHistogramTable, BuildMetrics<HistogramShape>>);
  add("otel_parse_metrics_exponential_histogram",
      OtelBind<kExponentialTable, BuildMetrics<ExponentialHistogramShape>>);
  add("otel_parse_metrics_summary",
      OtelBind<kSummaryTable, BuildMetrics<SummaryShape>>);

  loader.RegisterFunction(duckdb::TableFunction{kOtelSourceLogsFunction,
                                                {},
                                                OtelSourceExecute,
                                                OtelSourceBind,
                                                OtelSourceState::Init});
}

}  // namespace sdb::connector
