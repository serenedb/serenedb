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

#include <algorithm>
#include <array>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
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

// otel_source_<signal>(): the rows of the request the HTTP handler left on the
// connection. Bind carries no data -- only the target's columns -- so each
// INSERT ... SELECT * FROM otel_source_*() is prepared once per connection and
// re-executed per request. The scan writes the decoded model straight into
// the output vectors; a schema whose column types differ from the shipped DDL
// takes the generic row path instead.

struct ColumnSpec {
  std::string_view name;
  duckdb::LogicalTypeId type;
  // For a LIST column, its element type.
  duckdb::LogicalTypeId child = duckdb::LogicalTypeId::INVALID;
};

inline constexpr duckdb::idx_t kAbsent = duckdb::DConstants::INVALID_INDEX;

struct SourceBindData final : duckdb::TableFunctionData {
  OtelBindData generic;
  // Output column of each spec entry, or kAbsent.
  std::vector<duckdb::idx_t> slots;
  bool direct = true;
};

// Writes one output row. A chunk starts all-NULL (PrepareChunk), so a column
// nothing writes -- absent from the spec, or a deployment's own addition --
// stays NULL.
class Out {
 public:
  Out(duckdb::DataChunk& output, const SourceBindData& data)
    : _output{output}, _slots{data.slots} {}

  void Row(duckdb::idx_t row) { _row = row; }

  // Empty text is NULL, as NullableText maps it.
  template<typename Column>
  void Text(Column column, std::string_view text) {
    if (!text.empty()) {
      String(column, text);
    }
  }

  template<typename Column>
  void String(Column column, std::string_view text) {
    if (auto* vector = Slot(column)) {
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(*vector)[_row] =
        duckdb::StringVector::AddString(*vector, text.data(), text.size());
      Valid(*vector);
    }
  }

  template<typename T, typename Column>
  void Number(Column column, T value) {
    if (auto* vector = Slot(column)) {
      duckdb::FlatVector::GetDataMutable<T>(*vector)[_row] = value;
      Valid(*vector);
    }
  }

  template<typename Column>
  void Timestamp(Column column, uint64_t unix_nano) {
    Number(column, static_cast<int64_t>(unix_nano));
  }

  // Zero is "unset" for OTLP timestamps.
  template<typename Column>
  void OptionalTimestamp(Column column, uint64_t unix_nano) {
    if (unix_nano != 0) {
      Timestamp(column, unix_nano);
    }
  }

  template<typename Column>
  void Double(Column column, const std::optional<double>& value) {
    if (value) {
      Number(column, *value);
    }
  }

  template<typename Column>
  void Bigint(Column column, uint64_t value) {
    Number(column, static_cast<int64_t>(value));
  }

  template<typename Column, typename Range, typename Put>
  void List(Column column, const Range& items, Put put) {
    auto* vector = Slot(column);
    if (vector == nullptr) {
      return;
    }
    const auto offset = duckdb::ListVector::GetListSize(*vector);
    const auto count = static_cast<duckdb::idx_t>(std::size(items));
    duckdb::ListVector::Reserve(*vector, offset + count);
    auto& child = duckdb::ListVector::GetChildMutable(*vector);
    auto index = offset;
    for (const auto& item : items) {
      put(child, index++, item);
    }
    duckdb::ListVector::SetListSize(*vector, offset + count);
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(*vector)[_row] = {
      offset, count};
    Valid(*vector);
  }

  template<typename Column>
  void Bigints(Column column, const std::vector<uint64_t>& items) {
    List(column, items, [](duckdb::Vector& child, duckdb::idx_t i, uint64_t v) {
      duckdb::FlatVector::GetDataMutable<int64_t>(child)[i] =
        static_cast<int64_t>(v);
    });
  }

  template<typename Column>
  void Doubles(Column column, const std::vector<double>& items) {
    List(column, items, [](duckdb::Vector& child, duckdb::idx_t i, double v) {
      duckdb::FlatVector::GetDataMutable<double>(child)[i] = v;
    });
  }

  template<typename Column, typename Range, typename Project>
  void Texts(Column column, const Range& items, Project project) {
    List(column, items,
         [&](duckdb::Vector& child, duckdb::idx_t i, const auto& item) {
           const std::string_view text = project(item);
           duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child)[i] =
             duckdb::StringVector::AddString(child, text.data(), text.size());
         });
  }

 private:
  template<typename Column>
  duckdb::Vector* Slot(Column column) {
    const auto slot = _slots[std::to_underlying(column)];
    return slot == kAbsent ? nullptr : &_output.data[slot];
  }

  void Valid(duckdb::Vector& vector) {
    duckdb::FlatVector::ValidityMutable(vector).SetValid(_row);
  }

  duckdb::DataChunk& _output;
  const std::vector<duckdb::idx_t>& _slots;
  duckdb::idx_t _row = 0;
};

void PrepareChunk(duckdb::DataChunk& output) {
  for (auto& vector : output.data) {
    duckdb::FlatVector::ValidityMutable(vector).SetAllInvalid(
      STANDARD_VECTOR_SIZE);
    if (vector.GetType().id() == duckdb::LogicalTypeId::LIST) {
      auto* entries =
        duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(vector);
      std::fill_n(entries, STANDARD_VECTOR_SIZE, duckdb::list_entry_t{0, 0});
    }
  }
}

bool Matches(const duckdb::LogicalType& type, const ColumnSpec& spec) {
  if (type.id() != spec.type) {
    return false;
  }
  return spec.child == duckdb::LogicalTypeId::INVALID ||
         duckdb::ListType::GetChildType(type).id() == spec.child;
}

template<size_t N>
duckdb::unique_ptr<SourceBindData> BindSource(
  duckdb::ClientContext& context, std::string_view table,
  const std::array<ColumnSpec, N>& specs,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = duckdb::make_uniq<SourceBindData>();
  BindTarget(context, table, data->generic, return_types, names);
  data->slots.assign(N, kAbsent);
  for (size_t i = 0; i < N; ++i) {
    const auto it = data->generic.columns.find(std::string{specs[i].name});
    if (it == data->generic.columns.end()) {
      continue;
    }
    data->slots[i] = it->second;
    if (!Matches(return_types[it->second], specs[i])) {
      data->direct = false;
    }
  }
  return data;
}

// The columns every record of one scope shares, computed once per scope.
struct ScopeColumns {
  std::string service_name;
  bool has_service = false;
  std::string resource_attributes;
  std::string scope_attributes;
};

// Walks resources -> scopes -> records.
template<typename Record>
class RecordCursor {
 public:
  explicit RecordCursor(const otel::ExportRequest<Record>* request)
    : _request{request} {}

  const Record* Next() {
    while (_request != nullptr && _resource < _request->resources.size()) {
      const auto& scopes = _request->resources[_resource].scopes;
      if (_scope >= scopes.size()) {
        ++_resource;
        _scope = 0;
        continue;
      }
      if (_record >= scopes[_scope].records.size()) {
        ++_scope;
        _record = 0;
        _ready = false;
        continue;
      }
      if (!_ready) {
        const auto& resource = Resources().resource;
        const auto* service =
          otel::FindAttribute(resource.attributes, otel::kServiceNameKey);
        _shared.has_service = service != nullptr;
        _shared.service_name =
          service == nullptr ? std::string{} : otel::BodyToText(service);
        _shared.resource_attributes =
          otel::AttributesToJson(resource.attributes);
        _shared.scope_attributes =
          otel::AttributesToJson(Scopes().scope.attributes);
        _ready = true;
      }
      return &scopes[_scope].records[_record++];
    }
    return nullptr;
  }

  const otel::ResourceRecords<Record>& Resources() const {
    return _request->resources[_resource];
  }
  const otel::ScopeRecords<Record>& Scopes() const {
    return Resources().scopes[_scope];
  }
  const ScopeColumns& Shared() const { return _shared; }

 private:
  const otel::ExportRequest<Record>* _request;
  size_t _resource = 0;
  size_t _scope = 0;
  size_t _record = 0;
  bool _ready = false;
  ScopeColumns _shared;
};

// Every table's resource and scope columns, under the same enumerator names.
template<typename Column, typename Record>
void WriteScope(Out& out, const RecordCursor<Record>& cursor) {
  const auto& shared = cursor.Shared();
  if (shared.has_service) {
    out.Text(Column::ServiceName, shared.service_name);
  }
  out.Text(Column::ResourceSchemaUrl, cursor.Resources().schema_url);
  out.Text(Column::ScopeSchemaUrl, cursor.Scopes().schema_url);
  out.Text(Column::ScopeName, cursor.Scopes().scope.name);
  out.Text(Column::ScopeVersion, cursor.Scopes().scope.version);
  out.String(Column::ResourceAttributes, shared.resource_attributes);
  out.String(Column::ScopeAttributes, shared.scope_attributes);
}

// --- logs --------------------------------------------------------------------

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

// resources/otel/otel_schema.sql; JSON is VARCHAR underneath.
constexpr std::array<ColumnSpec, 17> kLogColumns{{
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

struct LogsSource {
  using Record = otel::LogRecord;
  using Cursor = RecordCursor<Record>;
  static constexpr std::string_view kTable = kOtelLogsTable;
  static constexpr const auto& kColumns = kLogColumns;

  static const otel::ExportLogsRequest* Request(ConnectionContext& ctx) {
    const auto* logs = ctx.GetOtelLogs();
    return logs == nullptr ? nullptr : &logs->request;
  }

  static void BuildRows(const otel::ExportLogsRequest& request,
                        std::vector<Row>& rows) {
    BuildLogRows(request, rows);
  }

  static bool WriteNext(Cursor& cursor, Out& out) {
    using C = LogColumn;
    const auto* record = cursor.Next();
    if (record == nullptr) {
      return false;
    }
    const uint64_t observed = record->observed_time_unix_nano;
    out.Timestamp(C::Timestamp, record->time_unix_nano != 0
                                  ? record->time_unix_nano
                                  : observed);
    out.OptionalTimestamp(C::ObservedTimestamp, observed);
    out.Text(C::TraceId, record->trace_id.hex);
    out.Text(C::SpanId, record->span_id.hex);
    out.Number(C::TraceFlags, static_cast<int32_t>(record->flags));
    out.Text(C::SeverityText, record->severity_text);
    if (record->severity_number != otel::SeverityNumber::Unspecified) {
      out.Number(
        C::SeverityNumber,
        static_cast<int16_t>(std::to_underlying(record->severity_number)));
    }
    if (!record->event_name.empty()) {
      out.Text(C::EventName, record->event_name);
    } else if (const auto* attribute =
                 otel::FindAttribute(record->attributes, otel::kEventNameKey)) {
      out.Text(C::EventName, otel::BodyToText(attribute));
    }
    out.Text(C::Body, otel::BodyToText(record->body));
    WriteScope<C>(out, cursor);
    out.String(C::LogAttributes, otel::AttributesToJson(record->attributes));
    return true;
  }
};

// --- traces ------------------------------------------------------------------

enum class SpanColumn : uint8_t {
  Timestamp,
  EndTimestamp,
  TraceId,
  SpanId,
  ParentSpanId,
  TraceState,
  SpanName,
  SpanKind,
  ServiceName,
  DurationNs,
  StatusCode,
  StatusMessage,
  ResourceSchemaUrl,
  ScopeSchemaUrl,
  ScopeName,
  ScopeVersion,
  ResourceAttributes,
  ScopeAttributes,
  SpanAttributes,
  Events,
  Links,
  EventNames,
  LinkTraceIds,
};

constexpr std::array<ColumnSpec, 23> kSpanColumns{{
  {"timestamp", duckdb::LogicalTypeId::TIMESTAMP_NS},
  {"end_timestamp", duckdb::LogicalTypeId::TIMESTAMP_NS},
  {"trace_id", duckdb::LogicalTypeId::VARCHAR},
  {"span_id", duckdb::LogicalTypeId::VARCHAR},
  {"parent_span_id", duckdb::LogicalTypeId::VARCHAR},
  {"trace_state", duckdb::LogicalTypeId::VARCHAR},
  {"span_name", duckdb::LogicalTypeId::VARCHAR},
  {"span_kind", duckdb::LogicalTypeId::VARCHAR},
  {"service_name", duckdb::LogicalTypeId::VARCHAR},
  {"duration_ns", duckdb::LogicalTypeId::BIGINT},
  {"status_code", duckdb::LogicalTypeId::VARCHAR},
  {"status_message", duckdb::LogicalTypeId::VARCHAR},
  {"resource_schema_url", duckdb::LogicalTypeId::VARCHAR},
  {"scope_schema_url", duckdb::LogicalTypeId::VARCHAR},
  {"scope_name", duckdb::LogicalTypeId::VARCHAR},
  {"scope_version", duckdb::LogicalTypeId::VARCHAR},
  {"resource_attributes", duckdb::LogicalTypeId::VARCHAR},
  {"scope_attributes", duckdb::LogicalTypeId::VARCHAR},
  {"span_attributes", duckdb::LogicalTypeId::VARCHAR},
  {"events", duckdb::LogicalTypeId::VARCHAR},
  {"links", duckdb::LogicalTypeId::VARCHAR},
  {"event_names", duckdb::LogicalTypeId::LIST, duckdb::LogicalTypeId::VARCHAR},
  {"link_trace_ids", duckdb::LogicalTypeId::LIST,
   duckdb::LogicalTypeId::VARCHAR},
}};

struct TracesSource {
  using Record = otel::Span;
  using Cursor = RecordCursor<Record>;
  static constexpr std::string_view kTable = kOtelTracesTable;
  static constexpr const auto& kColumns = kSpanColumns;

  static const otel::ExportTracesRequest* Request(ConnectionContext& ctx) {
    const auto* traces = ctx.GetOtelTraces();
    return traces == nullptr ? nullptr : &traces->request;
  }

  static void BuildRows(const otel::ExportTracesRequest& request,
                        std::vector<Row>& rows) {
    BuildSpanRows(request, rows);
  }

  static bool WriteNext(Cursor& cursor, Out& out) {
    using C = SpanColumn;
    const auto* span = cursor.Next();
    if (span == nullptr) {
      return false;
    }
    out.Timestamp(C::Timestamp, span->start_time_unix_nano);
    out.OptionalTimestamp(C::EndTimestamp, span->end_time_unix_nano);
    out.Text(C::TraceId, span->trace_id.hex);
    out.Text(C::SpanId, span->span_id.hex);
    out.Text(C::ParentSpanId, span->parent_span_id.hex);
    out.Text(C::TraceState, span->trace_state);
    out.Text(C::SpanName, span->name);
    out.String(C::SpanKind, otel::SpanKindName(span->kind));
    if (span->end_time_unix_nano >= span->start_time_unix_nano &&
        span->end_time_unix_nano != 0) {
      out.Number(C::DurationNs,
                 static_cast<int64_t>(span->end_time_unix_nano -
                                      span->start_time_unix_nano));
    }
    out.String(C::StatusCode, otel::StatusCodeName(span->status.code));
    out.Text(C::StatusMessage, span->status.message);
    WriteScope<C>(out, cursor);
    out.String(C::SpanAttributes, otel::AttributesToJson(span->attributes));
    out.String(C::Events, otel::EventsToJson(span->events));
    out.String(C::Links, otel::LinksToJson(span->links));
    out.Texts(C::EventNames, span->events,
              [](const otel::SpanEvent& event) { return event.name; });
    out.Texts(C::LinkTraceIds, span->links,
              [](const otel::SpanLink& link) -> std::string_view {
                return link.trace_id.hex;
              });
    return true;
  }
};

// --- metrics -----------------------------------------------------------------

// Every otel_metrics_* column; each table has a subset, and each shape writes
// only its own.
enum class MetricColumn : uint8_t {
  Timestamp,
  StartTimestamp,
  ServiceName,
  MetricName,
  MetricDescription,
  MetricUnit,
  ResourceSchemaUrl,
  ScopeSchemaUrl,
  ScopeName,
  ScopeVersion,
  ResourceAttributes,
  ScopeAttributes,
  Attributes,
  Flags,
  Value,
  Exemplars,
  AggregationTemporality,
  IsMonotonic,
  Count,
  Sum,
  BucketCounts,
  ExplicitBounds,
  Min,
  Max,
  Scale,
  ZeroCount,
  PositiveOffset,
  PositiveBucketCounts,
  NegativeOffset,
  NegativeBucketCounts,
  Quantiles,
  Values,
};

constexpr std::array<ColumnSpec, 32> kMetricColumns{{
  {"timestamp", duckdb::LogicalTypeId::TIMESTAMP_NS},
  {"start_timestamp", duckdb::LogicalTypeId::TIMESTAMP_NS},
  {"service_name", duckdb::LogicalTypeId::VARCHAR},
  {"metric_name", duckdb::LogicalTypeId::VARCHAR},
  {"metric_description", duckdb::LogicalTypeId::VARCHAR},
  {"metric_unit", duckdb::LogicalTypeId::VARCHAR},
  {"resource_schema_url", duckdb::LogicalTypeId::VARCHAR},
  {"scope_schema_url", duckdb::LogicalTypeId::VARCHAR},
  {"scope_name", duckdb::LogicalTypeId::VARCHAR},
  {"scope_version", duckdb::LogicalTypeId::VARCHAR},
  {"resource_attributes", duckdb::LogicalTypeId::VARCHAR},
  {"scope_attributes", duckdb::LogicalTypeId::VARCHAR},
  {"attributes", duckdb::LogicalTypeId::VARCHAR},
  {"flags", duckdb::LogicalTypeId::INTEGER},
  {"value", duckdb::LogicalTypeId::DOUBLE},
  {"exemplars", duckdb::LogicalTypeId::VARCHAR},
  {"aggregation_temporality", duckdb::LogicalTypeId::VARCHAR},
  {"is_monotonic", duckdb::LogicalTypeId::BOOLEAN},
  {"count", duckdb::LogicalTypeId::BIGINT},
  {"sum", duckdb::LogicalTypeId::DOUBLE},
  {"bucket_counts", duckdb::LogicalTypeId::LIST, duckdb::LogicalTypeId::BIGINT},
  {"explicit_bounds", duckdb::LogicalTypeId::LIST,
   duckdb::LogicalTypeId::DOUBLE},
  {"min", duckdb::LogicalTypeId::DOUBLE},
  {"max", duckdb::LogicalTypeId::DOUBLE},
  {"scale", duckdb::LogicalTypeId::INTEGER},
  {"zero_count", duckdb::LogicalTypeId::BIGINT},
  {"positive_offset", duckdb::LogicalTypeId::INTEGER},
  {"positive_bucket_counts", duckdb::LogicalTypeId::LIST,
   duckdb::LogicalTypeId::BIGINT},
  {"negative_offset", duckdb::LogicalTypeId::INTEGER},
  {"negative_bucket_counts", duckdb::LogicalTypeId::LIST,
   duckdb::LogicalTypeId::BIGINT},
  {"quantiles", duckdb::LogicalTypeId::LIST, duckdb::LogicalTypeId::DOUBLE},
  {"values", duckdb::LogicalTypeId::LIST, duckdb::LogicalTypeId::DOUBLE},
}};

// Walks the data points of the metrics holding MetricShape::Data.
template<typename MetricShape>
class PointCursor {
 public:
  using Data = typename MetricShape::Data;

  explicit PointCursor(const otel::ExportMetricsRequest* request)
    : _metrics{request} {}

  // The next point; `metric` and `data` are its metric and shape data.
  const auto* Next() {
    using Point = std::remove_cvref_t<decltype(Data{}.data_points[0])>;
    while (true) {
      if (_data != nullptr && _point < _data->data_points.size()) {
        return static_cast<const Point*>(&_data->data_points[_point++]);
      }
      _metric = _metrics.Next();
      if (_metric == nullptr) {
        return static_cast<const Point*>(nullptr);
      }
      _data = std::get_if<Data>(&_metric->data);
      _point = 0;
    }
  }

  const RecordCursor<otel::Metric>& Records() const { return _metrics; }
  const otel::Metric& Metric() const { return *_metric; }
  const Data& Shape() const { return *_data; }

 private:
  RecordCursor<otel::Metric> _metrics;
  const otel::Metric* _metric = nullptr;
  const Data* _data = nullptr;
  size_t _point = 0;
};

template<typename MetricShape>
struct MetricsSource {
  using Cursor = PointCursor<MetricShape>;
  static constexpr std::string_view kTable = MetricShape::kTable;
  static constexpr const auto& kColumns = kMetricColumns;

  static const otel::ExportMetricsRequest* Request(ConnectionContext& ctx) {
    const auto* metrics = ctx.GetOtelMetrics();
    return metrics == nullptr ? nullptr : &metrics->request;
  }

  static void BuildRows(const otel::ExportMetricsRequest& request,
                        std::vector<Row>& rows) {
    BuildMetricRows<typename MetricShape::RowShape>(request, rows);
  }

  static bool WriteNext(Cursor& cursor, Out& out) {
    using C = MetricColumn;
    const auto* point = cursor.Next();
    if (point == nullptr) {
      return false;
    }
    const auto& metric = cursor.Metric();
    out.Timestamp(C::Timestamp, point->time_unix_nano);
    out.OptionalTimestamp(C::StartTimestamp, point->start_time_unix_nano);
    out.String(C::MetricName, metric.name);
    out.Text(C::MetricDescription, metric.description);
    out.Text(C::MetricUnit, metric.unit);
    WriteScope<C>(out, cursor.Records());
    out.String(C::Attributes, otel::AttributesToJson(point->attributes));
    out.Number(C::Flags, static_cast<int32_t>(point->flags));
    MetricShape::Write(out, cursor.Shape(), *point);
    return true;
  }
};

void WriteNumber(Out& out,
                 const std::variant<std::monostate, int64_t, double>& number) {
  if (const auto* integer = std::get_if<int64_t>(&number)) {
    out.Number(MetricColumn::Value, static_cast<double>(*integer));
  } else if (const auto* real = std::get_if<double>(&number)) {
    out.Number(MetricColumn::Value, *real);
  }
}

void WriteTemporality(Out& out, otel::AggregationTemporality temporality) {
  out.String(MetricColumn::AggregationTemporality,
             otel::TemporalityName(temporality));
}

void WriteExemplars(Out& out, const std::vector<otel::Exemplar>& exemplars) {
  out.String(MetricColumn::Exemplars, otel::ExemplarsToJson(exemplars));
}

struct GaugeSource {
  using RowShape = GaugeShape;
  using Data = otel::Gauge;
  static constexpr std::string_view kTable = kOtelMetricTables[0];

  static void Write(Out& out, const Data&, const otel::NumberDataPoint& point) {
    WriteNumber(out, point.value);
    WriteExemplars(out, point.exemplars);
  }
};

struct SumSource {
  using RowShape = SumShape;
  using Data = otel::Sum;
  static constexpr std::string_view kTable = kOtelMetricTables[1];

  static void Write(Out& out, const Data& sum,
                    const otel::NumberDataPoint& point) {
    WriteNumber(out, point.value);
    WriteTemporality(out, sum.aggregation_temporality);
    out.Number(MetricColumn::IsMonotonic, sum.is_monotonic);
    WriteExemplars(out, point.exemplars);
  }
};

struct HistogramSource {
  using RowShape = HistogramShape;
  using Data = otel::Histogram;
  static constexpr std::string_view kTable = kOtelMetricTables[2];

  static void Write(Out& out, const Data& histogram,
                    const otel::HistogramDataPoint& point) {
    using C = MetricColumn;
    out.Bigint(C::Count, point.count);
    out.Double(C::Sum, point.sum);
    out.Bigints(C::BucketCounts, point.bucket_counts);
    out.Doubles(C::ExplicitBounds, point.explicit_bounds);
    out.Double(C::Min, point.min);
    out.Double(C::Max, point.max);
    WriteTemporality(out, histogram.aggregation_temporality);
    WriteExemplars(out, point.exemplars);
  }
};

struct ExponentialHistogramSource {
  using RowShape = ExponentialHistogramShape;
  using Data = otel::ExponentialHistogram;
  static constexpr std::string_view kTable = kOtelMetricTables[3];

  static void Write(Out& out, const Data& exponential,
                    const otel::ExponentialHistogramDataPoint& point) {
    using C = MetricColumn;
    out.Bigint(C::Count, point.count);
    out.Double(C::Sum, point.sum);
    out.Number(C::Scale, point.scale);
    out.Bigint(C::ZeroCount, point.zero_count);
    out.Number(C::PositiveOffset, point.positive.offset);
    out.Bigints(C::PositiveBucketCounts, point.positive.bucket_counts);
    out.Number(C::NegativeOffset, point.negative.offset);
    out.Bigints(C::NegativeBucketCounts, point.negative.bucket_counts);
    out.Double(C::Min, point.min);
    out.Double(C::Max, point.max);
    WriteTemporality(out, exponential.aggregation_temporality);
    WriteExemplars(out, point.exemplars);
  }
};

struct SummarySource {
  using RowShape = SummaryShape;
  using Data = otel::Summary;
  static constexpr std::string_view kTable = kOtelMetricTables[4];

  static void Write(Out& out, const Data&,
                    const otel::SummaryDataPoint& point) {
    using C = MetricColumn;
    out.Bigint(C::Count, point.count);
    out.Number(C::Sum, point.sum);
    std::vector<double> quantiles;
    std::vector<double> values;
    quantiles.reserve(point.quantile_values.size());
    values.reserve(point.quantile_values.size());
    for (const auto& quantile : point.quantile_values) {
      quantiles.push_back(quantile.quantile);
      values.push_back(quantile.value);
    }
    out.Doubles(C::Quantiles, quantiles);
    out.Doubles(C::Values, values);
  }
};

// --- the table function
// --------------------------------------------------------

template<typename Source>
struct SourceState final : duckdb::GlobalTableFunctionState {
  typename Source::Cursor cursor{nullptr};
  std::vector<Row> rows;
  size_t pos = 0;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
    auto state = duckdb::make_uniq<SourceState>();
    const auto* request = Source::Request(GetSereneDBContext(context));
    if (request == nullptr) {
      return state;
    }
    if (input.bind_data->Cast<SourceBindData>().direct) {
      state->cursor = typename Source::Cursor{request};
    } else {
      Source::BuildRows(*request, state->rows);
    }
    return state;
  }
};

template<typename Source>
duckdb::unique_ptr<duckdb::FunctionData> SourceBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  return BindSource(context, Source::kTable, Source::kColumns, return_types,
                    names);
}

template<typename Source>
void SourceExecute(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
                   duckdb::DataChunk& output) {
  const auto& data = input.bind_data->Cast<SourceBindData>();
  auto& state = input.global_state->Cast<SourceState<Source>>();
  if (!data.direct) {
    Emit(output, data.generic, state.rows, state.pos);
    return;
  }
  PrepareChunk(output);
  Out out{output, data};
  duckdb::idx_t row = 0;
  for (; row < STANDARD_VECTOR_SIZE; ++row) {
    out.Row(row);
    if (!Source::WriteNext(state.cursor, out)) {
      break;
    }
  }
  output.SetCardinality(row);
}

template<typename Source>
duckdb::TableFunction SourceFunction(std::string_view name) {
  return duckdb::TableFunction{duckdb::Identifier{std::string{name}},
                               duckdb::vector<duckdb::LogicalType>{},
                               SourceExecute<Source>, SourceBind<Source>,
                               SourceState<Source>::Init};
}

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

  loader.RegisterFunction(SourceFunction<LogsSource>(kOtelSourceLogsFunction));
  loader.RegisterFunction(
    SourceFunction<TracesSource>(kOtelSourceTracesFunction));
  loader.RegisterFunction(
    SourceFunction<MetricsSource<GaugeSource>>(kOtelSourceMetricsFunctions[0]));
  loader.RegisterFunction(
    SourceFunction<MetricsSource<SumSource>>(kOtelSourceMetricsFunctions[1]));
  loader.RegisterFunction(SourceFunction<MetricsSource<HistogramSource>>(
    kOtelSourceMetricsFunctions[2]));
  loader.RegisterFunction(
    SourceFunction<MetricsSource<ExponentialHistogramSource>>(
      kOtelSourceMetricsFunctions[3]));
  loader.RegisterFunction(SourceFunction<MetricsSource<SummarySource>>(
    kOtelSourceMetricsFunctions[4]));
}

}  // namespace sdb::connector
