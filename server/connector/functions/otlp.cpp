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

#include "connector/functions/otlp.h"

#include <absl/strings/escaping.h>

#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/value.hpp>
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

struct OtlpBindData final : duckdb::TableFunctionData {
  std::string body;
  std::vector<Row> rows;
  irs::containers::FlatHashMap<std::string, size_t> columns;
  duckdb::vector<duckdb::LogicalType> types;
};

struct OtlpState final : duckdb::GlobalTableFunctionState {
  size_t pos = 0;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<OtlpState>();
  }
};

void BindTarget(duckdb::ClientContext& context, std::string_view table_name,
                OtlpBindData& data,
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
        row.push_back({"trace_id", NullableText(record.trace_id)});
        row.push_back({"span_id", NullableText(record.span_id)});
        row.push_back({"trace_flags", duckdb::Value::INTEGER(
                                        static_cast<int32_t>(record.flags))});
        row.push_back({"severity_text", NullableText(record.severity_text)});
        row.push_back({"severity_number",
                       record.severity_number == 0
                         ? duckdb::Value{}
                         : duckdb::Value::SMALLINT(
                             static_cast<int16_t>(record.severity_number))});
        std::string event_name = record.event_name;
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
        row.push_back({"trace_id", NullableText(span.trace_id)});
        row.push_back({"span_id", NullableText(span.span_id)});
        row.push_back({"parent_span_id", NullableText(span.parent_span_id)});
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
          event_names.push_back(event.name);
        }
        std::vector<std::string> link_trace_ids;
        link_trace_ids.reserve(span.links.size());
        for (const auto& link : span.links) {
          link_trace_ids.push_back(link.trace_id);
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

enum class MetricTable : size_t {
  Gauge = 0,
  Sum = 1,
  Histogram = 2,
  ExponentialHistogram = 3,
  Summary = 4,
};

void BuildMetricRows(const otel::ExportMetricsRequest& request,
                     MetricTable table, std::vector<Row>& rows) {
  for (const auto& resource_records : request.resources) {
    const auto& resource = resource_records.resource;
    for (const auto& scope_records : resource_records.scopes) {
      const auto& scope = scope_records.scope;
      for (const auto& metric : scope_records.records) {
        const auto common = [&](Row& row, const auto& point) {
          AppendMetricCommon(row, metric, point, resource, scope,
                             resource_records.schema_url,
                             scope_records.schema_url);
        };
        if (table == MetricTable::Gauge) {
          const auto* gauge = std::get_if<otel::Gauge>(&metric.data);
          if (gauge == nullptr) {
            continue;
          }
          for (const auto& point : gauge->data_points) {
            Row row;
            common(row, point);
            row.push_back({"value", NumberValue(point.value)});
            row.push_back({"exemplars", duckdb::Value{otel::ExemplarsToJson(
                                          point.exemplars)}});
            rows.push_back(std::move(row));
          }
        } else if (table == MetricTable::Sum) {
          const auto* sum = std::get_if<otel::Sum>(&metric.data);
          if (sum == nullptr) {
            continue;
          }
          for (const auto& point : sum->data_points) {
            Row row;
            common(row, point);
            row.push_back({"value", NumberValue(point.value)});
            row.push_back({"aggregation_temporality",
                           duckdb::Value{std::string{otel::TemporalityName(
                             sum->aggregation_temporality)}}});
            row.push_back(
              {"is_monotonic", duckdb::Value::BOOLEAN(sum->is_monotonic)});
            row.push_back({"exemplars", duckdb::Value{otel::ExemplarsToJson(
                                          point.exemplars)}});
            rows.push_back(std::move(row));
          }
        } else if (table == MetricTable::Histogram) {
          const auto* histogram = std::get_if<otel::Histogram>(&metric.data);
          if (histogram == nullptr) {
            continue;
          }
          for (const auto& point : histogram->data_points) {
            Row row;
            common(row, point);
            row.push_back({"count", duckdb::Value::BIGINT(
                                      static_cast<int64_t>(point.count))});
            row.push_back({"sum", OptionalDouble(point.sum)});
            row.push_back({"bucket_counts", BigintList(point.bucket_counts)});
            row.push_back(
              {"explicit_bounds", DoubleList(point.explicit_bounds)});
            row.push_back({"min", OptionalDouble(point.min)});
            row.push_back({"max", OptionalDouble(point.max)});
            row.push_back({"aggregation_temporality",
                           duckdb::Value{std::string{otel::TemporalityName(
                             histogram->aggregation_temporality)}}});
            row.push_back({"exemplars", duckdb::Value{otel::ExemplarsToJson(
                                          point.exemplars)}});
            rows.push_back(std::move(row));
          }
        } else if (table == MetricTable::ExponentialHistogram) {
          const auto* exponential =
            std::get_if<otel::ExponentialHistogram>(&metric.data);
          if (exponential == nullptr) {
            continue;
          }
          for (const auto& point : exponential->data_points) {
            Row row;
            common(row, point);
            row.push_back({"count", duckdb::Value::BIGINT(
                                      static_cast<int64_t>(point.count))});
            row.push_back({"sum", OptionalDouble(point.sum)});
            row.push_back({"scale", duckdb::Value::INTEGER(point.scale)});
            row.push_back(
              {"zero_count",
               duckdb::Value::BIGINT(static_cast<int64_t>(point.zero_count))});
            row.push_back({"positive_offset",
                           duckdb::Value::INTEGER(point.positive.offset)});
            row.push_back({"positive_bucket_counts",
                           BigintList(point.positive.bucket_counts)});
            row.push_back({"negative_offset",
                           duckdb::Value::INTEGER(point.negative.offset)});
            row.push_back({"negative_bucket_counts",
                           BigintList(point.negative.bucket_counts)});
            row.push_back({"min", OptionalDouble(point.min)});
            row.push_back({"max", OptionalDouble(point.max)});
            row.push_back({"aggregation_temporality",
                           duckdb::Value{std::string{otel::TemporalityName(
                             exponential->aggregation_temporality)}}});
            row.push_back({"exemplars", duckdb::Value{otel::ExemplarsToJson(
                                          point.exemplars)}});
            rows.push_back(std::move(row));
          }
        } else {
          const auto* summary = std::get_if<otel::Summary>(&metric.data);
          if (summary == nullptr) {
            continue;
          }
          for (const auto& point : summary->data_points) {
            Row row;
            common(row, point);
            row.push_back({"count", duckdb::Value::BIGINT(
                                      static_cast<int64_t>(point.count))});
            row.push_back({"sum", duckdb::Value::DOUBLE(point.sum)});
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
            rows.push_back(std::move(row));
          }
        }
      }
    }
  }
}

void Emit(duckdb::DataChunk& output, const OtlpBindData& data,
          OtlpState& state) {
  duckdb::idx_t emitted = 0;
  while (emitted < STANDARD_VECTOR_SIZE && state.pos < data.rows.size()) {
    for (duckdb::idx_t column = 0; column < output.ColumnCount(); ++column) {
      output.SetValue(column, emitted, duckdb::Value{});
    }
    for (const auto& cell : data.rows[state.pos]) {
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
    ++state.pos;
    ++emitted;
  }
  output.SetCardinality(emitted);
}

template<const std::string_view& Table, auto Build>
duckdb::unique_ptr<duckdb::FunctionData> OtlpBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = duckdb::make_uniq<OtlpBindData>();
  BindTarget(context, Table, *data, return_types, names);
  data->body = ReadBodyArgument(input.inputs[0]);
  Build(context, data->body, ReadProtobufArgument(input.inputs), data->rows);
  return data;
}

void OtlpExecute(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
                 duckdb::DataChunk& output) {
  Emit(output, input.bind_data->Cast<OtlpBindData>(),
       input.global_state->Cast<OtlpState>());
}

void BuildLogs(duckdb::ClientContext&, const std::string& body, bool protobuf,
               std::vector<Row>& rows) {
  otel::ExportLogsRequest request;
  if (protobuf) {
    otel::DecodeLogsRequest(DecodeBase64Payload(body), request);
  } else {
    otel::ParseLogsRequest(body, request);
  }
  BuildLogRows(request, rows);
}

void BuildTraces(duckdb::ClientContext&, const std::string& body, bool protobuf,
                 std::vector<Row>& rows) {
  otel::ExportTracesRequest request;
  if (protobuf) {
    otel::DecodeTracesRequest(DecodeBase64Payload(body), request);
  } else {
    otel::ParseTracesRequest(body, request);
  }
  BuildSpanRows(request, rows);
}

template<MetricTable Table>
void BuildMetrics(duckdb::ClientContext& context, const std::string& body,
                  bool protobuf, std::vector<Row>& rows) {
  // One request feeds five tables, so the handler decodes the payload once and
  // leaves it on the connection; only a standalone SQL call decodes here.
  if (const auto* decoded = GetSereneDBContext(context).GetOtlpMetrics()) {
    BuildMetricRows(decoded->request, Table, rows);
    return;
  }
  otel::ExportMetricsRequest request;
  if (protobuf) {
    otel::DecodeMetricsRequest(DecodeBase64Payload(body), request);
  } else {
    otel::ParseMetricsRequest(body, request);
  }
  BuildMetricRows(request, Table, rows);
}

constexpr std::string_view kLogsTable = kOtelLogsTable;
constexpr std::string_view kTracesTable = kOtelTracesTable;
constexpr std::string_view kGaugeTable = kOtelMetricTables[0];
constexpr std::string_view kSumTable = kOtelMetricTables[1];
constexpr std::string_view kHistogramTable = kOtelMetricTables[2];
constexpr std::string_view kExponentialTable = kOtelMetricTables[3];
constexpr std::string_view kSummaryTable = kOtelMetricTables[4];

}  // namespace

void RegisterOtlpFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  const auto add = [&](const char* name, auto bind) {
    loader.RegisterFunction(
      duckdb::TableFunction{name,
                            {duckdb::LogicalType::VARCHAR},
                            OtlpExecute,
                            bind,
                            OtlpState::Init});
    loader.RegisterFunction(duckdb::TableFunction{
      name,
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      OtlpExecute,
      bind,
      OtlpState::Init});
  };

  add("otlp_logs", OtlpBind<kLogsTable, BuildLogs>);
  add("otlp_traces", OtlpBind<kTracesTable, BuildTraces>);
  add("otlp_metrics_gauge",
      OtlpBind<kGaugeTable, BuildMetrics<MetricTable::Gauge>>);
  add("otlp_metrics_sum", OtlpBind<kSumTable, BuildMetrics<MetricTable::Sum>>);
  add("otlp_metrics_histogram",
      OtlpBind<kHistogramTable, BuildMetrics<MetricTable::Histogram>>);
  add("otlp_metrics_exponential_histogram",
      OtlpBind<kExponentialTable,
               BuildMetrics<MetricTable::ExponentialHistogram>>);
  add("otlp_metrics_summary",
      OtlpBind<kSummaryTable, BuildMetrics<MetricTable::Summary>>);
}

}  // namespace sdb::connector
