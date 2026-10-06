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
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <compare>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "connector/duckdb_client_state.h"
#include "otel/mapper.h"
#include "otel/protobuf.h"
#include "otel/protojson.h"
#include "otel/schema_sql.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

namespace schema = otel::schema;

struct TargetColumns {
  irs::containers::FlatHashMap<std::string, size_t> columns;
};

void BindTarget(duckdb::ClientContext& context, std::string_view schema_name,
                std::string_view table_name, TargetColumns& data,
                duckdb::vector<duckdb::LogicalType>& return_types,
                duckdb::vector<duckdb::Identifier>& names) {
  auto table = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
    context,
    duckdb::QualifiedName{duckdb::DatabaseManager::GetDefaultDatabase(context),
                          duckdb::Identifier{schema_name},
                          duckdb::Identifier{table_name}},
    duckdb::OnEntryNotFound::RETURN_NULL);
  if (!table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", schema_name, ".", table_name,
                            "\" does not exist; create the OpenTelemetry "
                            "schema first"));
  }
  size_t index = 0;
  for (const auto& column : table->GetColumns().Logical()) {
    return_types.push_back(column.Type());
    names.emplace_back(column.Name().GetIdentifierName());
    data.columns.emplace(column.Name().GetIdentifierName(), index);
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

static_assert(kOtelLogsTable == schema::kLogs.name);
static_assert(kOtelTracesTable == schema::kTraces.name);
static_assert(kOtelMetricTables[0] == schema::kMetricsGauge.name);
static_assert(kOtelMetricTables[1] == schema::kMetricsSum.name);
static_assert(kOtelMetricTables[2] == schema::kMetricsHistogram.name);
static_assert(kOtelMetricTables[3] ==
              schema::kMetricsExponentialHistogram.name);
static_assert(kOtelMetricTables[4] == schema::kMetricsSummary.name);

// otel_source_<signal>(schema): the rows of the request the HTTP handler put in
// the connection's side channel; without one it returns no rows.
// Bind carries no data -- only the target's columns -- so each
// INSERT ... SELECT * FROM otel_source_*() is prepared once per connection and
// re-executed per request. The scan writes the decoded model straight into
// the output vectors, so bind rejects a table whose standard columns are
// missing or typed differently from the shipped DDL.

using schema::Type;

template<Type T>
struct CppType;
template<>
struct CppType<Type::TimestampNs> {
  using type = int64_t;
};
template<>
struct CppType<Type::Smallint> {
  using type = int16_t;
};
template<>
struct CppType<Type::Integer> {
  using type = int32_t;
};
template<>
struct CppType<Type::Bigint> {
  using type = int64_t;
};
template<>
struct CppType<Type::Double> {
  using type = double;
};
template<>
struct CppType<Type::Boolean> {
  using type = bool;
};

template<auto C>
constexpr const schema::Column& ColumnOf() {
  return schema::TableOf(C).columns[std::to_underlying(C)];
}

template<auto C, Type T, Type Element = Type::None>
constexpr void Expect() {
  static_assert(ColumnOf<C>().type == T && ColumnOf<C>().element == Element,
                "the column's type in otel_schema.sql does not match this "
                "write");
}

struct SourceBindData final : duckdb::TableFunctionData {
  const void* request = nullptr;
  // Output column of each schema column.
  std::vector<duckdb::idx_t> slots;
  std::shared_ptr<const void> parsed;
};

// Writes one output row. A chunk starts all-NULL (PrepareChunk), so a column
// nothing writes -- a deployment's own addition -- stays NULL.
class Out {
 public:
  Out(duckdb::DataChunk& output, const SourceBindData& data)
    : _output{output}, _slots{data.slots} {}

  void Row(duckdb::idx_t row) { _row = row; }

  // Empty text is NULL.
  template<auto C>
  void Text(std::string_view text) {
    if (!text.empty()) {
      String<C>(text);
    }
  }

  template<auto C>
  void String(std::string_view text) {
    Expect<C, Type::Varchar>();
    auto& vector = Slot<C>();
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vector)[_row] =
      duckdb::StringVector::AddString(vector, text.data(), text.size());
    Valid(vector);
  }

  template<auto C, typename T>
  void Number(T value) {
    static_assert(
      std::is_same_v<T, typename CppType<ColumnOf<C>().type>::type>,
      "the column's type in otel_schema.sql does not match this write");
    auto& vector = Slot<C>();
    duckdb::FlatVector::GetDataMutable<T>(vector)[_row] = value;
    Valid(vector);
  }

  template<auto C>
  void Timestamp(uint64_t unix_nano) {
    Expect<C, Type::TimestampNs>();
    Number<C>(static_cast<int64_t>(unix_nano));
  }

  // Zero is "unset" for OTLP timestamps.
  template<auto C>
  void OptionalTimestamp(uint64_t unix_nano) {
    if (unix_nano != 0) {
      Timestamp<C>(unix_nano);
    }
  }

  template<auto C>
  void Double(const std::optional<double>& value) {
    if (value) {
      Number<C>(*value);
    }
  }

  template<auto C>
  void Bigint(uint64_t value) {
    Number<C>(static_cast<int64_t>(value));
  }

  template<auto C>
  void Bigints(const std::vector<uint64_t>& items) {
    Expect<C, Type::List, Type::Bigint>();
    List<C>(items, [](duckdb::Vector& child, duckdb::idx_t i, uint64_t v) {
      duckdb::FlatVector::GetDataMutable<int64_t>(child)[i] =
        static_cast<int64_t>(v);
    });
  }

  template<auto C, typename Range, typename Project = std::identity>
  void Doubles(const Range& items, Project project = {}) {
    Expect<C, Type::List, Type::Double>();
    List<C>(
      items, [&](duckdb::Vector& child, duckdb::idx_t i, const auto& item) {
        duckdb::FlatVector::GetDataMutable<double>(child)[i] = project(item);
      });
  }

  template<auto C, typename Range, typename Project>
  void Texts(const Range& items, Project project) {
    Expect<C, Type::List, Type::Varchar>();
    List<C>(
      items, [&](duckdb::Vector& child, duckdb::idx_t i, const auto& item) {
        const std::string_view text = project(item);
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child)[i] =
          duckdb::StringVector::AddString(child, text.data(), text.size());
      });
  }

 private:
  template<auto C, typename Range, typename Put>
  void List(const Range& items, Put put) {
    auto& vector = Slot<C>();
    const auto offset = duckdb::ListVector::GetListSize(vector);
    const auto count = static_cast<duckdb::idx_t>(std::size(items));
    duckdb::ListVector::Reserve(vector, offset + count);
    auto& child = duckdb::ListVector::GetChildMutable(vector);
    auto index = offset;
    for (const auto& item : items) {
      put(child, index++, item);
    }
    duckdb::ListVector::SetListSize(vector, offset + count);
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(vector)[_row] = {
      offset, count};
    Valid(vector);
  }

  template<auto C>
  duckdb::Vector& Slot() {
    return _output.data[_slots[std::to_underlying(C)]];
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

duckdb::LogicalTypeId TypeId(Type type) {
  switch (type) {
    case Type::TimestampNs:
      return duckdb::LogicalTypeId::TIMESTAMP_NS;
    case Type::Varchar:
      return duckdb::LogicalTypeId::VARCHAR;
    case Type::Smallint:
      return duckdb::LogicalTypeId::SMALLINT;
    case Type::Integer:
      return duckdb::LogicalTypeId::INTEGER;
    case Type::Bigint:
      return duckdb::LogicalTypeId::BIGINT;
    case Type::Double:
      return duckdb::LogicalTypeId::DOUBLE;
    case Type::Boolean:
      return duckdb::LogicalTypeId::BOOLEAN;
    case Type::List:
      return duckdb::LogicalTypeId::LIST;
    case Type::None:
      break;
  }
  return duckdb::LogicalTypeId::INVALID;
}

bool Matches(const duckdb::LogicalType& type, const schema::Column& column) {
  if (type.id() != TypeId(column.type)) {
    return false;
  }
  return column.type != Type::List ||
         duckdb::ListType::GetChildType(type).id() == TypeId(column.element);
}

duckdb::LogicalType Expected(const schema::Column& column) {
  if (column.type != Type::List) {
    return duckdb::LogicalType{TypeId(column.type)};
  }
  return duckdb::LogicalType::LIST(duckdb::LogicalType{TypeId(column.element)});
}

template<typename Source>
duckdb::unique_ptr<SourceBindData> BindSource(
  duckdb::ClientContext& context, std::string_view schema_name,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  constexpr const auto& table = schema::TableOf(typename Source::Column{});
  TargetColumns target;
  BindTarget(context, schema_name, table.name, target, return_types, names);
  auto data = duckdb::make_uniq<SourceBindData>();
  data->slots.reserve(table.columns.size());
  for (const auto& column : table.columns) {
    const auto it = target.columns.find(std::string{column.name});
    if (it == target.columns.end()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TABLE_DEFINITION),
        ERR_MSG("invalid OpenTelemetry schema: column \"", column.name,
                "\" of \"", schema_name, ".", table.name, "\" is missing"));
    }
    const auto& actual = return_types[it->second];
    if (!Matches(actual, column)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TABLE_DEFINITION),
        ERR_MSG("invalid OpenTelemetry schema: column \"", column.name,
                "\" of \"", schema_name, ".", table.name, "\" is ",
                actual.ToString(), ", expected ", Expected(column).ToString()));
    }
    data->slots.push_back(it->second);
  }
  return data;
}

// Values a record inherits from its outer scope or resource, shared by every
// record of one scope and computed once per scope.
struct InheritedColumns {
  std::string service_name;
  bool has_service = false;
  std::string resource_attributes;
  std::string scope_attributes;
};

struct RecordPosition {
  size_t resource = 0;
  size_t scope = 0;
  size_t record = 0;

  auto operator<=>(const RecordPosition&) const = default;
};

inline constexpr size_t kMorselRows = 8 * STANDARD_VECTOR_SIZE;

template<typename Source>
std::vector<RecordPosition> SplitIntoMorsels(
  const typename Source::Request& request) {
  std::vector<RecordPosition> bounds{RecordPosition{}};
  size_t rows = 0;
  for (size_t r = 0; r < request.resources.size(); ++r) {
    const auto& scopes = request.resources[r].scopes;
    for (size_t s = 0; s < scopes.size(); ++s) {
      const auto& records = scopes[s].records;
      for (size_t i = 0; i < records.size(); ++i) {
        if (rows >= kMorselRows) {
          bounds.push_back(RecordPosition{r, s, i});
          rows = 0;
        }
        rows += Source::Rows(records[i]);
      }
    }
  }
  bounds.push_back(RecordPosition{request.resources.size(), 0, 0});
  return bounds;
}

// Walks resources -> scopes -> records from `begin` up to `end`.
template<typename Record>
class RecordCursor {
 public:
  RecordCursor(const otel::ExportRequest<Record>* request, RecordPosition begin,
               RecordPosition end)
    : _request{request},
      _resource{begin.resource},
      _scope{begin.scope},
      _record{begin.record},
      _end{end} {}

  const Record* Next() {
    while (_resource < _request->resources.size()) {
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
      if (RecordPosition{_resource, _scope, _record} >= _end) {
        return nullptr;
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
  const InheritedColumns& Shared() const { return _shared; }

 private:
  const otel::ExportRequest<Record>* _request;
  size_t _resource;
  size_t _scope;
  size_t _record;
  RecordPosition _end;
  bool _ready = false;
  InheritedColumns _shared;
};

template<typename Column, typename Put>
void ForEachColumn(Put&& put) {
  constexpr size_t kCount = schema::TableOf(Column{}).columns.size();
  [&]<size_t... I>(std::index_sequence<I...>) {
    (put.template operator()<static_cast<Column>(I)>(), ...);
  }(std::make_index_sequence<kCount>{});
}

template<auto C>
constexpr bool Named(std::string_view name) {
  return ColumnOf<C>().name == name;
}

// A column the record inherits from its outer scope or resource rather than
// carrying itself. Every table has them, under the same enumerator names.
template<auto C>
constexpr bool IsInherited() {
  using Column = decltype(C);
  return C == Column::ServiceName || C == Column::ResourceSchemaUrl ||
         C == Column::ScopeSchemaUrl || C == Column::ScopeName ||
         C == Column::ScopeVersion || C == Column::ResourceAttributes ||
         C == Column::ScopeAttributes;
}

template<auto C, typename Record>
void PutInherited(Out& out, const RecordCursor<Record>& cursor) {
  using Column = decltype(C);
  const auto& shared = cursor.Shared();
  if constexpr (C == Column::ServiceName) {
    if (shared.has_service) {
      out.Text<C>(shared.service_name);
    }
  } else if constexpr (C == Column::ResourceSchemaUrl) {
    out.Text<C>(cursor.Resources().schema_url);
  } else if constexpr (C == Column::ScopeSchemaUrl) {
    out.Text<C>(cursor.Scopes().schema_url);
  } else if constexpr (C == Column::ScopeName) {
    out.Text<C>(cursor.Scopes().scope.name);
  } else if constexpr (C == Column::ScopeVersion) {
    out.Text<C>(cursor.Scopes().scope.version);
  } else if constexpr (C == Column::ResourceAttributes) {
    out.String<C>(shared.resource_attributes);
  } else {
    out.String<C>(shared.scope_attributes);
  }
}

// --- logs --------------------------------------------------------------------

struct LogsSource {
  using Record = otel::LogRecord;
  using Cursor = RecordCursor<Record>;
  using Column = schema::LogsColumn;
  using Request = otel::ExportLogsRequest;

  static size_t Rows(const Record&) { return 1; }

  template<Column C>
  static void Put(Out& out, const Cursor& cursor, const Record& record) {
    using enum Column;
    if constexpr (IsInherited<C>()) {
      PutInherited<C>(out, cursor);
    } else if constexpr (C == Timestamp) {
      out.Timestamp<C>(record.time_unix_nano != 0
                         ? record.time_unix_nano
                         : record.observed_time_unix_nano);
    } else if constexpr (C == ObservedTimestamp) {
      out.OptionalTimestamp<C>(record.observed_time_unix_nano);
    } else if constexpr (C == TraceId) {
      out.Text<C>(record.trace_id.hex);
    } else if constexpr (C == SpanId) {
      out.Text<C>(record.span_id.hex);
    } else if constexpr (C == TraceFlags) {
      out.Number<C>(static_cast<int32_t>(record.flags));
    } else if constexpr (C == SeverityText) {
      out.Text<C>(record.severity_text);
    } else if constexpr (C == SeverityNumber) {
      if (record.severity_number != otel::SeverityNumber::Unspecified) {
        out.Number<C>(
          static_cast<int16_t>(std::to_underlying(record.severity_number)));
      }
    } else if constexpr (C == EventName) {
      if (!record.event_name.empty()) {
        out.Text<C>(record.event_name);
      } else if (const auto* attribute = otel::FindAttribute(
                   record.attributes, otel::kEventNameKey)) {
        out.Text<C>(otel::BodyToText(attribute));
      }
    } else if constexpr (C == Body) {
      out.Text<C>(otel::BodyToText(record.body));
    } else if constexpr (C == LogAttributes) {
      out.String<C>(otel::AttributesToJson(record.attributes));
    } else {
      static_assert(false, ColumnOf<C>().name);
    }
  }

  static bool WriteNext(Cursor& cursor, Out& out) {
    const auto* record = cursor.Next();
    if (record == nullptr) {
      return false;
    }
    ForEachColumn<Column>([&]<Column C> { Put<C>(out, cursor, *record); });
    return true;
  }
};

// --- traces ------------------------------------------------------------------

struct TracesSource {
  using Record = otel::Span;
  using Cursor = RecordCursor<Record>;
  using Column = schema::TracesColumn;
  using Request = otel::ExportTracesRequest;

  static size_t Rows(const Record&) { return 1; }

  template<Column C>
  static void Put(Out& out, const Cursor& cursor, const Record& span) {
    using enum Column;
    if constexpr (IsInherited<C>()) {
      PutInherited<C>(out, cursor);
    } else if constexpr (C == Timestamp) {
      out.Timestamp<C>(span.start_time_unix_nano);
    } else if constexpr (C == EndTimestamp) {
      out.OptionalTimestamp<C>(span.end_time_unix_nano);
    } else if constexpr (C == TraceId) {
      out.Text<C>(span.trace_id.hex);
    } else if constexpr (C == SpanId) {
      out.Text<C>(span.span_id.hex);
    } else if constexpr (C == ParentSpanId) {
      out.Text<C>(span.parent_span_id.hex);
    } else if constexpr (C == TraceState) {
      out.Text<C>(span.trace_state);
    } else if constexpr (C == SpanName) {
      out.Text<C>(span.name);
    } else if constexpr (C == SpanKind) {
      out.String<C>(otel::SpanKindName(span.kind));
    } else if constexpr (C == DurationNs) {
      if (span.end_time_unix_nano >= span.start_time_unix_nano &&
          span.end_time_unix_nano != 0) {
        out.Number<C>(static_cast<int64_t>(span.end_time_unix_nano -
                                           span.start_time_unix_nano));
      }
    } else if constexpr (C == StatusCode) {
      out.String<C>(otel::StatusCodeName(span.status.code));
    } else if constexpr (C == StatusMessage) {
      out.Text<C>(span.status.message);
    } else if constexpr (C == SpanAttributes) {
      out.String<C>(otel::AttributesToJson(span.attributes));
    } else if constexpr (C == Events) {
      out.String<C>(otel::EventsToJson(span.events));
    } else if constexpr (C == Links) {
      out.String<C>(otel::LinksToJson(span.links));
    } else if constexpr (C == EventNames) {
      out.Texts<C>(span.events,
                   [](const otel::SpanEvent& event) { return event.name; });
    } else if constexpr (C == LinkTraceIds) {
      out.Texts<C>(span.links,
                   [](const otel::SpanLink& link) -> std::string_view {
                     return link.trace_id.hex;
                   });
    } else {
      static_assert(false, ColumnOf<C>().name);
    }
  }

  static bool WriteNext(Cursor& cursor, Out& out) {
    const auto* span = cursor.Next();
    if (span == nullptr) {
      return false;
    }
    ForEachColumn<Column>([&]<Column C> { Put<C>(out, cursor, *span); });
    return true;
  }
};

// --- metrics -----------------------------------------------------------------

// Walks the data points of the metrics holding MetricShape::Data.
template<typename MetricShape>
class PointCursor {
 public:
  using Data = typename MetricShape::Data;
  using Point = std::remove_cvref_t<decltype(Data{}.data_points[0])>;

  PointCursor(const otel::ExportMetricsRequest* request, RecordPosition begin,
              RecordPosition end)
    : _metrics{request, begin, end} {}

  // The next point; `metric` and `data` are its metric and shape data.
  const Point* Next() {
    while (true) {
      if (_data != nullptr && _point < _data->data_points.size()) {
        return &_data->data_points[_point++];
      }
      _metric = _metrics.Next();
      if (_metric == nullptr) {
        return nullptr;
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
  using Column = typename MetricShape::Column;
  using Request = otel::ExportMetricsRequest;

  // Single metric may have multiple datapoints
  static size_t Rows(const otel::Metric& metric) {
    const auto* data = std::get_if<typename MetricShape::Data>(&metric.data);
    return data == nullptr ? 0 : data->data_points.size();
  }

  template<Column C>
  static void Put(Out& out, const Cursor& cursor,
                  const typename Cursor::Point& point) {
    const auto& metric = cursor.Metric();
    if constexpr (IsInherited<C>()) {
      PutInherited<C>(out, cursor.Records());
    } else if constexpr (C == Column::Timestamp) {
      out.Timestamp<C>(point.time_unix_nano);
    } else if constexpr (C == Column::StartTimestamp) {
      out.OptionalTimestamp<C>(point.start_time_unix_nano);
    } else if constexpr (C == Column::MetricName) {
      out.String<C>(metric.name);
    } else if constexpr (C == Column::MetricDescription) {
      out.Text<C>(metric.description);
    } else if constexpr (C == Column::MetricUnit) {
      out.Text<C>(metric.unit);
    } else if constexpr (C == Column::Attributes) {
      out.String<C>(otel::AttributesToJson(point.attributes));
    } else if constexpr (C == Column::Flags) {
      out.Number<C>(static_cast<int32_t>(point.flags));
    } else if constexpr (Named<C>("value")) {
      if (const auto* integer = std::get_if<int64_t>(&point.value)) {
        out.Number<C>(static_cast<double>(*integer));
      } else if (const auto* real = std::get_if<double>(&point.value)) {
        out.Number<C>(*real);
      }
    } else if constexpr (Named<C>("aggregation_temporality")) {
      out.String<C>(
        otel::TemporalityName(cursor.Shape().aggregation_temporality));
    } else if constexpr (Named<C>("exemplars")) {
      out.String<C>(otel::ExemplarsToJson(point.exemplars));
    } else {
      MetricShape::template Put<C>(out, cursor.Shape(), point);
    }
  }

  static bool WriteNext(Cursor& cursor, Out& out) {
    const auto* point = cursor.Next();
    if (point == nullptr) {
      return false;
    }
    ForEachColumn<Column>([&]<Column C> { Put<C>(out, cursor, *point); });
    return true;
  }
};

struct GaugeSource {
  using Data = otel::Gauge;
  using Column = schema::MetricsGaugeColumn;

  template<Column C>
  static void Put(Out&, const Data&, const otel::NumberDataPoint&) {
    static_assert(false, ColumnOf<C>().name);
  }
};

struct SumSource {
  using Data = otel::Sum;
  using Column = schema::MetricsSumColumn;

  template<Column C>
  static void Put(Out& out, const Data& sum, const otel::NumberDataPoint&) {
    if constexpr (C == Column::IsMonotonic) {
      out.Number<C>(sum.is_monotonic);
    } else {
      static_assert(false, ColumnOf<C>().name);
    }
  }
};

struct HistogramSource {
  using Data = otel::Histogram;
  using Column = schema::MetricsHistogramColumn;

  template<Column C>
  static void Put(Out& out, const Data&,
                  const otel::HistogramDataPoint& point) {
    using enum Column;
    if constexpr (C == Count) {
      out.Bigint<C>(point.count);
    } else if constexpr (C == Sum) {
      out.Double<C>(point.sum);
    } else if constexpr (C == BucketCounts) {
      out.Bigints<C>(point.bucket_counts);
    } else if constexpr (C == ExplicitBounds) {
      out.Doubles<C>(point.explicit_bounds);
    } else if constexpr (C == Min) {
      out.Double<C>(point.min);
    } else if constexpr (C == Max) {
      out.Double<C>(point.max);
    } else {
      static_assert(false, ColumnOf<C>().name);
    }
  }
};

struct ExponentialHistogramSource {
  using Data = otel::ExponentialHistogram;
  using Column = schema::MetricsExponentialHistogramColumn;

  template<Column C>
  static void Put(Out& out, const Data&,
                  const otel::ExponentialHistogramDataPoint& point) {
    using enum Column;
    if constexpr (C == Count) {
      out.Bigint<C>(point.count);
    } else if constexpr (C == Sum) {
      out.Double<C>(point.sum);
    } else if constexpr (C == Scale) {
      out.Number<C>(point.scale);
    } else if constexpr (C == ZeroCount) {
      out.Bigint<C>(point.zero_count);
    } else if constexpr (C == PositiveOffset) {
      out.Number<C>(point.positive.offset);
    } else if constexpr (C == PositiveBucketCounts) {
      out.Bigints<C>(point.positive.bucket_counts);
    } else if constexpr (C == NegativeOffset) {
      out.Number<C>(point.negative.offset);
    } else if constexpr (C == NegativeBucketCounts) {
      out.Bigints<C>(point.negative.bucket_counts);
    } else if constexpr (C == Min) {
      out.Double<C>(point.min);
    } else if constexpr (C == Max) {
      out.Double<C>(point.max);
    } else {
      static_assert(false, ColumnOf<C>().name);
    }
  }
};

struct SummarySource {
  using Data = otel::Summary;
  using Column = schema::MetricsSummaryColumn;

  template<Column C>
  static void Put(Out& out, const Data&, const otel::SummaryDataPoint& point) {
    using enum Column;
    if constexpr (C == Count) {
      out.Bigint<C>(point.count);
    } else if constexpr (C == Sum) {
      out.Number<C>(point.sum);
    } else if constexpr (C == Quantiles) {
      out.Doubles<C>(point.quantile_values,
                     [](const auto& quantile) { return quantile.quantile; });
    } else if constexpr (C == Values) {
      out.Doubles<C>(point.quantile_values,
                     [](const auto& quantile) { return quantile.value; });
    } else {
      static_assert(false, ColumnOf<C>().name);
    }
  }
};

// --- the table function ------------------------------------------------------

template<typename Source>
struct SourceState final : duckdb::GlobalTableFunctionState {
  const typename Source::Request* request = nullptr;
  std::vector<RecordPosition> bounds;
  std::atomic<size_t> next{0};

  duckdb::idx_t MaxThreads() const final { return bounds.size() - 1; }

  std::optional<typename Source::Cursor> Claim() {
    const size_t morsel = next.fetch_add(1, std::memory_order_relaxed);
    if (morsel + 1 >= bounds.size()) {
      return std::nullopt;
    }
    return typename Source::Cursor{request, bounds[morsel], bounds[morsel + 1]};
  }

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
    using Request = const typename Source::Request;
    auto state = duckdb::make_uniq<SourceState>();
    const auto& data = input.bind_data->Cast<SourceBindData>();
    const auto* request =
      data.request != nullptr
        ? static_cast<Request*>(data.request)
        : GetSereneDBContext(context).GetSideChannel<Request>();
    if (request == nullptr) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("OpenTelemetry source functions can only be called by the "
                "OTLP/HTTP endpoint"));
    }
    state->request = request;
    state->bounds = SplitIntoMorsels<Source>(*request);
    return state;
  }
};

template<typename Source>
struct SourceLocalState final : duckdb::LocalTableFunctionState {
  std::optional<typename Source::Cursor> cursor;

  static duckdb::unique_ptr<duckdb::LocalTableFunctionState> Init(
    duckdb::ExecutionContext&, duckdb::TableFunctionInitInput&,
    duckdb::GlobalTableFunctionState*) {
    return duckdb::make_uniq<SourceLocalState>();
  }
};

template<typename Source>
duckdb::unique_ptr<duckdb::FunctionData> SourceBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  if (input.inputs[0].IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("schema name cannot be NULL"));
  }
  return BindSource<Source>(context, input.inputs[0].GetValue<std::string>(),
                            return_types, names);
}

template<typename Source>
void SourceExecute(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
                   duckdb::DataChunk& output) {
  const auto& data = input.bind_data->Cast<SourceBindData>();
  auto& state = input.global_state->Cast<SourceState<Source>>();
  auto& local = input.local_state->Cast<SourceLocalState<Source>>();
  PrepareChunk(output);
  Out out{output, data};
  duckdb::idx_t row = 0;
  while (row < STANDARD_VECTOR_SIZE) {
    out.Row(row);
    if (local.cursor && Source::WriteNext(*local.cursor, out)) {
      ++row;
      continue;
    }
    local.cursor = state.Claim();
    if (!local.cursor) {
      break;
    }
  }
  output.SetCardinality(row);
}

void Decode(std::string_view wire, bool protobuf,
            simdjson::ondemand::parser& parser, otel::ExportLogsRequest& out) {
  if (protobuf) {
    otel::DecodeLogsRequest(wire, out);
  } else {
    otel::ParseLogsRequest(wire, parser, out, /*padded=*/true);
  }
}

void Decode(std::string_view wire, bool protobuf,
            simdjson::ondemand::parser& parser,
            otel::ExportTracesRequest& out) {
  if (protobuf) {
    otel::DecodeTracesRequest(wire, out);
  } else {
    otel::ParseTracesRequest(wire, parser, out, /*padded=*/true);
  }
}

void Decode(std::string_view wire, bool protobuf,
            simdjson::ondemand::parser& parser,
            otel::ExportMetricsRequest& out) {
  if (protobuf) {
    otel::DecodeMetricsRequest(wire, out);
  } else {
    otel::ParseMetricsRequest(wire, parser, out, /*padded=*/true);
  }
}

template<typename Request>
struct ParsedPayload {
  std::string wire;
  simdjson::ondemand::parser parser;
  Request request;
};

template<typename Source>
duckdb::unique_ptr<duckdb::FunctionData> ParseBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  auto data = BindSource<Source>(context, kOtelSchema, return_types, names);
  const bool protobuf = ReadProtobufArgument(input.inputs);
  auto parsed = std::make_shared<ParsedPayload<typename Source::Request>>();
  parsed->wire = ReadBodyArgument(input.inputs[0]);
  if (protobuf) {
    parsed->wire = DecodeBase64Payload(parsed->wire);
  }
  const size_t size = parsed->wire.size();
  parsed->wire.append(otel::kJsonPadding, '\0');
  Decode(std::string_view{parsed->wire.data(), size}, protobuf, parsed->parser,
         parsed->request);
  data->request = &parsed->request;
  data->parsed = std::move(parsed);
  return data;
}

}  // namespace

void RegisterOtelFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  const auto add = [&]<typename Source>(const char* name) {
    for (auto arguments :
         {duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::VARCHAR},
          duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::VARCHAR,
                                              duckdb::LogicalType::VARCHAR}}) {
      loader.RegisterFunction(duckdb::TableFunction{
        name, std::move(arguments), SourceExecute<Source>, ParseBind<Source>,
        SourceState<Source>::Init, SourceLocalState<Source>::Init});
    }
  };

  const auto source = [&]<typename Source>(std::string_view name) {
    loader.RegisterFunction(duckdb::TableFunction{
      duckdb::Identifier{std::string{name}},
      duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::VARCHAR},
      SourceExecute<Source>, SourceBind<Source>, SourceState<Source>::Init,
      SourceLocalState<Source>::Init});
  };
  source.operator()<LogsSource>(kOtelSourceLogsFunction);
  source.operator()<TracesSource>(kOtelSourceTracesFunction);
  source.operator()<MetricsSource<GaugeSource>>(kOtelSourceMetricsFunctions[0]);
  source.operator()<MetricsSource<SumSource>>(kOtelSourceMetricsFunctions[1]);
  source.operator()<MetricsSource<HistogramSource>>(
    kOtelSourceMetricsFunctions[2]);
  source.operator()<MetricsSource<ExponentialHistogramSource>>(
    kOtelSourceMetricsFunctions[3]);
  source.operator()<MetricsSource<SummarySource>>(
    kOtelSourceMetricsFunctions[4]);

  add.operator()<LogsSource>("otel_parse_logs");
  add.operator()<TracesSource>("otel_parse_traces");
  add.operator()<MetricsSource<GaugeSource>>("otel_parse_metrics_gauge");
  add.operator()<MetricsSource<SumSource>>("otel_parse_metrics_sum");
  add.operator()<MetricsSource<HistogramSource>>(
    "otel_parse_metrics_histogram");
  add.operator()<MetricsSource<ExponentialHistogramSource>>(
    "otel_parse_metrics_exponential_histogram");
  add.operator()<MetricsSource<SummarySource>>("otel_parse_metrics_summary");
}

}  // namespace sdb::connector
