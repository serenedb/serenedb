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

#include "otel/protobuf.h"

#include <absl/strings/escaping.h>

#include <cstdint>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <protozero/exception.hpp>
#include <protozero/pbf_reader.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Wire format:      https://protobuf.dev/programming-guides/encoding/
// Message schemas:  https://github.com/open-telemetry/opentelemetry-proto
namespace sdb::otel {
namespace {

using protozero::pbf_reader;
using protozero::pbf_wire_type;

template<typename Field>
constexpr uint32_t Tag(Field field) {
  return std::to_underlying(field);
}

std::string_view View(protozero::data_view view) {
  return {view.data(), view.size()};
}

std::string_view Text(pbf_reader& reader) { return View(reader.get_view()); }

std::string ReadHexId(pbf_reader& reader) {
  const auto raw = View(reader.get_view());
  if (raw.empty() || raw.find_first_not_of('\0') == std::string_view::npos) {
    return {};
  }
  return absl::BytesToHexString(raw);
}

template<typename T, typename Packed, typename Single>
void ReadRepeated(pbf_reader& reader, std::vector<T>& out, Packed packed,
                  Single single) {
  if (reader.wire_type() == pbf_wire_type::length_delimited) {
    for (const auto item : packed(reader)) {
      out.push_back(item);
    }
    return;
  }
  out.push_back(single(reader));
}

void ReadUint64s(pbf_reader& reader, std::vector<uint64_t>& out) {
  ReadRepeated(
    reader, out, [](pbf_reader& r) { return r.get_packed_uint64(); },
    [](pbf_reader& r) { return r.get_uint64(); });
}

void ReadFixed64s(pbf_reader& reader, std::vector<uint64_t>& out) {
  ReadRepeated(
    reader, out, [](pbf_reader& r) { return r.get_packed_fixed64(); },
    [](pbf_reader& r) { return r.get_fixed64(); });
}

void ReadDoubles(pbf_reader& reader, std::vector<double>& out) {
  ReadRepeated(
    reader, out, [](pbf_reader& r) { return r.get_packed_double(); },
    [](pbf_reader& r) { return r.get_double(); });
}

AnyValue* DecodeAnyValue(pbf_reader reader, ValueArena& arena);

void DecodeKeyValue(pbf_reader reader, KeyValue& out, ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<KeyValueTag>(reader.tag())) {
      case KeyValueTag::Key:
        out.key = Text(reader);
        break;
      case KeyValueTag::Value:
        out.value = DecodeAnyValue(reader.get_message(), arena);
        break;
      default:
        reader.skip();
    }
  }
}

KeyValueList DecodeKeyValueList(pbf_reader reader, ValueArena& arena) {
  KeyValueList out;
  while (reader.next()) {
    if (reader.tag() == Tag(KeyValueListTag::Values)) {
      DecodeKeyValue(reader.get_message(), out.emplace_back(), arena);
    } else {
      reader.skip();
    }
  }
  return out;
}

AnyValue* DecodeAnyValue(pbf_reader reader, ValueArena& arena) {
  AnyValue* out = arena.Make();
  while (reader.next()) {
    switch (static_cast<AnyValueTag>(reader.tag())) {
      case AnyValueTag::StringValue:
        out->value = Text(reader);
        break;
      case AnyValueTag::BoolValue:
        out->value = reader.get_bool();
        break;
      case AnyValueTag::IntValue:
        out->value = reader.get_int64();
        break;
      case AnyValueTag::DoubleValue:
        out->value = reader.get_double();
        break;
      case AnyValueTag::ArrayValue: {
        ArrayValue array;
        pbf_reader values = reader.get_message();
        while (values.next()) {
          if (values.tag() == Tag(ArrayValueTag::Values)) {
            array.values.push_back(DecodeAnyValue(values.get_message(), arena));
          } else {
            values.skip();
          }
        }
        out->value = std::move(array);
        break;
      }
      case AnyValueTag::KvlistValue:
        out->value = KvlistValue{
          .values = DecodeKeyValueList(reader.get_message(), arena)};
        break;
      case AnyValueTag::BytesValue:
        out->value =
          BytesValue{.data = absl::Base64Escape(View(reader.get_view()))};
        break;
      default:
        reader.skip();
    }
  }
  return out;
}

void DecodeResource(pbf_reader reader, Resource& out, ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<ResourceTag>(reader.tag())) {
      case ResourceTag::Attributes:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case ResourceTag::DroppedAttributesCount:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeScope(pbf_reader reader, InstrumentationScope& out,
                 ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<InstrumentationScopeTag>(reader.tag())) {
      case InstrumentationScopeTag::Name:
        out.name = Text(reader);
        break;
      case InstrumentationScopeTag::Version:
        out.version = Text(reader);
        break;
      case InstrumentationScopeTag::Attributes:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case InstrumentationScopeTag::DroppedAttributesCount:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeLogRecord(pbf_reader reader, LogRecord& out, ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<LogRecordTag>(reader.tag())) {
      case LogRecordTag::TimeUnixNano:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case LogRecordTag::SeverityNumber:
        out.severity_number = static_cast<SeverityNumber>(reader.get_enum());
        break;
      case LogRecordTag::SeverityText:
        out.severity_text = Text(reader);
        break;
      case LogRecordTag::Body:
        out.body = DecodeAnyValue(reader.get_message(), arena);
        break;
      case LogRecordTag::Attributes:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case LogRecordTag::DroppedAttributesCount:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      case LogRecordTag::Flags:
        out.flags = reader.get_fixed32();
        break;
      case LogRecordTag::TraceId:
        out.trace_id.hex = ReadHexId(reader);
        break;
      case LogRecordTag::SpanId:
        out.span_id.hex = ReadHexId(reader);
        break;
      case LogRecordTag::ObservedTimeUnixNano:
        out.observed_time_unix_nano = reader.get_fixed64();
        break;
      case LogRecordTag::EventName:
        out.event_name = Text(reader);
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeSpanEvent(pbf_reader reader, SpanEvent& out, ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<SpanEventTag>(reader.tag())) {
      case SpanEventTag::TimeUnixNano:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case SpanEventTag::Name:
        out.name = Text(reader);
        break;
      case SpanEventTag::Attributes:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case SpanEventTag::DroppedAttributesCount:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeSpanLink(pbf_reader reader, SpanLink& out, ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<SpanLinkTag>(reader.tag())) {
      case SpanLinkTag::TraceId:
        out.trace_id.hex = ReadHexId(reader);
        break;
      case SpanLinkTag::SpanId:
        out.span_id.hex = ReadHexId(reader);
        break;
      case SpanLinkTag::TraceState:
        out.trace_state = Text(reader);
        break;
      case SpanLinkTag::Attributes:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case SpanLinkTag::DroppedAttributesCount:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      case SpanLinkTag::Flags:
        out.flags = reader.get_fixed32();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeStatus(pbf_reader reader, Status& out) {
  while (reader.next()) {
    switch (static_cast<StatusTag>(reader.tag())) {
      case StatusTag::Message:
        out.message = Text(reader);
        break;
      case StatusTag::Code:
        out.code = static_cast<StatusCode>(reader.get_enum());
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeSpan(pbf_reader reader, Span& out, ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<SpanTag>(reader.tag())) {
      case SpanTag::TraceId:
        out.trace_id.hex = ReadHexId(reader);
        break;
      case SpanTag::SpanId:
        out.span_id.hex = ReadHexId(reader);
        break;
      case SpanTag::TraceState:
        out.trace_state = Text(reader);
        break;
      case SpanTag::ParentSpanId:
        out.parent_span_id.hex = ReadHexId(reader);
        break;
      case SpanTag::Name:
        out.name = Text(reader);
        break;
      case SpanTag::Kind:
        out.kind = static_cast<SpanKind>(reader.get_enum());
        break;
      case SpanTag::StartTimeUnixNano:
        out.start_time_unix_nano = reader.get_fixed64();
        break;
      case SpanTag::EndTimeUnixNano:
        out.end_time_unix_nano = reader.get_fixed64();
        break;
      case SpanTag::Attributes:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case SpanTag::DroppedAttributesCount:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      case SpanTag::Events:
        DecodeSpanEvent(reader.get_message(), out.events.emplace_back(), arena);
        break;
      case SpanTag::DroppedEventsCount:
        out.dropped_events_count = reader.get_uint32();
        break;
      case SpanTag::Links:
        DecodeSpanLink(reader.get_message(), out.links.emplace_back(), arena);
        break;
      case SpanTag::DroppedLinksCount:
        out.dropped_links_count = reader.get_uint32();
        break;
      case SpanTag::Status:
        DecodeStatus(reader.get_message(), out.status);
        break;
      case SpanTag::Flags:
        out.flags = reader.get_fixed32();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeExemplar(pbf_reader reader, Exemplar& out, ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<ExemplarTag>(reader.tag())) {
      case ExemplarTag::TimeUnixNano:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case ExemplarTag::AsDouble:
        out.value = reader.get_double();
        break;
      case ExemplarTag::SpanId:
        out.span_id.hex = ReadHexId(reader);
        break;
      case ExemplarTag::TraceId:
        out.trace_id.hex = ReadHexId(reader);
        break;
      case ExemplarTag::AsInt:
        out.value = reader.get_sfixed64();
        break;
      case ExemplarTag::FilteredAttributes:
        DecodeKeyValue(reader.get_message(),
                       out.filtered_attributes.emplace_back(), arena);
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeNumberDataPoint(pbf_reader reader, NumberDataPoint& out,
                           ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<NumberDataPointTag>(reader.tag())) {
      case NumberDataPointTag::StartTimeUnixNano:
        out.start_time_unix_nano = reader.get_fixed64();
        break;
      case NumberDataPointTag::TimeUnixNano:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case NumberDataPointTag::AsDouble:
        out.value = reader.get_double();
        break;
      case NumberDataPointTag::Exemplars:
        DecodeExemplar(reader.get_message(), out.exemplars.emplace_back(),
                       arena);
        break;
      case NumberDataPointTag::AsInt:
        out.value = reader.get_sfixed64();
        break;
      case NumberDataPointTag::Attributes:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case NumberDataPointTag::Flags:
        out.flags = reader.get_uint32();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeHistogramDataPoint(pbf_reader reader, HistogramDataPoint& out,
                              ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<HistogramDataPointTag>(reader.tag())) {
      case HistogramDataPointTag::StartTimeUnixNano:
        out.start_time_unix_nano = reader.get_fixed64();
        break;
      case HistogramDataPointTag::TimeUnixNano:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case HistogramDataPointTag::Count:
        out.count = reader.get_fixed64();
        break;
      case HistogramDataPointTag::Sum:
        out.sum = reader.get_double();
        break;
      case HistogramDataPointTag::BucketCounts:
        ReadFixed64s(reader, out.bucket_counts);
        break;
      case HistogramDataPointTag::ExplicitBounds:
        ReadDoubles(reader, out.explicit_bounds);
        break;
      case HistogramDataPointTag::Exemplars:
        DecodeExemplar(reader.get_message(), out.exemplars.emplace_back(),
                       arena);
        break;
      case HistogramDataPointTag::Attributes:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case HistogramDataPointTag::Flags:
        out.flags = reader.get_uint32();
        break;
      case HistogramDataPointTag::Min:
        out.min = reader.get_double();
        break;
      case HistogramDataPointTag::Max:
        out.max = reader.get_double();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeExponentialBuckets(pbf_reader reader,
                              ExponentialHistogramBuckets& out) {
  while (reader.next()) {
    switch (static_cast<ExponentialHistogramBucketsTag>(reader.tag())) {
      case ExponentialHistogramBucketsTag::Offset:
        out.offset = reader.get_sint32();
        break;
      case ExponentialHistogramBucketsTag::BucketCounts:
        ReadUint64s(reader, out.bucket_counts);
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeExponentialHistogramDataPoint(pbf_reader reader,
                                         ExponentialHistogramDataPoint& out,
                                         ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<ExponentialHistogramDataPointTag>(reader.tag())) {
      case ExponentialHistogramDataPointTag::Attributes:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case ExponentialHistogramDataPointTag::StartTimeUnixNano:
        out.start_time_unix_nano = reader.get_fixed64();
        break;
      case ExponentialHistogramDataPointTag::TimeUnixNano:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case ExponentialHistogramDataPointTag::Count:
        out.count = reader.get_fixed64();
        break;
      case ExponentialHistogramDataPointTag::Sum:
        out.sum = reader.get_double();
        break;
      case ExponentialHistogramDataPointTag::Scale:
        out.scale = reader.get_sint32();
        break;
      case ExponentialHistogramDataPointTag::ZeroCount:
        out.zero_count = reader.get_fixed64();
        break;
      case ExponentialHistogramDataPointTag::Positive:
        DecodeExponentialBuckets(reader.get_message(), out.positive);
        break;
      case ExponentialHistogramDataPointTag::Negative:
        DecodeExponentialBuckets(reader.get_message(), out.negative);
        break;
      case ExponentialHistogramDataPointTag::Flags:
        out.flags = reader.get_uint32();
        break;
      case ExponentialHistogramDataPointTag::Exemplars:
        DecodeExemplar(reader.get_message(), out.exemplars.emplace_back(),
                       arena);
        break;
      case ExponentialHistogramDataPointTag::Min:
        out.min = reader.get_double();
        break;
      case ExponentialHistogramDataPointTag::Max:
        out.max = reader.get_double();
        break;
      case ExponentialHistogramDataPointTag::ZeroThreshold:
        out.zero_threshold = reader.get_double();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeSummaryDataPoint(pbf_reader reader, SummaryDataPoint& out,
                            ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<SummaryDataPointTag>(reader.tag())) {
      case SummaryDataPointTag::StartTimeUnixNano:
        out.start_time_unix_nano = reader.get_fixed64();
        break;
      case SummaryDataPointTag::TimeUnixNano:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case SummaryDataPointTag::Count:
        out.count = reader.get_fixed64();
        break;
      case SummaryDataPointTag::Sum:
        out.sum = reader.get_double();
        break;
      case SummaryDataPointTag::QuantileValues: {
        auto& quantile = out.quantile_values.emplace_back();
        pbf_reader inner = reader.get_message();
        while (inner.next()) {
          switch (static_cast<SummaryQuantileValueTag>(inner.tag())) {
            case SummaryQuantileValueTag::Quantile:
              quantile.quantile = inner.get_double();
              break;
            case SummaryQuantileValueTag::Value:
              quantile.value = inner.get_double();
              break;
            default:
              inner.skip();
          }
        }
        break;
      }
      case SummaryDataPointTag::Attributes:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case SummaryDataPointTag::Flags:
        out.flags = reader.get_uint32();
        break;
      default:
        reader.skip();
    }
  }
}

template<typename Point, typename DecodeOne>
void DecodePoints(pbf_reader reader, std::vector<Point>& points,
                  AggregationTemporality* temporality, bool* is_monotonic,
                  ValueArena& arena, DecodeOne decode_one) {
  while (reader.next()) {
    if (reader.tag() == Tag(MetricShapeTag::DataPoints)) {
      decode_one(reader.get_message(), points.emplace_back(), arena);
    } else if (reader.tag() == Tag(MetricShapeTag::AggregationTemporality) &&
               temporality != nullptr) {
      *temporality = static_cast<AggregationTemporality>(reader.get_enum());
    } else if (reader.tag() == Tag(MetricShapeTag::IsMonotonic) &&
               is_monotonic != nullptr) {
      *is_monotonic = reader.get_bool();
    } else {
      reader.skip();
    }
  }
}

void DecodeMetric(pbf_reader reader, Metric& out, ValueArena& arena) {
  while (reader.next()) {
    switch (static_cast<MetricTag>(reader.tag())) {
      case MetricTag::Name:
        out.name = Text(reader);
        break;
      case MetricTag::Description:
        out.description = Text(reader);
        break;
      case MetricTag::Unit:
        out.unit = Text(reader);
        break;
      case MetricTag::Gauge: {
        Gauge gauge;
        DecodePoints(reader.get_message(), gauge.data_points, nullptr, nullptr,
                     arena, DecodeNumberDataPoint);
        out.data = std::move(gauge);
        break;
      }
      case MetricTag::Sum: {
        Sum sum;
        DecodePoints(reader.get_message(), sum.data_points,
                     &sum.aggregation_temporality, &sum.is_monotonic, arena,
                     DecodeNumberDataPoint);
        out.data = std::move(sum);
        break;
      }
      case MetricTag::Histogram: {
        Histogram histogram;
        DecodePoints(reader.get_message(), histogram.data_points,
                     &histogram.aggregation_temporality, nullptr, arena,
                     DecodeHistogramDataPoint);
        out.data = std::move(histogram);
        break;
      }
      case MetricTag::ExponentialHistogram: {
        ExponentialHistogram exponential;
        DecodePoints(reader.get_message(), exponential.data_points,
                     &exponential.aggregation_temporality, nullptr, arena,
                     DecodeExponentialHistogramDataPoint);
        out.data = std::move(exponential);
        break;
      }
      case MetricTag::Summary: {
        Summary summary;
        DecodePoints(reader.get_message(), summary.data_points, nullptr,
                     nullptr, arena, DecodeSummaryDataPoint);
        out.data = std::move(summary);
        break;
      }
      case MetricTag::Metadata:
        DecodeKeyValue(reader.get_message(), out.metadata.emplace_back(),
                       arena);
        break;
      default:
        reader.skip();
    }
  }
}

template<typename Record, typename DecodeOne>
void DecodeRequest(std::string_view wire, ExportRequest<Record>& out,
                   DecodeOne decode_one) {
  pbf_reader reader{wire.data(), wire.size()};
  while (reader.next()) {
    if (reader.tag() != Tag(ExportRequestTag::ResourceRecords)) {
      reader.skip();
      continue;
    }
    auto& resource_records = out.resources.emplace_back();
    pbf_reader resource_reader = reader.get_message();
    while (resource_reader.next()) {
      switch (static_cast<ResourceRecordsTag>(resource_reader.tag())) {
        case ResourceRecordsTag::Resource:
          DecodeResource(resource_reader.get_message(),
                         resource_records.resource, out.arena);
          break;
        case ResourceRecordsTag::ScopeRecords: {
          auto& scope_records = resource_records.scopes.emplace_back();
          pbf_reader scope_reader = resource_reader.get_message();
          while (scope_reader.next()) {
            switch (static_cast<ScopeRecordsTag>(scope_reader.tag())) {
              case ScopeRecordsTag::Scope:
                DecodeScope(scope_reader.get_message(), scope_records.scope,
                            out.arena);
                break;
              case ScopeRecordsTag::Records:
                decode_one(scope_reader.get_message(),
                           scope_records.records.emplace_back(), out.arena);
                break;
              case ScopeRecordsTag::SchemaUrl:
                scope_records.schema_url = Text(scope_reader);
                break;
              default:
                scope_reader.skip();
            }
          }
          break;
        }
        case ResourceRecordsTag::SchemaUrl:
          resource_records.schema_url = Text(resource_reader);
          break;
        default:
          resource_reader.skip();
      }
    }
  }
}

template<typename Request, typename DecodeOne>
void Decode(std::string_view wire, Request& out, std::string_view what,
            DecodeOne decode_one) {
  try {
    DecodeRequest(wire, out, decode_one);
  } catch (const protozero::exception& error) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("OTLP/protobuf: malformed ", what, ": ", error.what()));
  }
}

}  // namespace

void DecodeLogsRequest(std::string_view wire, ExportLogsRequest& out) {
  Decode(wire, out, "ExportLogsServiceRequest", DecodeLogRecord);
}

void DecodeTracesRequest(std::string_view wire, ExportTracesRequest& out) {
  Decode(wire, out, "ExportTraceServiceRequest", DecodeSpan);
}

void DecodeMetricsRequest(std::string_view wire, ExportMetricsRequest& out) {
  Decode(wire, out, "ExportMetricsServiceRequest", DecodeMetric);
}

}  // namespace sdb::otel
