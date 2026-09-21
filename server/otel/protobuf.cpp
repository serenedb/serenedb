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
#include <vector>

// Wire format:      https://protobuf.dev/programming-guides/encoding/
// Message schemas:  https://github.com/open-telemetry/opentelemetry-proto
namespace sdb::otel {
namespace {

using protozero::pbf_reader;
using protozero::pbf_wire_type;

std::string_view View(protozero::data_view view) {
  return {view.data(), view.size()};
}

std::string Text(pbf_reader& reader) {
  return std::string{View(reader.get_view())};
}

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
    switch (reader.tag()) {
      case 1:
        out.key = Text(reader);
        break;
      case 2:
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
    if (reader.tag() == 1) {
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
    switch (reader.tag()) {
      case 1:
        out->value = Text(reader);
        break;
      case 2:
        out->value = reader.get_bool();
        break;
      case 3:
        out->value = reader.get_int64();
        break;
      case 4:
        out->value = reader.get_double();
        break;
      case 5: {
        ArrayValue array;
        pbf_reader values = reader.get_message();
        while (values.next()) {
          if (values.tag() == 1) {
            array.values.push_back(DecodeAnyValue(values.get_message(), arena));
          } else {
            values.skip();
          }
        }
        out->value = std::move(array);
        break;
      }
      case 6:
        out->value = KvlistValue{
          .values = DecodeKeyValueList(reader.get_message(), arena)};
        break;
      case 7:
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
    switch (reader.tag()) {
      case 1:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case 2:
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
    switch (reader.tag()) {
      case 1:
        out.name = Text(reader);
        break;
      case 2:
        out.version = Text(reader);
        break;
      case 3:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case 4:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeLogRecord(pbf_reader reader, LogRecord& out, ValueArena& arena) {
  while (reader.next()) {
    switch (reader.tag()) {
      case 1:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case 2:
        out.severity_number = static_cast<SeverityNumber>(reader.get_enum());
        break;
      case 3:
        out.severity_text = Text(reader);
        break;
      case 5:
        out.body = DecodeAnyValue(reader.get_message(), arena);
        break;
      case 6:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case 7:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      case 8:
        out.flags = reader.get_fixed32();
        break;
      case 9:
        out.trace_id.hex = ReadHexId(reader);
        break;
      case 10:
        out.span_id.hex = ReadHexId(reader);
        break;
      case 11:
        out.observed_time_unix_nano = reader.get_fixed64();
        break;
      case 12:
        out.event_name = Text(reader);
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeSpanEvent(pbf_reader reader, SpanEvent& out, ValueArena& arena) {
  while (reader.next()) {
    switch (reader.tag()) {
      case 1:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case 2:
        out.name = Text(reader);
        break;
      case 3:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case 4:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeSpanLink(pbf_reader reader, SpanLink& out, ValueArena& arena) {
  while (reader.next()) {
    switch (reader.tag()) {
      case 1:
        out.trace_id.hex = ReadHexId(reader);
        break;
      case 2:
        out.span_id.hex = ReadHexId(reader);
        break;
      case 3:
        out.trace_state = Text(reader);
        break;
      case 4:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case 5:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      case 6:
        out.flags = reader.get_fixed32();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeStatus(pbf_reader reader, Status& out) {
  while (reader.next()) {
    switch (reader.tag()) {
      case 2:
        out.message = Text(reader);
        break;
      case 3:
        out.code = static_cast<StatusCode>(reader.get_enum());
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeSpan(pbf_reader reader, Span& out, ValueArena& arena) {
  while (reader.next()) {
    switch (reader.tag()) {
      case 1:
        out.trace_id.hex = ReadHexId(reader);
        break;
      case 2:
        out.span_id.hex = ReadHexId(reader);
        break;
      case 3:
        out.trace_state = Text(reader);
        break;
      case 4:
        out.parent_span_id.hex = ReadHexId(reader);
        break;
      case 5:
        out.name = Text(reader);
        break;
      case 6:
        out.kind = static_cast<SpanKind>(reader.get_enum());
        break;
      case 7:
        out.start_time_unix_nano = reader.get_fixed64();
        break;
      case 8:
        out.end_time_unix_nano = reader.get_fixed64();
        break;
      case 9:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case 10:
        out.dropped_attributes_count = reader.get_uint32();
        break;
      case 11:
        DecodeSpanEvent(reader.get_message(), out.events.emplace_back(), arena);
        break;
      case 12:
        out.dropped_events_count = reader.get_uint32();
        break;
      case 13:
        DecodeSpanLink(reader.get_message(), out.links.emplace_back(), arena);
        break;
      case 14:
        out.dropped_links_count = reader.get_uint32();
        break;
      case 15:
        DecodeStatus(reader.get_message(), out.status);
        break;
      case 16:
        out.flags = reader.get_fixed32();
        break;
      default:
        reader.skip();
    }
  }
}

void DecodeExemplar(pbf_reader reader, Exemplar& out, ValueArena& arena) {
  while (reader.next()) {
    switch (reader.tag()) {
      case 2:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case 3:
        out.value = reader.get_double();
        break;
      case 4:
        out.span_id.hex = ReadHexId(reader);
        break;
      case 5:
        out.trace_id.hex = ReadHexId(reader);
        break;
      case 6:
        out.value = reader.get_sfixed64();
        break;
      case 7:
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
    switch (reader.tag()) {
      case 2:
        out.start_time_unix_nano = reader.get_fixed64();
        break;
      case 3:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case 4:
        out.value = reader.get_double();
        break;
      case 5:
        DecodeExemplar(reader.get_message(), out.exemplars.emplace_back(),
                       arena);
        break;
      case 6:
        out.value = reader.get_sfixed64();
        break;
      case 7:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case 8:
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
    switch (reader.tag()) {
      case 2:
        out.start_time_unix_nano = reader.get_fixed64();
        break;
      case 3:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case 4:
        out.count = reader.get_fixed64();
        break;
      case 5:
        out.sum = reader.get_double();
        break;
      case 6:
        ReadFixed64s(reader, out.bucket_counts);
        break;
      case 7:
        ReadDoubles(reader, out.explicit_bounds);
        break;
      case 8:
        DecodeExemplar(reader.get_message(), out.exemplars.emplace_back(),
                       arena);
        break;
      case 9:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case 10:
        out.flags = reader.get_uint32();
        break;
      case 11:
        out.min = reader.get_double();
        break;
      case 12:
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
    switch (reader.tag()) {
      case 1:
        out.offset = reader.get_sint32();
        break;
      case 2:
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
    switch (reader.tag()) {
      case 1:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case 2:
        out.start_time_unix_nano = reader.get_fixed64();
        break;
      case 3:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case 4:
        out.count = reader.get_fixed64();
        break;
      case 5:
        out.sum = reader.get_double();
        break;
      case 6:
        out.scale = reader.get_sint32();
        break;
      case 7:
        out.zero_count = reader.get_fixed64();
        break;
      case 8:
        DecodeExponentialBuckets(reader.get_message(), out.positive);
        break;
      case 9:
        DecodeExponentialBuckets(reader.get_message(), out.negative);
        break;
      case 10:
        out.flags = reader.get_uint32();
        break;
      case 11:
        DecodeExemplar(reader.get_message(), out.exemplars.emplace_back(),
                       arena);
        break;
      case 12:
        out.min = reader.get_double();
        break;
      case 13:
        out.max = reader.get_double();
        break;
      case 14:
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
    switch (reader.tag()) {
      case 2:
        out.start_time_unix_nano = reader.get_fixed64();
        break;
      case 3:
        out.time_unix_nano = reader.get_fixed64();
        break;
      case 4:
        out.count = reader.get_fixed64();
        break;
      case 5:
        out.sum = reader.get_double();
        break;
      case 6: {
        auto& quantile = out.quantile_values.emplace_back();
        pbf_reader inner = reader.get_message();
        while (inner.next()) {
          switch (inner.tag()) {
            case 1:
              quantile.quantile = inner.get_double();
              break;
            case 2:
              quantile.value = inner.get_double();
              break;
            default:
              inner.skip();
          }
        }
        break;
      }
      case 7:
        DecodeKeyValue(reader.get_message(), out.attributes.emplace_back(),
                       arena);
        break;
      case 8:
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
    if (reader.tag() == 1) {
      decode_one(reader.get_message(), points.emplace_back(), arena);
    } else if (reader.tag() == 2 && temporality != nullptr) {
      *temporality = static_cast<AggregationTemporality>(reader.get_enum());
    } else if (reader.tag() == 3 && is_monotonic != nullptr) {
      *is_monotonic = reader.get_bool();
    } else {
      reader.skip();
    }
  }
}

void DecodeMetric(pbf_reader reader, Metric& out, ValueArena& arena) {
  while (reader.next()) {
    switch (reader.tag()) {
      case 1:
        out.name = Text(reader);
        break;
      case 2:
        out.description = Text(reader);
        break;
      case 3:
        out.unit = Text(reader);
        break;
      case 5: {
        Gauge gauge;
        DecodePoints(reader.get_message(), gauge.data_points, nullptr, nullptr,
                     arena, DecodeNumberDataPoint);
        out.data = std::move(gauge);
        break;
      }
      case 7: {
        Sum sum;
        DecodePoints(reader.get_message(), sum.data_points,
                     &sum.aggregation_temporality, &sum.is_monotonic, arena,
                     DecodeNumberDataPoint);
        out.data = std::move(sum);
        break;
      }
      case 9: {
        Histogram histogram;
        DecodePoints(reader.get_message(), histogram.data_points,
                     &histogram.aggregation_temporality, nullptr, arena,
                     DecodeHistogramDataPoint);
        out.data = std::move(histogram);
        break;
      }
      case 10: {
        ExponentialHistogram exponential;
        DecodePoints(reader.get_message(), exponential.data_points,
                     &exponential.aggregation_temporality, nullptr, arena,
                     DecodeExponentialHistogramDataPoint);
        out.data = std::move(exponential);
        break;
      }
      case 11: {
        Summary summary;
        DecodePoints(reader.get_message(), summary.data_points, nullptr,
                     nullptr, arena, DecodeSummaryDataPoint);
        out.data = std::move(summary);
        break;
      }
      case 12:
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
    if (reader.tag() != 1) {
      reader.skip();
      continue;
    }
    auto& resource_records = out.resources.emplace_back();
    pbf_reader resource_reader = reader.get_message();
    while (resource_reader.next()) {
      switch (resource_reader.tag()) {
        case 1:
          DecodeResource(resource_reader.get_message(),
                         resource_records.resource, out.arena);
          break;
        case 2: {
          auto& scope_records = resource_records.scopes.emplace_back();
          pbf_reader scope_reader = resource_reader.get_message();
          while (scope_reader.next()) {
            switch (scope_reader.tag()) {
              case 1:
                DecodeScope(scope_reader.get_message(), scope_records.scope,
                            out.arena);
                break;
              case 2:
                decode_one(scope_reader.get_message(),
                           scope_records.records.emplace_back(), out.arena);
                break;
              case 3:
                scope_records.schema_url = Text(scope_reader);
                break;
              default:
                scope_reader.skip();
            }
          }
          break;
        }
        case 3:
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
