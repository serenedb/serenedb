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
#include <cstring>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <vector>

namespace sdb::otel {
namespace {

enum class WireType : uint32_t {
  Varint = 0,
  Fixed64 = 1,
  LengthDelimited = 2,
  StartGroup = 3,
  EndGroup = 4,
  Fixed32 = 5,
};

[[noreturn]] void Throw(std::string_view detail) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                  ERR_MSG("OTLP/protobuf: ", detail));
}

class Reader {
 public:
  explicit Reader(std::string_view data) : _data{data} {}

  bool Done() const { return _pos >= _data.size(); }

  uint64_t Varint() {
    uint64_t value = 0;
    uint32_t shift = 0;
    while (true) {
      if (_pos >= _data.size()) {
        Throw("truncated varint");
      }
      const auto byte = static_cast<uint8_t>(_data[_pos++]);
      if (shift == 63 && (byte & 0xFE) != 0) {
        Throw("varint overflows 64 bits");
      }
      value |= static_cast<uint64_t>(byte & 0x7F) << shift;
      if ((byte & 0x80) == 0) {
        return value;
      }
      shift += 7;
      if (shift > 63) {
        Throw("varint overflows 64 bits");
      }
    }
  }

  uint64_t Fixed64() {
    if (_pos + 8 > _data.size()) {
      Throw("truncated fixed64");
    }
    uint64_t value = 0;
    std::memcpy(&value, _data.data() + _pos, 8);
    _pos += 8;
    return value;
  }

  uint32_t Fixed32() {
    if (_pos + 4 > _data.size()) {
      Throw("truncated fixed32");
    }
    uint32_t value = 0;
    std::memcpy(&value, _data.data() + _pos, 4);
    _pos += 4;
    return value;
  }

  std::string_view Bytes() {
    const auto length = Varint();
    if (length > _data.size() - _pos) {
      Throw("length-delimited field runs past the end of the message");
    }
    const auto view = _data.substr(_pos, length);
    _pos += length;
    return view;
  }

  // Returns false at the end of the message; otherwise fills the tag.
  bool NextTag(uint32_t& field, WireType& type) {
    if (Done()) {
      return false;
    }
    const auto tag = Varint();
    field = static_cast<uint32_t>(tag >> 3);
    type = static_cast<WireType>(tag & 0x7);
    if (field == 0) {
      Throw("field number 0 is not valid");
    }
    return true;
  }

  // Unknown fields are skipped rather than rejected: that is what keeps a
  // newer sender compatible with this decoder.
  void Skip(WireType type) {
    switch (type) {
      case WireType::Varint:
        Varint();
        return;
      case WireType::Fixed64:
        Fixed64();
        return;
      case WireType::LengthDelimited:
        Bytes();
        return;
      case WireType::Fixed32:
        Fixed32();
        return;
      case WireType::StartGroup:
      case WireType::EndGroup:
        Throw("groups are not part of the OTLP schema");
    }
    Throw("unknown wire type");
  }

 private:
  std::string_view _data;
  size_t _pos = 0;
};

// sint32/sint64 are zigzag-encoded: the sign lives in the low bit, so a plain
// varint read turns -2 into 3 and 2 into 4.
int32_t ZigZag32(uint64_t encoded) {
  return static_cast<int32_t>((encoded >> 1) ^ (~(encoded & 1) + 1));
}

double AsDouble(uint64_t bits) {
  double value = 0;
  std::memcpy(&value, &bits, sizeof value);
  return value;
}

std::string HexId(std::string_view raw, size_t width) {
  if (raw.empty()) {
    return {};
  }
  if (raw.size() != width) {
    Throw("trace or span id has the wrong width");
  }
  if (raw.find_first_not_of('\0') == std::string_view::npos) {
    return {};
  }
  return absl::BytesToHexString(raw);
}

// A repeated numeric field may arrive packed (one length-delimited run) or
// unpacked (one tag per element); both encodings are legal for the same field.
template<typename T, typename ReadOne>
void ReadRepeated(Reader& reader, WireType type, std::vector<T>& out,
                  ReadOne read_one) {
  if (type != WireType::LengthDelimited) {
    out.push_back(read_one(reader));
    return;
  }
  Reader packed{reader.Bytes()};
  while (!packed.Done()) {
    out.push_back(read_one(packed));
  }
}

uint64_t ReadVarint64(Reader& reader) { return reader.Varint(); }

double ReadDouble(Reader& reader) { return AsDouble(reader.Fixed64()); }

void DecodeAnyValue(std::string_view wire, AnyValue& out, ValueArena& arena);

void DecodeKeyValue(std::string_view wire, KeyValue& out, ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        out.key = std::string{reader.Bytes()};
        break;
      case 2:
        out.value = arena.Make();
        DecodeAnyValue(reader.Bytes(), *out.value, arena);
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

KeyValueList DecodeKeyValueList(std::string_view wire, ValueArena& arena) {
  KeyValueList out;
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    if (field == 1) {
      DecodeKeyValue(reader.Bytes(), out.emplace_back(), arena);
    } else {
      reader.Skip(type);
    }
  }
  return out;
}

void DecodeAnyValue(std::string_view wire, AnyValue& out, ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        out.value = std::string{reader.Bytes()};
        break;
      case 2:
        out.value = reader.Varint() != 0;
        break;
      case 3:
        out.value = static_cast<int64_t>(reader.Varint());
        break;
      case 4:
        out.value = AsDouble(reader.Fixed64());
        break;
      case 5: {
        ArrayValue array;
        Reader values{reader.Bytes()};
        uint32_t inner_field = 0;
        WireType inner_type{};
        while (values.NextTag(inner_field, inner_type)) {
          if (inner_field == 1) {
            auto* element = arena.Make();
            DecodeAnyValue(values.Bytes(), *element, arena);
            array.values.push_back(element);
          } else {
            values.Skip(inner_type);
          }
        }
        out.value = std::move(array);
        break;
      }
      case 6:
        out.value =
          KvlistValue{.values = DecodeKeyValueList(reader.Bytes(), arena)};
        break;
      case 7:
        out.value = BytesValue{.data = absl::Base64Escape(reader.Bytes())};
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeResource(std::string_view wire, Resource& out, ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        DecodeKeyValue(reader.Bytes(), out.attributes.emplace_back(), arena);
        break;
      case 2:
        out.dropped_attributes_count = static_cast<uint32_t>(reader.Varint());
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeScope(std::string_view wire, InstrumentationScope& out,
                 ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        out.name = std::string{reader.Bytes()};
        break;
      case 2:
        out.version = std::string{reader.Bytes()};
        break;
      case 3:
        DecodeKeyValue(reader.Bytes(), out.attributes.emplace_back(), arena);
        break;
      case 4:
        out.dropped_attributes_count = static_cast<uint32_t>(reader.Varint());
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeLogRecord(std::string_view wire, LogRecord& out, ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        out.time_unix_nano = reader.Fixed64();
        break;
      case 2:
        out.severity_number = static_cast<int32_t>(reader.Varint());
        break;
      case 3:
        out.severity_text = std::string{reader.Bytes()};
        break;
      case 5:
        out.body = arena.Make();
        DecodeAnyValue(reader.Bytes(), *out.body, arena);
        break;
      case 6:
        DecodeKeyValue(reader.Bytes(), out.attributes.emplace_back(), arena);
        break;
      case 7:
        out.dropped_attributes_count = static_cast<uint32_t>(reader.Varint());
        break;
      case 8:
        out.flags = static_cast<uint32_t>(reader.Fixed32());
        break;
      case 9:
        out.trace_id = HexId(reader.Bytes(), 16);
        break;
      case 10:
        out.span_id = HexId(reader.Bytes(), 8);
        break;
      case 11:
        out.observed_time_unix_nano = reader.Fixed64();
        break;
      case 12:
        out.event_name = std::string{reader.Bytes()};
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeSpanEvent(std::string_view wire, SpanEvent& out, ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        out.time_unix_nano = reader.Fixed64();
        break;
      case 2:
        out.name = std::string{reader.Bytes()};
        break;
      case 3:
        DecodeKeyValue(reader.Bytes(), out.attributes.emplace_back(), arena);
        break;
      case 4:
        out.dropped_attributes_count = static_cast<uint32_t>(reader.Varint());
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeSpanLink(std::string_view wire, SpanLink& out, ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        out.trace_id = HexId(reader.Bytes(), 16);
        break;
      case 2:
        out.span_id = HexId(reader.Bytes(), 8);
        break;
      case 3:
        out.trace_state = std::string{reader.Bytes()};
        break;
      case 4:
        DecodeKeyValue(reader.Bytes(), out.attributes.emplace_back(), arena);
        break;
      case 5:
        out.dropped_attributes_count = static_cast<uint32_t>(reader.Varint());
        break;
      case 6:
        out.flags = static_cast<uint32_t>(reader.Fixed32());
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeStatus(std::string_view wire, Status& out) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 2:
        out.message = std::string{reader.Bytes()};
        break;
      case 3:
        out.code = static_cast<StatusCode>(reader.Varint());
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeSpan(std::string_view wire, Span& out, ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        out.trace_id = HexId(reader.Bytes(), 16);
        break;
      case 2:
        out.span_id = HexId(reader.Bytes(), 8);
        break;
      case 3:
        out.trace_state = std::string{reader.Bytes()};
        break;
      case 4:
        out.parent_span_id = HexId(reader.Bytes(), 8);
        break;
      case 5:
        out.name = std::string{reader.Bytes()};
        break;
      case 6:
        out.kind = static_cast<SpanKind>(reader.Varint());
        break;
      case 7:
        out.start_time_unix_nano = reader.Fixed64();
        break;
      case 8:
        out.end_time_unix_nano = reader.Fixed64();
        break;
      case 9:
        DecodeKeyValue(reader.Bytes(), out.attributes.emplace_back(), arena);
        break;
      case 10:
        out.dropped_attributes_count = static_cast<uint32_t>(reader.Varint());
        break;
      case 11:
        DecodeSpanEvent(reader.Bytes(), out.events.emplace_back(), arena);
        break;
      case 12:
        out.dropped_events_count = static_cast<uint32_t>(reader.Varint());
        break;
      case 13:
        DecodeSpanLink(reader.Bytes(), out.links.emplace_back(), arena);
        break;
      case 14:
        out.dropped_links_count = static_cast<uint32_t>(reader.Varint());
        break;
      case 15:
        DecodeStatus(reader.Bytes(), out.status);
        break;
      case 16:
        out.flags = static_cast<uint32_t>(reader.Fixed32());
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeExemplar(std::string_view wire, Exemplar& out, ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 2:
        out.time_unix_nano = reader.Fixed64();
        break;
      case 3:
        out.value = AsDouble(reader.Fixed64());
        break;
      case 4:
        out.span_id = HexId(reader.Bytes(), 8);
        break;
      case 5:
        out.trace_id = HexId(reader.Bytes(), 16);
        break;
      case 6:
        out.value = static_cast<int64_t>(reader.Fixed64());
        break;
      case 7:
        DecodeKeyValue(reader.Bytes(), out.filtered_attributes.emplace_back(),
                       arena);
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeNumberDataPoint(std::string_view wire, NumberDataPoint& out,
                           ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 2:
        out.start_time_unix_nano = reader.Fixed64();
        break;
      case 3:
        out.time_unix_nano = reader.Fixed64();
        break;
      case 4:
        out.value = AsDouble(reader.Fixed64());
        break;
      case 5:
        DecodeExemplar(reader.Bytes(), out.exemplars.emplace_back(), arena);
        break;
      case 6:
        out.value = static_cast<int64_t>(reader.Fixed64());
        break;
      case 7:
        DecodeKeyValue(reader.Bytes(), out.attributes.emplace_back(), arena);
        break;
      case 8:
        out.flags = static_cast<uint32_t>(reader.Varint());
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeHistogramDataPoint(std::string_view wire, HistogramDataPoint& out,
                              ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 2:
        out.start_time_unix_nano = reader.Fixed64();
        break;
      case 3:
        out.time_unix_nano = reader.Fixed64();
        break;
      case 4:
        out.count = reader.Fixed64();
        break;
      case 5:
        out.sum = AsDouble(reader.Fixed64());
        break;
      case 6:
        ReadRepeated(reader, type, out.bucket_counts,
                     [](Reader& r) { return r.Fixed64(); });
        break;
      case 7:
        ReadRepeated(reader, type, out.explicit_bounds, ReadDouble);
        break;
      case 8:
        DecodeExemplar(reader.Bytes(), out.exemplars.emplace_back(), arena);
        break;
      case 9:
        DecodeKeyValue(reader.Bytes(), out.attributes.emplace_back(), arena);
        break;
      case 10:
        out.flags = static_cast<uint32_t>(reader.Varint());
        break;
      case 11:
        out.min = AsDouble(reader.Fixed64());
        break;
      case 12:
        out.max = AsDouble(reader.Fixed64());
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeExponentialBuckets(std::string_view wire,
                              ExponentialHistogramBuckets& out) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        out.offset = ZigZag32(reader.Varint());
        break;
      case 2:
        ReadRepeated(reader, type, out.bucket_counts, ReadVarint64);
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeExponentialHistogramDataPoint(std::string_view wire,
                                         ExponentialHistogramDataPoint& out,
                                         ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        DecodeKeyValue(reader.Bytes(), out.attributes.emplace_back(), arena);
        break;
      case 2:
        out.start_time_unix_nano = reader.Fixed64();
        break;
      case 3:
        out.time_unix_nano = reader.Fixed64();
        break;
      case 4:
        out.count = reader.Fixed64();
        break;
      case 5:
        out.sum = AsDouble(reader.Fixed64());
        break;
      case 6:
        out.scale = ZigZag32(reader.Varint());
        break;
      case 7:
        out.zero_count = reader.Fixed64();
        break;
      case 8:
        DecodeExponentialBuckets(reader.Bytes(), out.positive);
        break;
      case 9:
        DecodeExponentialBuckets(reader.Bytes(), out.negative);
        break;
      case 10:
        out.flags = static_cast<uint32_t>(reader.Varint());
        break;
      case 11:
        DecodeExemplar(reader.Bytes(), out.exemplars.emplace_back(), arena);
        break;
      case 12:
        out.min = AsDouble(reader.Fixed64());
        break;
      case 13:
        out.max = AsDouble(reader.Fixed64());
        break;
      case 14:
        out.zero_threshold = AsDouble(reader.Fixed64());
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

void DecodeSummaryDataPoint(std::string_view wire, SummaryDataPoint& out,
                            ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 2:
        out.start_time_unix_nano = reader.Fixed64();
        break;
      case 3:
        out.time_unix_nano = reader.Fixed64();
        break;
      case 4:
        out.count = reader.Fixed64();
        break;
      case 5:
        out.sum = AsDouble(reader.Fixed64());
        break;
      case 6: {
        auto& quantile = out.quantile_values.emplace_back();
        Reader inner{reader.Bytes()};
        uint32_t inner_field = 0;
        WireType inner_type{};
        while (inner.NextTag(inner_field, inner_type)) {
          if (inner_field == 1) {
            quantile.quantile = AsDouble(inner.Fixed64());
          } else if (inner_field == 2) {
            quantile.value = AsDouble(inner.Fixed64());
          } else {
            inner.Skip(inner_type);
          }
        }
        break;
      }
      case 7:
        DecodeKeyValue(reader.Bytes(), out.attributes.emplace_back(), arena);
        break;
      case 8:
        out.flags = static_cast<uint32_t>(reader.Varint());
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

template<typename Point, typename DecodeOne>
void DecodePoints(std::string_view wire, std::vector<Point>& points,
                  AggregationTemporality* temporality, bool* is_monotonic,
                  ValueArena& arena, DecodeOne decode_one) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    if (field == 1) {
      decode_one(reader.Bytes(), points.emplace_back(), arena);
    } else if (field == 2 && temporality != nullptr) {
      *temporality = static_cast<AggregationTemporality>(reader.Varint());
    } else if (field == 3 && is_monotonic != nullptr) {
      *is_monotonic = reader.Varint() != 0;
    } else {
      reader.Skip(type);
    }
  }
}

void DecodeMetric(std::string_view wire, Metric& out, ValueArena& arena) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    switch (field) {
      case 1:
        out.name = std::string{reader.Bytes()};
        break;
      case 2:
        out.description = std::string{reader.Bytes()};
        break;
      case 3:
        out.unit = std::string{reader.Bytes()};
        break;
      case 5: {
        Gauge gauge;
        DecodePoints(reader.Bytes(), gauge.data_points, nullptr, nullptr, arena,
                     DecodeNumberDataPoint);
        out.data = std::move(gauge);
        break;
      }
      case 7: {
        Sum sum;
        DecodePoints(reader.Bytes(), sum.data_points,
                     &sum.aggregation_temporality, &sum.is_monotonic, arena,
                     DecodeNumberDataPoint);
        out.data = std::move(sum);
        break;
      }
      case 9: {
        Histogram histogram;
        DecodePoints(reader.Bytes(), histogram.data_points,
                     &histogram.aggregation_temporality, nullptr, arena,
                     DecodeHistogramDataPoint);
        out.data = std::move(histogram);
        break;
      }
      case 10: {
        ExponentialHistogram exponential;
        DecodePoints(reader.Bytes(), exponential.data_points,
                     &exponential.aggregation_temporality, nullptr, arena,
                     DecodeExponentialHistogramDataPoint);
        out.data = std::move(exponential);
        break;
      }
      case 11: {
        Summary summary;
        DecodePoints(reader.Bytes(), summary.data_points, nullptr, nullptr,
                     arena, DecodeSummaryDataPoint);
        out.data = std::move(summary);
        break;
      }
      case 12:
        DecodeKeyValue(reader.Bytes(), out.metadata.emplace_back(), arena);
        break;
      default:
        reader.Skip(type);
        break;
    }
  }
}

template<typename Record, typename DecodeOne>
void DecodeRequest(std::string_view wire, ExportRequest<Record>& out,
                   DecodeOne decode_one) {
  Reader reader{wire};
  uint32_t field = 0;
  WireType type{};
  while (reader.NextTag(field, type)) {
    if (field != 1) {
      reader.Skip(type);
      continue;
    }
    auto& resource_records = out.resources.emplace_back();
    Reader resource_reader{reader.Bytes()};
    uint32_t resource_field = 0;
    WireType resource_type{};
    while (resource_reader.NextTag(resource_field, resource_type)) {
      switch (resource_field) {
        case 1:
          DecodeResource(resource_reader.Bytes(), resource_records.resource,
                         out.arena);
          break;
        case 2: {
          auto& scope_records = resource_records.scopes.emplace_back();
          Reader scope_reader{resource_reader.Bytes()};
          uint32_t scope_field = 0;
          WireType scope_type{};
          while (scope_reader.NextTag(scope_field, scope_type)) {
            switch (scope_field) {
              case 1:
                DecodeScope(scope_reader.Bytes(), scope_records.scope,
                            out.arena);
                break;
              case 2:
                decode_one(scope_reader.Bytes(),
                           scope_records.records.emplace_back(), out.arena);
                break;
              case 3:
                scope_records.schema_url = std::string{scope_reader.Bytes()};
                break;
              default:
                scope_reader.Skip(scope_type);
                break;
            }
          }
          break;
        }
        case 3:
          resource_records.schema_url = std::string{resource_reader.Bytes()};
          break;
        default:
          resource_reader.Skip(resource_type);
          break;
      }
    }
  }
}

}  // namespace

void DecodeLogsRequest(std::string_view wire, ExportLogsRequest& out) {
  DecodeRequest<LogRecord>(wire, out, DecodeLogRecord);
}

void DecodeTracesRequest(std::string_view wire, ExportTracesRequest& out) {
  DecodeRequest<Span>(wire, out, DecodeSpan);
}

void DecodeMetricsRequest(std::string_view wire, ExportMetricsRequest& out) {
  DecodeRequest<Metric>(wire, out, DecodeMetric);
}

}  // namespace sdb::otel
