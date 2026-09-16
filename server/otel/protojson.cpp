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

#include "otel/protojson.h"

#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <simdjson.h>

#include <cstdint>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace sdb::otel {
namespace {

using Value = simdjson::ondemand::value;
using Object = simdjson::ondemand::object;

[[noreturn]] void Throw(std::string_view field, std::string_view expected) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                  ERR_MSG("OTLP/JSON: field [", field, "] must be ", expected));
}

bool NameIs(std::string_view key, std::string_view camel,
            std::string_view snake) {
  return key == camel || key == snake;
}

std::string ReadString(Value value, std::string_view field) {
  std::string_view text;
  if (value.get_string().get(text) != simdjson::SUCCESS) {
    Throw(field, "a string");
  }
  return std::string{text};
}

bool ReadBool(Value value, std::string_view field) {
  bool flag = false;
  if (value.get_bool().get(flag) == simdjson::SUCCESS) {
    return flag;
  }
  std::string_view text;
  if (value.get_string().get(text) == simdjson::SUCCESS) {
    if (text == "true") {
      return true;
    }
    if (text == "false") {
      return false;
    }
  }
  Throw(field, "a boolean");
}

// ProtoJSON writes 64-bit integers as decimal strings, but hand-written
// payloads and some SDKs send bare numbers; accept both.
uint64_t ReadUint64(Value value, std::string_view field) {
  uint64_t number = 0;
  if (value.get_uint64().get(number) == simdjson::SUCCESS) {
    return number;
  }
  std::string_view text;
  if (value.get_string().get(text) == simdjson::SUCCESS &&
      absl::SimpleAtoi(text, &number)) {
    return number;
  }
  Throw(field, "a 64-bit unsigned integer");
}

int64_t ReadInt64(Value value, std::string_view field) {
  int64_t number = 0;
  if (value.get_int64().get(number) == simdjson::SUCCESS) {
    return number;
  }
  std::string_view text;
  if (value.get_string().get(text) == simdjson::SUCCESS &&
      absl::SimpleAtoi(text, &number)) {
    return number;
  }
  Throw(field, "a 64-bit signed integer");
}

int32_t ReadInt32(Value value, std::string_view field) {
  const int64_t number = ReadInt64(value, field);
  if (number < INT32_MIN || number > INT32_MAX) {
    Throw(field, "a 32-bit signed integer");
  }
  return static_cast<int32_t>(number);
}

uint32_t ReadUint32(Value value, std::string_view field) {
  const uint64_t number = ReadUint64(value, field);
  if (number > UINT32_MAX) {
    Throw(field, "a 32-bit unsigned integer");
  }
  return static_cast<uint32_t>(number);
}

double ReadDouble(Value value, std::string_view field) {
  double number = 0;
  if (value.get_double().get(number) == simdjson::SUCCESS) {
    return number;
  }
  std::string_view text;
  if (value.get_string().get(text) == simdjson::SUCCESS) {
    if (text == "NaN") {
      return std::numeric_limits<double>::quiet_NaN();
    }
    if (text == "Infinity") {
      return std::numeric_limits<double>::infinity();
    }
    if (text == "-Infinity") {
      return -std::numeric_limits<double>::infinity();
    }
    if (absl::SimpleAtod(text, &number)) {
      return number;
    }
  }
  Throw(field, "a number");
}

// Enums arrive as the integer value or as the proto enum name; the name form
// is what `telemetrygen` and the Go collector's JSON marshaller emit.
int32_t ReadEnum(Value value, std::string_view field,
                 std::span<const std::pair<std::string_view, int32_t>> names) {
  std::string_view text;
  if (value.get_string().get(text) == simdjson::SUCCESS) {
    for (const auto& [name, number] : names) {
      if (text == name) {
        return number;
      }
    }
    int32_t parsed = 0;
    if (absl::SimpleAtoi(text, &parsed)) {
      return parsed;
    }
    Throw(field, "a known enum name or an integer");
  }
  return ReadInt32(value, field);
}

// OTLP/JSON encodes trace and span ids as lowercase hex, unlike the base64
// the generic ProtoJSON bytes rule would give.
std::string ReadId(Value value, std::string_view field, size_t hex_length) {
  auto text = ReadString(value, field);
  if (text.empty()) {
    return {};
  }
  if (text.size() != hex_length) {
    Throw(field, "a hex string of the declared width");
  }
  absl::AsciiStrToLower(&text);
  for (const char c : text) {
    const bool hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    if (!hex) {
      Throw(field, "a hex string");
    }
  }
  if (text.find_first_not_of('0') == std::string::npos) {
    return {};
  }
  return text;
}

AnyValue* ParseAnyValue(Value value, ValueArena& arena);

KeyValueList ParseKeyValueList(Value value, ValueArena& arena,
                               std::string_view field) {
  KeyValueList out;
  simdjson::ondemand::array items;
  if (value.get_array().get(items) != simdjson::SUCCESS) {
    Throw(field, "an array of key/value objects");
  }
  for (auto item : items) {
    Object entry;
    if (item.get_object().get(entry) != simdjson::SUCCESS) {
      Throw(field, "an array of key/value objects");
    }
    KeyValue kv;
    for (auto member : entry) {
      std::string_view key;
      if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
        Throw(field, "an array of key/value objects");
      }
      if (key == "key") {
        kv.key = ReadString(member.value().value(), "key");
      } else if (key == "value") {
        kv.value = ParseAnyValue(member.value().value(), arena);
      }
    }
    out.push_back(std::move(kv));
  }
  return out;
}

AnyValue* ParseAnyValue(Value value, ValueArena& arena) {
  AnyValue* out = arena.Make();
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("value", "an AnyValue object");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("value", "an AnyValue object");
    }
    auto field = member.value().value();
    if (NameIs(key, "stringValue", "string_value")) {
      out->value = ReadString(field, key);
    } else if (NameIs(key, "boolValue", "bool_value")) {
      out->value = ReadBool(field, key);
    } else if (NameIs(key, "intValue", "int_value")) {
      out->value = ReadInt64(field, key);
    } else if (NameIs(key, "doubleValue", "double_value")) {
      out->value = ReadDouble(field, key);
    } else if (NameIs(key, "bytesValue", "bytes_value")) {
      out->value = BytesValue{.data = ReadString(field, key)};
    } else if (NameIs(key, "arrayValue", "array_value")) {
      ArrayValue array;
      Object wrapper;
      if (field.get_object().get(wrapper) != simdjson::SUCCESS) {
        Throw(key, "an ArrayValue object");
      }
      for (auto inner : wrapper) {
        std::string_view inner_key;
        if (inner.unescaped_key().get(inner_key) != simdjson::SUCCESS) {
          Throw(key, "an ArrayValue object");
        }
        if (inner_key != "values") {
          continue;
        }
        simdjson::ondemand::array items;
        if (inner.value().get_array().get(items) != simdjson::SUCCESS) {
          Throw(key, "an array of AnyValue");
        }
        for (auto item : items) {
          array.values.push_back(ParseAnyValue(item.value(), arena));
        }
      }
      out->value = std::move(array);
    } else if (NameIs(key, "kvlistValue", "kvlist_value")) {
      KvlistValue kvlist;
      Object wrapper;
      if (field.get_object().get(wrapper) != simdjson::SUCCESS) {
        Throw(key, "a KeyValueList object");
      }
      for (auto inner : wrapper) {
        std::string_view inner_key;
        if (inner.unescaped_key().get(inner_key) != simdjson::SUCCESS) {
          Throw(key, "a KeyValueList object");
        }
        if (inner_key == "values") {
          kvlist.values =
            ParseKeyValueList(inner.value().value(), arena, "values");
        }
      }
      out->value = std::move(kvlist);
    }
  }
  return out;
}

void ParseResource(Value value, Resource& out, ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("resource", "an object");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("resource", "an object");
    }
    if (key == "attributes") {
      out.attributes = ParseKeyValueList(member.value().value(), arena, key);
    } else if (NameIs(key, "droppedAttributesCount",
                      "dropped_attributes_count")) {
      out.dropped_attributes_count = ReadUint32(member.value().value(), key);
    }
  }
}

void ParseScope(Value value, InstrumentationScope& out, ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("scope", "an object");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("scope", "an object");
    }
    auto field = member.value().value();
    if (key == "name") {
      out.name = ReadString(field, key);
    } else if (key == "version") {
      out.version = ReadString(field, key);
    } else if (key == "attributes") {
      out.attributes = ParseKeyValueList(field, arena, key);
    } else if (NameIs(key, "droppedAttributesCount",
                      "dropped_attributes_count")) {
      out.dropped_attributes_count = ReadUint32(field, key);
    }
  }
}

void ParseLogRecord(Value value, LogRecord& out, ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("logRecords", "an array of LogRecord objects");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("logRecords", "an array of LogRecord objects");
    }
    auto field = member.value().value();
    if (NameIs(key, "timeUnixNano", "time_unix_nano")) {
      out.time_unix_nano = ReadUint64(field, key);
    } else if (NameIs(key, "observedTimeUnixNano", "observed_time_unix_nano")) {
      out.observed_time_unix_nano = ReadUint64(field, key);
    } else if (NameIs(key, "severityNumber", "severity_number")) {
      static constexpr std::pair<std::string_view, int32_t> kNames[]{
        {"SEVERITY_NUMBER_UNSPECIFIED", 0}, {"SEVERITY_NUMBER_TRACE", 1},
        {"SEVERITY_NUMBER_TRACE2", 2},      {"SEVERITY_NUMBER_TRACE3", 3},
        {"SEVERITY_NUMBER_TRACE4", 4},      {"SEVERITY_NUMBER_DEBUG", 5},
        {"SEVERITY_NUMBER_DEBUG2", 6},      {"SEVERITY_NUMBER_DEBUG3", 7},
        {"SEVERITY_NUMBER_DEBUG4", 8},      {"SEVERITY_NUMBER_INFO", 9},
        {"SEVERITY_NUMBER_INFO2", 10},      {"SEVERITY_NUMBER_INFO3", 11},
        {"SEVERITY_NUMBER_INFO4", 12},      {"SEVERITY_NUMBER_WARN", 13},
        {"SEVERITY_NUMBER_WARN2", 14},      {"SEVERITY_NUMBER_WARN3", 15},
        {"SEVERITY_NUMBER_WARN4", 16},      {"SEVERITY_NUMBER_ERROR", 17},
        {"SEVERITY_NUMBER_ERROR2", 18},     {"SEVERITY_NUMBER_ERROR3", 19},
        {"SEVERITY_NUMBER_ERROR4", 20},     {"SEVERITY_NUMBER_FATAL", 21},
        {"SEVERITY_NUMBER_FATAL2", 22},     {"SEVERITY_NUMBER_FATAL3", 23},
        {"SEVERITY_NUMBER_FATAL4", 24},
      };
      out.severity_number = ReadEnum(field, key, kNames);
    } else if (NameIs(key, "severityText", "severity_text")) {
      out.severity_text = ReadString(field, key);
    } else if (NameIs(key, "eventName", "event_name")) {
      out.event_name = ReadString(field, key);
    } else if (key == "body") {
      out.body = ParseAnyValue(field, arena);
    } else if (key == "attributes") {
      out.attributes = ParseKeyValueList(field, arena, key);
    } else if (NameIs(key, "droppedAttributesCount",
                      "dropped_attributes_count")) {
      out.dropped_attributes_count = ReadUint32(field, key);
    } else if (key == "flags") {
      out.flags = ReadUint32(field, key);
    } else if (NameIs(key, "traceId", "trace_id")) {
      out.trace_id = ReadId(field, key, 32);
    } else if (NameIs(key, "spanId", "span_id")) {
      out.span_id = ReadId(field, key, 16);
    }
  }
}

void ParseSpanEvent(Value value, SpanEvent& out, ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("events", "an array of Event objects");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("events", "an array of Event objects");
    }
    auto field = member.value().value();
    if (NameIs(key, "timeUnixNano", "time_unix_nano")) {
      out.time_unix_nano = ReadUint64(field, key);
    } else if (key == "name") {
      out.name = ReadString(field, key);
    } else if (key == "attributes") {
      out.attributes = ParseKeyValueList(field, arena, key);
    } else if (NameIs(key, "droppedAttributesCount",
                      "dropped_attributes_count")) {
      out.dropped_attributes_count = ReadUint32(field, key);
    }
  }
}

void ParseSpanLink(Value value, SpanLink& out, ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("links", "an array of Link objects");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("links", "an array of Link objects");
    }
    auto field = member.value().value();
    if (NameIs(key, "traceId", "trace_id")) {
      out.trace_id = ReadId(field, key, 32);
    } else if (NameIs(key, "spanId", "span_id")) {
      out.span_id = ReadId(field, key, 16);
    } else if (NameIs(key, "traceState", "trace_state")) {
      out.trace_state = ReadString(field, key);
    } else if (key == "attributes") {
      out.attributes = ParseKeyValueList(field, arena, key);
    } else if (NameIs(key, "droppedAttributesCount",
                      "dropped_attributes_count")) {
      out.dropped_attributes_count = ReadUint32(field, key);
    } else if (key == "flags") {
      out.flags = ReadUint32(field, key);
    }
  }
}

void ParseStatus(Value value, Status& out) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("status", "an object");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("status", "an object");
    }
    auto field = member.value().value();
    if (key == "code") {
      static constexpr std::pair<std::string_view, int32_t> kNames[]{
        {"STATUS_CODE_UNSET", 0},
        {"STATUS_CODE_OK", 1},
        {"STATUS_CODE_ERROR", 2},
      };
      out.code = static_cast<StatusCode>(ReadEnum(field, key, kNames));
    } else if (key == "message") {
      out.message = ReadString(field, key);
    }
  }
}

void ParseSpan(Value value, Span& out, ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("spans", "an array of Span objects");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("spans", "an array of Span objects");
    }
    auto field = member.value().value();
    if (NameIs(key, "traceId", "trace_id")) {
      out.trace_id = ReadId(field, key, 32);
    } else if (NameIs(key, "spanId", "span_id")) {
      out.span_id = ReadId(field, key, 16);
    } else if (NameIs(key, "traceState", "trace_state")) {
      out.trace_state = ReadString(field, key);
    } else if (NameIs(key, "parentSpanId", "parent_span_id")) {
      out.parent_span_id = ReadId(field, key, 16);
    } else if (key == "flags") {
      out.flags = ReadUint32(field, key);
    } else if (key == "name") {
      out.name = ReadString(field, key);
    } else if (key == "kind") {
      static constexpr std::pair<std::string_view, int32_t> kNames[]{
        {"SPAN_KIND_UNSPECIFIED", 0}, {"SPAN_KIND_INTERNAL", 1},
        {"SPAN_KIND_SERVER", 2},      {"SPAN_KIND_CLIENT", 3},
        {"SPAN_KIND_PRODUCER", 4},    {"SPAN_KIND_CONSUMER", 5},
      };
      out.kind = static_cast<SpanKind>(ReadEnum(field, key, kNames));
    } else if (NameIs(key, "startTimeUnixNano", "start_time_unix_nano")) {
      out.start_time_unix_nano = ReadUint64(field, key);
    } else if (NameIs(key, "endTimeUnixNano", "end_time_unix_nano")) {
      out.end_time_unix_nano = ReadUint64(field, key);
    } else if (key == "attributes") {
      out.attributes = ParseKeyValueList(field, arena, key);
    } else if (NameIs(key, "droppedAttributesCount",
                      "dropped_attributes_count")) {
      out.dropped_attributes_count = ReadUint32(field, key);
    } else if (key == "events") {
      simdjson::ondemand::array items;
      if (field.get_array().get(items) != simdjson::SUCCESS) {
        Throw(key, "an array");
      }
      for (auto item : items) {
        ParseSpanEvent(item.value(), out.events.emplace_back(), arena);
      }
    } else if (NameIs(key, "droppedEventsCount", "dropped_events_count")) {
      out.dropped_events_count = ReadUint32(field, key);
    } else if (key == "links") {
      simdjson::ondemand::array items;
      if (field.get_array().get(items) != simdjson::SUCCESS) {
        Throw(key, "an array");
      }
      for (auto item : items) {
        ParseSpanLink(item.value(), out.links.emplace_back(), arena);
      }
    } else if (NameIs(key, "droppedLinksCount", "dropped_links_count")) {
      out.dropped_links_count = ReadUint32(field, key);
    } else if (key == "status") {
      ParseStatus(field, out.status);
    }
  }
}

constexpr std::pair<std::string_view, int32_t> kTemporalityNames[]{
  {"AGGREGATION_TEMPORALITY_UNSPECIFIED", 0},
  {"AGGREGATION_TEMPORALITY_DELTA", 1},
  {"AGGREGATION_TEMPORALITY_CUMULATIVE", 2},
};

void ParseExemplar(Value value, Exemplar& out, ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("exemplars", "an array of Exemplar objects");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("exemplars", "an array of Exemplar objects");
    }
    auto field = member.value().value();
    if (NameIs(key, "filteredAttributes", "filtered_attributes")) {
      out.filtered_attributes = ParseKeyValueList(field, arena, key);
    } else if (NameIs(key, "timeUnixNano", "time_unix_nano")) {
      out.time_unix_nano = ReadUint64(field, key);
    } else if (NameIs(key, "asDouble", "as_double")) {
      out.value = ReadDouble(field, key);
    } else if (NameIs(key, "asInt", "as_int")) {
      out.value = ReadInt64(field, key);
    } else if (NameIs(key, "spanId", "span_id")) {
      out.span_id = ReadId(field, key, 16);
    } else if (NameIs(key, "traceId", "trace_id")) {
      out.trace_id = ReadId(field, key, 32);
    }
  }
}

void ParseExemplars(Value value, std::vector<Exemplar>& out,
                    ValueArena& arena) {
  simdjson::ondemand::array items;
  if (value.get_array().get(items) != simdjson::SUCCESS) {
    Throw("exemplars", "an array");
  }
  for (auto item : items) {
    ParseExemplar(item.value(), out.emplace_back(), arena);
  }
}

void ParseNumberDataPoint(Value value, NumberDataPoint& out,
                          ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("dataPoints", "an array of NumberDataPoint objects");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("dataPoints", "an array of NumberDataPoint objects");
    }
    auto field = member.value().value();
    if (key == "attributes") {
      out.attributes = ParseKeyValueList(field, arena, key);
    } else if (NameIs(key, "startTimeUnixNano", "start_time_unix_nano")) {
      out.start_time_unix_nano = ReadUint64(field, key);
    } else if (NameIs(key, "timeUnixNano", "time_unix_nano")) {
      out.time_unix_nano = ReadUint64(field, key);
    } else if (NameIs(key, "asDouble", "as_double")) {
      out.value = ReadDouble(field, key);
    } else if (NameIs(key, "asInt", "as_int")) {
      out.value = ReadInt64(field, key);
    } else if (key == "exemplars") {
      ParseExemplars(field, out.exemplars, arena);
    } else if (key == "flags") {
      out.flags = ReadUint32(field, key);
    }
  }
}

std::vector<uint64_t> ReadUint64Array(Value value, std::string_view field) {
  std::vector<uint64_t> out;
  simdjson::ondemand::array items;
  if (value.get_array().get(items) != simdjson::SUCCESS) {
    Throw(field, "an array");
  }
  for (auto item : items) {
    out.push_back(ReadUint64(item.value(), field));
  }
  return out;
}

std::vector<double> ReadDoubleArray(Value value, std::string_view field) {
  std::vector<double> out;
  simdjson::ondemand::array items;
  if (value.get_array().get(items) != simdjson::SUCCESS) {
    Throw(field, "an array");
  }
  for (auto item : items) {
    out.push_back(ReadDouble(item.value(), field));
  }
  return out;
}

void ParseHistogramDataPoint(Value value, HistogramDataPoint& out,
                             ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("dataPoints", "an array of HistogramDataPoint objects");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("dataPoints", "an array of HistogramDataPoint objects");
    }
    auto field = member.value().value();
    if (key == "attributes") {
      out.attributes = ParseKeyValueList(field, arena, key);
    } else if (NameIs(key, "startTimeUnixNano", "start_time_unix_nano")) {
      out.start_time_unix_nano = ReadUint64(field, key);
    } else if (NameIs(key, "timeUnixNano", "time_unix_nano")) {
      out.time_unix_nano = ReadUint64(field, key);
    } else if (key == "count") {
      out.count = ReadUint64(field, key);
    } else if (key == "sum") {
      out.sum = ReadDouble(field, key);
    } else if (NameIs(key, "bucketCounts", "bucket_counts")) {
      out.bucket_counts = ReadUint64Array(field, key);
    } else if (NameIs(key, "explicitBounds", "explicit_bounds")) {
      out.explicit_bounds = ReadDoubleArray(field, key);
    } else if (key == "exemplars") {
      ParseExemplars(field, out.exemplars, arena);
    } else if (key == "flags") {
      out.flags = ReadUint32(field, key);
    } else if (key == "min") {
      out.min = ReadDouble(field, key);
    } else if (key == "max") {
      out.max = ReadDouble(field, key);
    }
  }
}

void ParseExponentialBuckets(Value value, ExponentialHistogramBuckets& out) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("buckets", "an object");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("buckets", "an object");
    }
    auto field = member.value().value();
    if (key == "offset") {
      out.offset = ReadInt32(field, key);
    } else if (NameIs(key, "bucketCounts", "bucket_counts")) {
      out.bucket_counts = ReadUint64Array(field, key);
    }
  }
}

void ParseExponentialHistogramDataPoint(Value value,
                                        ExponentialHistogramDataPoint& out,
                                        ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("dataPoints", "an array of ExponentialHistogramDataPoint objects");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("dataPoints", "an array of ExponentialHistogramDataPoint objects");
    }
    auto field = member.value().value();
    if (key == "attributes") {
      out.attributes = ParseKeyValueList(field, arena, key);
    } else if (NameIs(key, "startTimeUnixNano", "start_time_unix_nano")) {
      out.start_time_unix_nano = ReadUint64(field, key);
    } else if (NameIs(key, "timeUnixNano", "time_unix_nano")) {
      out.time_unix_nano = ReadUint64(field, key);
    } else if (key == "count") {
      out.count = ReadUint64(field, key);
    } else if (key == "sum") {
      out.sum = ReadDouble(field, key);
    } else if (key == "scale") {
      out.scale = ReadInt32(field, key);
    } else if (NameIs(key, "zeroCount", "zero_count")) {
      out.zero_count = ReadUint64(field, key);
    } else if (key == "positive") {
      ParseExponentialBuckets(field, out.positive);
    } else if (key == "negative") {
      ParseExponentialBuckets(field, out.negative);
    } else if (key == "flags") {
      out.flags = ReadUint32(field, key);
    } else if (key == "exemplars") {
      ParseExemplars(field, out.exemplars, arena);
    } else if (key == "min") {
      out.min = ReadDouble(field, key);
    } else if (key == "max") {
      out.max = ReadDouble(field, key);
    } else if (NameIs(key, "zeroThreshold", "zero_threshold")) {
      out.zero_threshold = ReadDouble(field, key);
    }
  }
}

void ParseSummaryDataPoint(Value value, SummaryDataPoint& out,
                           ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("dataPoints", "an array of SummaryDataPoint objects");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("dataPoints", "an array of SummaryDataPoint objects");
    }
    auto field = member.value().value();
    if (key == "attributes") {
      out.attributes = ParseKeyValueList(field, arena, key);
    } else if (NameIs(key, "startTimeUnixNano", "start_time_unix_nano")) {
      out.start_time_unix_nano = ReadUint64(field, key);
    } else if (NameIs(key, "timeUnixNano", "time_unix_nano")) {
      out.time_unix_nano = ReadUint64(field, key);
    } else if (key == "count") {
      out.count = ReadUint64(field, key);
    } else if (key == "sum") {
      out.sum = ReadDouble(field, key);
    } else if (NameIs(key, "quantileValues", "quantile_values")) {
      simdjson::ondemand::array items;
      if (field.get_array().get(items) != simdjson::SUCCESS) {
        Throw(key, "an array");
      }
      for (auto item : items) {
        Object entry;
        if (item.get_object().get(entry) != simdjson::SUCCESS) {
          Throw(key, "an array of objects");
        }
        auto& quantile = out.quantile_values.emplace_back();
        for (auto inner : entry) {
          std::string_view inner_key;
          if (inner.unescaped_key().get(inner_key) != simdjson::SUCCESS) {
            Throw(key, "an array of objects");
          }
          if (inner_key == "quantile") {
            quantile.quantile = ReadDouble(inner.value().value(), inner_key);
          } else if (inner_key == "value") {
            quantile.value = ReadDouble(inner.value().value(), inner_key);
          }
        }
      }
    } else if (key == "flags") {
      out.flags = ReadUint32(field, key);
    }
  }
}

template<typename Point, typename ParseOne>
std::vector<Point> ParseDataPoints(Value value, ValueArena& arena,
                                   ParseOne parse_one) {
  std::vector<Point> out;
  simdjson::ondemand::array items;
  if (value.get_array().get(items) != simdjson::SUCCESS) {
    Throw("dataPoints", "an array");
  }
  for (auto item : items) {
    parse_one(item.value(), out.emplace_back(), arena);
  }
  return out;
}

enum class MetricShape {
  None,
  Gauge,
  Sum,
  Histogram,
  ExponentialHistogram,
  Summary,
};

MetricShape ShapeOf(std::string_view key) {
  if (key == "gauge") {
    return MetricShape::Gauge;
  }
  if (key == "sum") {
    return MetricShape::Sum;
  }
  if (key == "histogram") {
    return MetricShape::Histogram;
  }
  if (NameIs(key, "exponentialHistogram", "exponential_histogram")) {
    return MetricShape::ExponentialHistogram;
  }
  if (key == "summary") {
    return MetricShape::Summary;
  }
  return MetricShape::None;
}

void ParseMetricShape(Value value, MetricShape shape, Metric& out,
                      ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("metric data", "an object");
  }
  Gauge gauge;
  Sum sum;
  Histogram histogram;
  ExponentialHistogram exponential;
  Summary summary;
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("metric data", "an object");
    }
    auto field = member.value().value();
    const bool points = NameIs(key, "dataPoints", "data_points");
    const bool temporality =
      NameIs(key, "aggregationTemporality", "aggregation_temporality");
    switch (shape) {
      case MetricShape::Gauge:
        if (points) {
          gauge.data_points = ParseDataPoints<NumberDataPoint>(
            field, arena, ParseNumberDataPoint);
        }
        break;
      case MetricShape::Sum:
        if (points) {
          sum.data_points = ParseDataPoints<NumberDataPoint>(
            field, arena, ParseNumberDataPoint);
        } else if (temporality) {
          sum.aggregation_temporality = static_cast<AggregationTemporality>(
            ReadEnum(field, key, kTemporalityNames));
        } else if (NameIs(key, "isMonotonic", "is_monotonic")) {
          sum.is_monotonic = ReadBool(field, key);
        }
        break;
      case MetricShape::Histogram:
        if (points) {
          histogram.data_points = ParseDataPoints<HistogramDataPoint>(
            field, arena, ParseHistogramDataPoint);
        } else if (temporality) {
          histogram.aggregation_temporality =
            static_cast<AggregationTemporality>(
              ReadEnum(field, key, kTemporalityNames));
        }
        break;
      case MetricShape::ExponentialHistogram:
        if (points) {
          exponential.data_points =
            ParseDataPoints<ExponentialHistogramDataPoint>(
              field, arena, ParseExponentialHistogramDataPoint);
        } else if (temporality) {
          exponential.aggregation_temporality =
            static_cast<AggregationTemporality>(
              ReadEnum(field, key, kTemporalityNames));
        }
        break;
      case MetricShape::Summary:
        if (points) {
          summary.data_points = ParseDataPoints<SummaryDataPoint>(
            field, arena, ParseSummaryDataPoint);
        }
        break;
      case MetricShape::None:
        break;
    }
  }
  switch (shape) {
    case MetricShape::Gauge:
      out.data = std::move(gauge);
      break;
    case MetricShape::Sum:
      out.data = std::move(sum);
      break;
    case MetricShape::Histogram:
      out.data = std::move(histogram);
      break;
    case MetricShape::ExponentialHistogram:
      out.data = std::move(exponential);
      break;
    case MetricShape::Summary:
      out.data = std::move(summary);
      break;
    case MetricShape::None:
      break;
  }
}

void ParseMetric(Value value, Metric& out, ValueArena& arena) {
  Object fields;
  if (value.get_object().get(fields) != simdjson::SUCCESS) {
    Throw("metrics", "an array of Metric objects");
  }
  for (auto member : fields) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("metrics", "an array of Metric objects");
    }
    auto field = member.value().value();
    if (key == "name") {
      out.name = ReadString(field, key);
    } else if (key == "description") {
      out.description = ReadString(field, key);
    } else if (key == "unit") {
      out.unit = ReadString(field, key);
    } else if (key == "metadata") {
      out.metadata = ParseKeyValueList(field, arena, key);
    } else if (const auto shape = ShapeOf(key); shape != MetricShape::None) {
      ParseMetricShape(field, shape, out, arena);
    }
  }
}

template<typename Record, typename ParseRecord>
void ParseRequest(std::string_view json, ExportRequest<Record>& out,
                  std::string_view resource_field,
                  std::string_view resource_field_snake,
                  std::string_view scope_field,
                  std::string_view scope_field_snake,
                  std::string_view record_field,
                  std::string_view record_field_snake,
                  ParseRecord parse_record) {
  simdjson::ondemand::parser parser;
  simdjson::padded_string padded{json};
  simdjson::ondemand::document doc;
  if (const auto ec = parser.iterate(padded).get(doc);
      ec != simdjson::SUCCESS) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("OTLP/JSON: ", simdjson::error_message(ec)));
  }
  Object root;
  if (doc.get_object().get(root) != simdjson::SUCCESS) {
    Throw("<root>", "an object");
  }
  for (auto member : root) {
    std::string_view key;
    if (member.unescaped_key().get(key) != simdjson::SUCCESS) {
      Throw("<root>", "an object");
    }
    if (!NameIs(key, resource_field, resource_field_snake)) {
      continue;
    }
    simdjson::ondemand::array resources;
    if (member.value().get_array().get(resources) != simdjson::SUCCESS) {
      Throw(key, "an array");
    }
    for (auto entry : resources) {
      auto& resource_records = out.resources.emplace_back();
      Object resource_object;
      if (entry.get_object().get(resource_object) != simdjson::SUCCESS) {
        Throw(key, "an array of objects");
      }
      for (auto resource_member : resource_object) {
        std::string_view resource_key;
        if (resource_member.unescaped_key().get(resource_key) !=
            simdjson::SUCCESS) {
          Throw(key, "an array of objects");
        }
        auto resource_value = resource_member.value().value();
        if (resource_key == "resource") {
          ParseResource(resource_value, resource_records.resource, out.arena);
        } else if (NameIs(resource_key, "schemaUrl", "schema_url")) {
          resource_records.schema_url =
            ReadString(resource_value, resource_key);
        } else if (NameIs(resource_key, scope_field, scope_field_snake)) {
          simdjson::ondemand::array scopes;
          if (resource_value.get_array().get(scopes) != simdjson::SUCCESS) {
            Throw(resource_key, "an array");
          }
          for (auto scope_entry : scopes) {
            auto& scope_records = resource_records.scopes.emplace_back();
            Object scope_object;
            if (scope_entry.get_object().get(scope_object) !=
                simdjson::SUCCESS) {
              Throw(resource_key, "an array of objects");
            }
            for (auto scope_member : scope_object) {
              std::string_view scope_key;
              if (scope_member.unescaped_key().get(scope_key) !=
                  simdjson::SUCCESS) {
                Throw(resource_key, "an array of objects");
              }
              auto scope_value = scope_member.value().value();
              if (scope_key == "scope") {
                ParseScope(scope_value, scope_records.scope, out.arena);
              } else if (NameIs(scope_key, "schemaUrl", "schema_url")) {
                scope_records.schema_url = ReadString(scope_value, scope_key);
              } else if (NameIs(scope_key, record_field, record_field_snake)) {
                simdjson::ondemand::array records;
                if (scope_value.get_array().get(records) != simdjson::SUCCESS) {
                  Throw(scope_key, "an array");
                }
                for (auto record : records) {
                  parse_record(record.value(),
                               scope_records.records.emplace_back(), out.arena);
                }
              }
            }
          }
        }
      }
    }
  }
}

}  // namespace

void ParseLogsRequest(std::string_view json, ExportLogsRequest& out) {
  ParseRequest<LogRecord>(json, out, "resourceLogs", "resource_logs",
                          "scopeLogs", "scope_logs", "logRecords",
                          "log_records", ParseLogRecord);
}

void ParseTracesRequest(std::string_view json, ExportTracesRequest& out) {
  ParseRequest<Span>(json, out, "resourceSpans", "resource_spans", "scopeSpans",
                     "scope_spans", "spans", "spans", ParseSpan);
}

void ParseMetricsRequest(std::string_view json, ExportMetricsRequest& out) {
  ParseRequest<Metric>(json, out, "resourceMetrics", "resource_metrics",
                       "scopeMetrics", "scope_metrics", "metrics", "metrics",
                       ParseMetric);
}

}  // namespace sdb::otel
