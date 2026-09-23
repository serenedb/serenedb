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

#include "otel/mapper.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_format.h>
#include <absl/time/civil_time.h>
#include <absl/time/time.h>
#include <simdjson.h>

#include <iresearch/utils/serializer.hpp>
#include <optional>
#include <variant>

#include "server/utils/simdjson_sink.h"

// Enum members:  https://github.com/open-telemetry/opentelemetry-proto
// Attribute keys (service.name, event.name) are semantic conventions:
// https://opentelemetry.io/docs/specs/semconv/
namespace sdb::otel {
namespace {

template<typename Context>
concept JsonWriteContext = requires(Context ctx) { ctx.io().WriteNull(); };

struct SortedAttributes {
  const KeyValueList* attributes;
};

struct NullableText {
  std::string_view text;
};

struct NullableNumber {
  std::optional<double> number;
};

template<JsonWriteContext Context>
void SerdeWrite(Context ctx, SortedAttributes attributes) {
  std::vector<const KeyValue*> sorted;
  sorted.reserve(attributes.attributes->size());
  for (const auto& kv : *attributes.attributes) {
    sorted.push_back(&kv);
  }
  absl::c_stable_sort(sorted, [](const KeyValue* lhs, const KeyValue* rhs) {
    return lhs->key < rhs->key;
  });
  auto& sink = ctx.io();
  sink.OnObjectBegin();
  bool first = true;
  for (const auto* kv : sorted) {
    if (!std::exchange(first, false)) {
      sink.OnSeparator();
    }
    sink.OnPropertyBegin(kv->key);
    irs::utils::WriteObject(sink, kv->value);
  }
  sink.OnObjectEnd();
}

template<JsonWriteContext Context>
void SerdeWrite(Context ctx, NullableText text) {
  if (text.text.empty()) {
    ctx.io().WriteNull();
  } else {
    ctx.io().WriteValue(text.text);
  }
}

template<JsonWriteContext Context>
void SerdeWrite(Context ctx, NullableNumber number) {
  if (number.number) {
    ctx.io().WriteValue(*number.number);
  } else {
    ctx.io().WriteNull();
  }
}

template<typename T>
std::string ToJson(const T& value) {
  simdjson::builder::string_builder sb;
  utils::JsonSink sink{sb};
  irs::utils::WriteObject(sink, value);
  auto view = sb.view();
  if (view.error() != simdjson::SUCCESS) {
    return "null";
  }
  return std::string{view.value()};
}

std::string FormatTimestampNs(uint64_t unix_nano) {
  const auto seconds = static_cast<int64_t>(unix_nano / 1000000000ULL);
  const auto nanos = static_cast<uint32_t>(unix_nano % 1000000000ULL);
  const absl::Time time = absl::FromUnixSeconds(seconds);
  return absl::StrFormat(
    "%sT%s.%09uZ", absl::FormatTime("%Y-%m-%d", time, absl::UTCTimeZone()),
    absl::FormatTime("%H:%M:%S", time, absl::UTCTimeZone()), nanos);
}

NullableNumber ExemplarNumber(const Exemplar& exemplar) {
  if (const auto* number = std::get_if<int64_t>(&exemplar.value)) {
    return {.number = static_cast<double>(*number)};
  }
  if (const auto* real = std::get_if<double>(&exemplar.value)) {
    return {.number = *real};
  }
  return {};
}

struct EventJson {
  SortedAttributes attributes;
  uint32_t dropped_attributes_count;
  std::string_view name;
  std::string timestamp;
};

struct LinkJson {
  SortedAttributes attributes;
  NullableText span_id;
  NullableText trace_id;
  NullableText trace_state;
};

struct ExemplarJson {
  SortedAttributes filtered_attributes;
  NullableText span_id;
  std::string timestamp;
  NullableText trace_id;
  NullableNumber value;
};

}  // namespace

template<JsonWriteContext Context>
void SerdeWrite(Context ctx, const AnyValue* value) {
  auto& sink = ctx.io();
  if (value == nullptr) {
    sink.WriteNull();
    return;
  }
  std::visit(
    [&]<typename Held>(const Held& held) {
      if constexpr (std::is_same_v<Held, std::monostate>) {
        sink.WriteNull();
      } else if constexpr (std::is_same_v<Held, std::string_view>) {
        sink.WriteValue(std::string_view{held});
      } else if constexpr (std::is_same_v<Held, BytesValue>) {
        sink.WriteValue(std::string_view{held.data});
      } else if constexpr (std::is_same_v<Held, ArrayValue>) {
        irs::utils::WriteObject(sink, held.values);
      } else if constexpr (std::is_same_v<Held, KvlistValue>) {
        irs::utils::WriteObject(sink, SortedAttributes{&held.values});
      } else {
        sink.WriteValue(held);
      }
    },
    value->value);
}

std::string AttributesToJson(const KeyValueList& attributes) {
  return ToJson(SortedAttributes{&attributes});
}

std::string BodyToText(const AnyValue* body) {
  if (body == nullptr) {
    return {};
  }
  if (const auto* text = std::get_if<std::string_view>(&body->value)) {
    return std::string{*text};
  }
  if (const auto* bytes = std::get_if<BytesValue>(&body->value)) {
    return bytes->data;
  }
  if (std::holds_alternative<std::monostate>(body->value)) {
    return {};
  }
  return ToJson(body);
}

const AnyValue* FindAttribute(const KeyValueList& attributes,
                              std::string_view key) {
  for (const auto& kv : attributes) {
    if (kv.key == key) {
      return kv.value;
    }
  }
  return nullptr;
}

std::string_view SpanKindName(SpanKind kind) {
  switch (kind) {
    case SpanKind::Internal:
      return "Internal";
    case SpanKind::Server:
      return "Server";
    case SpanKind::Client:
      return "Client";
    case SpanKind::Producer:
      return "Producer";
    case SpanKind::Consumer:
      return "Consumer";
    case SpanKind::Unspecified:
      break;
  }
  return "Unspecified";
}

std::string_view StatusCodeName(StatusCode code) {
  switch (code) {
    case StatusCode::Ok:
      return "Ok";
    case StatusCode::Error:
      return "Error";
    case StatusCode::Unset:
      break;
  }
  return "Unset";
}

std::string_view TemporalityName(AggregationTemporality temporality) {
  switch (temporality) {
    case AggregationTemporality::Delta:
      return "Delta";
    case AggregationTemporality::Cumulative:
      return "Cumulative";
    case AggregationTemporality::Unspecified:
      break;
  }
  return "Unspecified";
}

std::string EventsToJson(const std::vector<SpanEvent>& events) {
  std::vector<EventJson> items;
  items.reserve(events.size());
  for (const auto& event : events) {
    items.push_back({.attributes = {&event.attributes},
                     .dropped_attributes_count = event.dropped_attributes_count,
                     .name = event.name,
                     .timestamp = FormatTimestampNs(event.time_unix_nano)});
  }
  return ToJson(items);
}

std::string LinksToJson(const std::vector<SpanLink>& links) {
  std::vector<LinkJson> items;
  items.reserve(links.size());
  for (const auto& link : links) {
    items.push_back({.attributes = {&link.attributes},
                     .span_id = {link.span_id.hex},
                     .trace_id = {link.trace_id.hex},
                     .trace_state = {link.trace_state}});
  }
  return ToJson(items);
}

std::string ExemplarsToJson(const std::vector<Exemplar>& exemplars) {
  std::vector<ExemplarJson> items;
  items.reserve(exemplars.size());
  for (const auto& exemplar : exemplars) {
    items.push_back({.filtered_attributes = {&exemplar.filtered_attributes},
                     .span_id = {exemplar.span_id.hex},
                     .timestamp = FormatTimestampNs(exemplar.time_unix_nano),
                     .trace_id = {exemplar.trace_id.hex},
                     .value = ExemplarNumber(exemplar)});
  }
  return ToJson(items);
}

}  // namespace sdb::otel
