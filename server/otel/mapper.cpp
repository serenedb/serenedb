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

#include <variant>

// Canonical value rules shared by every ingestion route: attribute JSON with
// sorted keys, body stringification, and the string forms stored for the
// SpanKind / StatusCode / AggregationTemporality enums.
//
// Enum members:  https://github.com/open-telemetry/opentelemetry-proto
// Attribute keys (service.name, event.name) are semantic conventions:
// https://opentelemetry.io/docs/specs/semconv/
namespace sdb::otel {
namespace {

using StringBuilder = simdjson::builder::string_builder;

void AppendAnyValue(StringBuilder& sb, const AnyValue* value);

void AppendAttributes(StringBuilder& sb, const KeyValueList& attributes) {
  std::vector<const KeyValue*> sorted;
  sorted.reserve(attributes.size());
  for (const auto& kv : attributes) {
    sorted.push_back(&kv);
  }
  absl::c_stable_sort(sorted, [](const KeyValue* lhs, const KeyValue* rhs) {
    return lhs->key < rhs->key;
  });
  sb.start_object();
  bool first = true;
  for (const auto* kv : sorted) {
    if (!first) {
      sb.append_comma();
    }
    first = false;
    sb.escape_and_append_with_quotes(kv->key);
    sb.append_colon();
    AppendAnyValue(sb, kv->value);
  }
  sb.end_object();
}

void AppendAnyValue(StringBuilder& sb, const AnyValue* value) {
  if (value == nullptr) {
    sb.append_null();
    return;
  }
  std::visit(
    [&](const auto& held) {
      using Held = std::decay_t<decltype(held)>;
      if constexpr (std::is_same_v<Held, std::monostate>) {
        sb.append_null();
      } else if constexpr (std::is_same_v<Held, std::string>) {
        sb.escape_and_append_with_quotes(held);
      } else if constexpr (std::is_same_v<Held, bool>) {
        sb.append_raw(held ? "true" : "false");
      } else if constexpr (std::is_same_v<Held, int64_t>) {
        sb.append(held);
      } else if constexpr (std::is_same_v<Held, double>) {
        sb.append(held);
      } else if constexpr (std::is_same_v<Held, BytesValue>) {
        sb.escape_and_append_with_quotes(held.data);
      } else if constexpr (std::is_same_v<Held, ArrayValue>) {
        sb.start_array();
        bool first = true;
        for (const auto* element : held.values) {
          if (!first) {
            sb.append_comma();
          }
          first = false;
          AppendAnyValue(sb, element);
        }
        sb.end_array();
      } else if constexpr (std::is_same_v<Held, KvlistValue>) {
        AppendAttributes(sb, held.values);
      }
    },
    value->value);
}

std::string Finish(StringBuilder& sb) {
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

std::string AnyValueToJson(const AnyValue* value) {
  StringBuilder sb;
  AppendAnyValue(sb, value);
  return Finish(sb);
}

}  // namespace

std::string AttributesToJson(const KeyValueList& attributes) {
  StringBuilder sb;
  AppendAttributes(sb, attributes);
  return Finish(sb);
}

std::string BodyToText(const AnyValue* body) {
  if (body == nullptr) {
    return {};
  }
  if (const auto* text = std::get_if<std::string>(&body->value)) {
    return *text;
  }
  if (const auto* bytes = std::get_if<BytesValue>(&body->value)) {
    return bytes->data;
  }
  if (std::holds_alternative<std::monostate>(body->value)) {
    return {};
  }
  return AnyValueToJson(body);
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
  StringBuilder sb;
  sb.start_array();
  bool first = true;
  for (const auto& event : events) {
    if (!first) {
      sb.append_comma();
    }
    first = false;
    sb.start_object();
    sb.escape_and_append_with_quotes("attributes");
    sb.append_colon();
    AppendAttributes(sb, event.attributes);
    sb.append_comma();
    sb.escape_and_append_with_quotes("dropped_attributes_count");
    sb.append_colon();
    sb.append(static_cast<uint64_t>(event.dropped_attributes_count));
    sb.append_comma();
    sb.escape_and_append_with_quotes("name");
    sb.append_colon();
    sb.escape_and_append_with_quotes(event.name);
    sb.append_comma();
    sb.escape_and_append_with_quotes("timestamp");
    sb.append_colon();
    sb.escape_and_append_with_quotes(FormatTimestampNs(event.time_unix_nano));
    sb.end_object();
  }
  sb.end_array();
  return Finish(sb);
}

std::string LinksToJson(const std::vector<SpanLink>& links) {
  StringBuilder sb;
  sb.start_array();
  bool first = true;
  for (const auto& link : links) {
    if (!first) {
      sb.append_comma();
    }
    first = false;
    sb.start_object();
    sb.escape_and_append_with_quotes("attributes");
    sb.append_colon();
    AppendAttributes(sb, link.attributes);
    sb.append_comma();
    sb.escape_and_append_with_quotes("span_id");
    sb.append_colon();
    if (link.span_id.empty()) {
      sb.append_null();
    } else {
      sb.escape_and_append_with_quotes(link.span_id);
    }
    sb.append_comma();
    sb.escape_and_append_with_quotes("trace_id");
    sb.append_colon();
    if (link.trace_id.empty()) {
      sb.append_null();
    } else {
      sb.escape_and_append_with_quotes(link.trace_id);
    }
    sb.append_comma();
    sb.escape_and_append_with_quotes("trace_state");
    sb.append_colon();
    if (link.trace_state.empty()) {
      sb.append_null();
    } else {
      sb.escape_and_append_with_quotes(link.trace_state);
    }
    sb.end_object();
  }
  sb.end_array();
  return Finish(sb);
}

std::string ExemplarsToJson(const std::vector<Exemplar>& exemplars) {
  StringBuilder sb;
  sb.start_array();
  bool first = true;
  for (const auto& exemplar : exemplars) {
    if (!first) {
      sb.append_comma();
    }
    first = false;
    sb.start_object();
    sb.escape_and_append_with_quotes("filtered_attributes");
    sb.append_colon();
    AppendAttributes(sb, exemplar.filtered_attributes);
    sb.append_comma();
    sb.escape_and_append_with_quotes("span_id");
    sb.append_colon();
    if (exemplar.span_id.empty()) {
      sb.append_null();
    } else {
      sb.escape_and_append_with_quotes(exemplar.span_id);
    }
    sb.append_comma();
    sb.escape_and_append_with_quotes("timestamp");
    sb.append_colon();
    sb.escape_and_append_with_quotes(
      FormatTimestampNs(exemplar.time_unix_nano));
    sb.append_comma();
    sb.escape_and_append_with_quotes("trace_id");
    sb.append_colon();
    if (exemplar.trace_id.empty()) {
      sb.append_null();
    } else {
      sb.escape_and_append_with_quotes(exemplar.trace_id);
    }
    sb.append_comma();
    sb.escape_and_append_with_quotes("value");
    sb.append_colon();
    if (const auto* number = std::get_if<int64_t>(&exemplar.value)) {
      sb.append(static_cast<double>(*number));
    } else if (const auto* real = std::get_if<double>(&exemplar.value)) {
      sb.append(*real);
    } else {
      sb.append_null();
    }
    sb.end_object();
  }
  sb.end_array();
  return Finish(sb);
}

}  // namespace sdb::otel
