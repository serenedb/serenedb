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
#include <absl/strings/escaping.h>
#include <absl/strings/str_format.h>
#include <absl/time/time.h>
#include <simdjson.h>

#include <vector>

// Canonical value rules shared by both decoders: attribute JSON with
// sorted keys, body stringification, and the string forms stored for the
// SpanKind / StatusCode / AggregationTemporality enums.
//
// Enum members:  https://github.com/open-telemetry/opentelemetry-proto
// Attribute keys (service.name, event.name) are semantic conventions:
// https://opentelemetry.io/docs/specs/semconv/
namespace sdb::otel {
namespace {

using StringBuilder = simdjson::builder::string_builder;

void AppendAnyValue(StringBuilder& sb, const AnyValue& value);

void AppendAttributes(StringBuilder& sb, const Attributes& attributes) {
  std::vector<const KeyValue*> sorted;
  sorted.reserve(attributes.size());
  for (const auto& kv : attributes) {
    sorted.push_back(&kv);
  }
  absl::c_stable_sort(sorted, [](const KeyValue* lhs, const KeyValue* rhs) {
    return lhs->key() < rhs->key();
  });
  sb.start_object();
  bool first = true;
  for (const auto* kv : sorted) {
    if (!first) {
      sb.append_comma();
    }
    first = false;
    sb.escape_and_append_with_quotes(kv->key());
    sb.append_colon();
    AppendAnyValue(sb, kv->value());
  }
  sb.end_object();
}

void AppendAnyValue(StringBuilder& sb, const AnyValue& value) {
  switch (value.value_case()) {
    case AnyValue::kStringValue:
      sb.escape_and_append_with_quotes(value.string_value());
      return;
    case AnyValue::kBoolValue:
      sb.append_raw(value.bool_value() ? "true" : "false");
      return;
    case AnyValue::kIntValue:
      sb.append(value.int_value());
      return;
    case AnyValue::kDoubleValue:
      sb.append(value.double_value());
      return;
    case AnyValue::kBytesValue:
      sb.escape_and_append_with_quotes(absl::Base64Escape(value.bytes_value()));
      return;
    case AnyValue::kArrayValue: {
      sb.start_array();
      bool first = true;
      for (const auto& element : value.array_value().values()) {
        if (!first) {
          sb.append_comma();
        }
        first = false;
        AppendAnyValue(sb, element);
      }
      sb.end_array();
      return;
    }
    case AnyValue::kKvlistValue:
      AppendAttributes(sb, value.kvlist_value().values());
      return;
    default:
      sb.append_null();
      return;
  }
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

void AppendIdOrNull(StringBuilder& sb, std::string_view raw) {
  const auto hex = HexId(raw);
  if (hex.empty()) {
    sb.append_null();
  } else {
    sb.escape_and_append_with_quotes(hex);
  }
}

}  // namespace

std::string AttributesToJson(const Attributes& attributes) {
  StringBuilder sb;
  AppendAttributes(sb, attributes);
  return Finish(sb);
}

std::string BodyToText(const AnyValue& body) {
  switch (body.value_case()) {
    case AnyValue::kStringValue:
      return body.string_value();
    case AnyValue::kBytesValue:
      return body.bytes_value();
    case AnyValue::VALUE_NOT_SET:
      return {};
    default:
      break;
  }
  StringBuilder sb;
  AppendAnyValue(sb, body);
  return Finish(sb);
}

const AnyValue* FindAttribute(const Attributes& attributes,
                              std::string_view key) {
  for (const auto& kv : attributes) {
    if (kv.key() == key) {
      return &kv.value();
    }
  }
  return nullptr;
}

std::string HexId(std::string_view raw) {
  if (raw.empty() || raw.find_first_not_of('\0') == std::string_view::npos) {
    return {};
  }
  return absl::BytesToHexString(raw);
}

std::string_view SpanKindName(pb::trace::v1::Span_SpanKind kind) {
  switch (kind) {
    case pb::trace::v1::Span_SpanKind_SPAN_KIND_INTERNAL:
      return "Internal";
    case pb::trace::v1::Span_SpanKind_SPAN_KIND_SERVER:
      return "Server";
    case pb::trace::v1::Span_SpanKind_SPAN_KIND_CLIENT:
      return "Client";
    case pb::trace::v1::Span_SpanKind_SPAN_KIND_PRODUCER:
      return "Producer";
    case pb::trace::v1::Span_SpanKind_SPAN_KIND_CONSUMER:
      return "Consumer";
    default:
      return "Unspecified";
  }
}

std::string_view StatusCodeName(pb::trace::v1::Status_StatusCode code) {
  switch (code) {
    case pb::trace::v1::Status_StatusCode_STATUS_CODE_OK:
      return "Ok";
    case pb::trace::v1::Status_StatusCode_STATUS_CODE_ERROR:
      return "Error";
    default:
      return "Unset";
  }
}

std::string_view TemporalityName(
  pb::metrics::v1::AggregationTemporality temporality) {
  switch (temporality) {
    case pb::metrics::v1::AGGREGATION_TEMPORALITY_DELTA:
      return "Delta";
    case pb::metrics::v1::AGGREGATION_TEMPORALITY_CUMULATIVE:
      return "Cumulative";
    default:
      return "Unspecified";
  }
}

std::string EventsToJson(
  const ::google::protobuf::RepeatedPtrField<pb::trace::v1::Span_Event>&
    events) {
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
    AppendAttributes(sb, event.attributes());
    sb.append_comma();
    sb.escape_and_append_with_quotes("dropped_attributes_count");
    sb.append_colon();
    sb.append(static_cast<uint64_t>(event.dropped_attributes_count()));
    sb.append_comma();
    sb.escape_and_append_with_quotes("name");
    sb.append_colon();
    sb.escape_and_append_with_quotes(event.name());
    sb.append_comma();
    sb.escape_and_append_with_quotes("timestamp");
    sb.append_colon();
    sb.escape_and_append_with_quotes(FormatTimestampNs(event.time_unix_nano()));
    sb.end_object();
  }
  sb.end_array();
  return Finish(sb);
}

std::string LinksToJson(
  const ::google::protobuf::RepeatedPtrField<pb::trace::v1::Span_Link>& links) {
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
    AppendAttributes(sb, link.attributes());
    sb.append_comma();
    sb.escape_and_append_with_quotes("span_id");
    sb.append_colon();
    AppendIdOrNull(sb, link.span_id());
    sb.append_comma();
    sb.escape_and_append_with_quotes("trace_id");
    sb.append_colon();
    AppendIdOrNull(sb, link.trace_id());
    sb.append_comma();
    sb.escape_and_append_with_quotes("trace_state");
    sb.append_colon();
    if (link.trace_state().empty()) {
      sb.append_null();
    } else {
      sb.escape_and_append_with_quotes(link.trace_state());
    }
    sb.end_object();
  }
  sb.end_array();
  return Finish(sb);
}

std::string ExemplarsToJson(
  const ::google::protobuf::RepeatedPtrField<Exemplar>& exemplars) {
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
    AppendAttributes(sb, exemplar.filtered_attributes());
    sb.append_comma();
    sb.escape_and_append_with_quotes("span_id");
    sb.append_colon();
    AppendIdOrNull(sb, exemplar.span_id());
    sb.append_comma();
    sb.escape_and_append_with_quotes("timestamp");
    sb.append_colon();
    sb.escape_and_append_with_quotes(
      FormatTimestampNs(exemplar.time_unix_nano()));
    sb.append_comma();
    sb.escape_and_append_with_quotes("trace_id");
    sb.append_colon();
    AppendIdOrNull(sb, exemplar.trace_id());
    sb.append_comma();
    sb.escape_and_append_with_quotes("value");
    sb.append_colon();
    if (exemplar.has_as_int()) {
      sb.append(static_cast<double>(exemplar.as_int()));
    } else if (exemplar.has_as_double()) {
      sb.append(exemplar.as_double());
    } else {
      sb.append_null();
    }
    sb.end_object();
  }
  sb.end_array();
  return Finish(sb);
}

}  // namespace sdb::otel
