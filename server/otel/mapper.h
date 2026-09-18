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

#pragma once

#include <google/protobuf/repeated_ptr_field.h>

#include <cstdint>
#include <string>
#include <string_view>

#include "otel/otlp.h"

namespace sdb::otel {

// https://opentelemetry.io/docs/specs/semconv/resource/#service
inline constexpr std::string_view kServiceNameKey = "service.name";
inline constexpr std::string_view kEventNameKey = "event.name";

using Attributes = ::google::protobuf::RepeatedPtrField<KeyValue>;

std::string AttributesToJson(const Attributes& attributes);

std::string BodyToText(const AnyValue& body);

const AnyValue* FindAttribute(const Attributes& attributes,
                              std::string_view key);

// Raw id bytes to lowercase hex; empty for an unset or all-zero id, which the
// mapping turns into SQL NULL.
std::string HexId(std::string_view raw);

std::string_view SpanKindName(pb::trace::v1::Span_SpanKind kind);

std::string_view StatusCodeName(pb::trace::v1::Status_StatusCode code);

std::string_view TemporalityName(
  pb::metrics::v1::AggregationTemporality temporality);

std::string EventsToJson(
  const ::google::protobuf::RepeatedPtrField<pb::trace::v1::Span_Event>&
    events);

std::string LinksToJson(
  const ::google::protobuf::RepeatedPtrField<pb::trace::v1::Span_Link>& links);

std::string ExemplarsToJson(
  const ::google::protobuf::RepeatedPtrField<Exemplar>& exemplars);

}  // namespace sdb::otel
