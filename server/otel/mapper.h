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

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "otel/model.h"

namespace sdb::otel {

inline constexpr std::string_view kServiceNameKey = "service.name";
inline constexpr std::string_view kEventNameKey = "event.name";

std::string AttributesToJson(const KeyValueList& attributes);

std::string AnyValueToJson(const AnyValue* value);

std::string BodyToText(const AnyValue* body);

const AnyValue* FindAttribute(const KeyValueList& attributes,
                              std::string_view key);

std::string_view SpanKindName(SpanKind kind);

std::string_view StatusCodeName(StatusCode code);

std::string_view TemporalityName(AggregationTemporality temporality);

std::string FormatTimestampNs(uint64_t unix_nano);

std::string EventsToJson(const std::vector<SpanEvent>& events);

std::string LinksToJson(const std::vector<SpanLink>& links);

std::string ExemplarsToJson(const std::vector<Exemplar>& exemplars);

}  // namespace sdb::otel
