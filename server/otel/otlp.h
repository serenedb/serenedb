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

#include <opentelemetry/proto/collector/logs/v1/logs_service.pb.h>
#include <opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h>
#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

// The OTLP data model, generated from the vendored opentelemetry-proto by
// protoc; see third_party/opentelemetry-proto and server/otel/CMakeLists.txt.
// Both decoders (protojson.cpp, protobuf.cpp) fill these messages and
// mapper.cpp reads them, so every ingestion route produces the same rows.
namespace sdb::otel {

namespace pb = ::opentelemetry::proto;

using AnyValue = pb::common::v1::AnyValue;
using KeyValue = pb::common::v1::KeyValue;
using InstrumentationScope = pb::common::v1::InstrumentationScope;
using Resource = pb::resource::v1::Resource;

using LogRecord = pb::logs::v1::LogRecord;
using Span = pb::trace::v1::Span;
using Metric = pb::metrics::v1::Metric;
using Exemplar = pb::metrics::v1::Exemplar;

using ExportLogsRequest = pb::collector::logs::v1::ExportLogsServiceRequest;
using ExportTracesRequest = pb::collector::trace::v1::ExportTraceServiceRequest;
using ExportMetricsRequest =
  pb::collector::metrics::v1::ExportMetricsServiceRequest;

// One decoded metrics payload, shared by the five otlp_metrics_* binds it
// fans out into.
struct DecodedMetrics {
  ExportMetricsRequest request;
};

}  // namespace sdb::otel
