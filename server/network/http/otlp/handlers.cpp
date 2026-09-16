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

#include "network/http/otlp/handlers.h"

#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <array>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "connector/functions/otlp.h"
#include "network/http/common.h"
#include "network/http/handler.h"
#include "otel/schema_sql.h"

namespace sdb::network::http::otlp {
namespace {

inline constexpr std::string_view kProtobufContentType =
  "application/x-protobuf";

void AppendVarint(std::string& out, uint64_t value) {
  while (value >= 0x80) {
    out.push_back(static_cast<char>((value & 0x7F) | 0x80));
    value >>= 7;
  }
  out.push_back(static_cast<char>(value));
}

void AppendTag(std::string& out, uint32_t field, uint32_t wire_type) {
  AppendVarint(out, (static_cast<uint64_t>(field) << 3) | wire_type);
}

// google.rpc.Status: code = 1 (varint), message = 2 (length-delimited).
std::string EncodeStatus(int32_t code, std::string_view message) {
  std::string out;
  if (code != 0) {
    AppendTag(out, 1, 0);
    AppendVarint(out, static_cast<uint64_t>(code));
  }
  if (!message.empty()) {
    AppendTag(out, 2, 2);
    AppendVarint(out, message.size());
    out.append(message);
  }
  return out;
}

// google.rpc.Status, the body the OTLP spec asks for on a failed export. The
// response encoding must match the request's.
void WriteStatus(HttpResponseWriter& writer, HttpStatus status, int32_t code,
                 std::string_view message, bool protobuf) {
  if (protobuf) {
    writer.Fixed(status, kProtobufContentType, EncodeStatus(code, message));
    return;
  }
  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"code":)");
  sb.append(static_cast<int64_t>(code));
  sb.append_raw(R"(,"message":)");
  sb.escape_and_append_with_quotes(message);
  sb.append_raw(R"(,"details":[]})");
  auto view = sb.view();
  writer.Json(status, view.error() == simdjson::SUCCESS
                        ? view.value()
                        : std::string_view{R"({"code":13})"});
}

// A full success is the empty Export<Signal>ServiceResponse; a partial one
// carries the rejected count, which clients must not retry.
void WriteExportResponse(HttpResponseWriter& writer, std::string_view field,
                         int64_t rejected, std::string_view error_message,
                         bool protobuf) {
  if (rejected == 0) {
    // An empty Export<Signal>ServiceResponse: no bytes on the wire, `{}` in
    // ProtoJSON.
    if (protobuf) {
      writer.Fixed(HttpStatus::Ok, kProtobufContentType, {});
    } else {
      writer.Json(HttpStatus::Ok, "{}");
    }
    return;
  }
  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"partialSuccess":{")");
  sb.append_raw(field);
  sb.append_raw(R"(":)");
  sb.append(rejected);
  sb.append_raw(R"(,"errorMessage":)");
  sb.escape_and_append_with_quotes(error_message);
  sb.append_raw("}}");
  auto view = sb.view();
  writer.Json(HttpStatus::Ok, view.error() == simdjson::SUCCESS
                                ? view.value()
                                : std::string_view{"{}"});
}

bool IsJsonRequest(const HttpRequest& request) {
  const auto content_type = request.Header(HttpHeader::ContentType);
  return content_type.empty() ||
         absl::StartsWithIgnoreCase(content_type, "application/json");
}

bool IsProtobufRequest(const HttpRequest& request) {
  return absl::StartsWithIgnoreCase(request.Header(HttpHeader::ContentType),
                                    "application/x-protobuf");
}

yaclib::Task<bool> CreateSchema(RequestContext& ctx) {
  for (const auto statement : otel::kSchemaStatements) {
    auto result =
      co_await ctx.RunQuery(std::string{statement}, /*writes=*/true);
    if (result->HasError()) {
      co_return false;
    }
  }
  co_return true;
}

struct InsertOutcome {
  bool ok = false;
  bool missing_table = false;
  std::string error;
};

yaclib::Task<InsertOutcome> RunInsert(RequestContext& ctx, std::string sql) {
  auto result = co_await ctx.RunQuery(std::move(sql), /*writes=*/true);
  if (!result->HasError()) {
    co_return InsertOutcome{.ok = true};
  }
  const auto message = result->GetError();
  const bool missing = message.find("does not exist") != std::string::npos;
  co_return InsertOutcome{
    .ok = false, .missing_table = missing, .error = message};
}

std::string InsertSql(std::string_view table, std::string_view function,
                      std::string_view body, bool protobuf) {
  return absl::StrCat("INSERT INTO ", SqlIdentifier(connector::kOtelSchema),
                      ".", SqlIdentifier(table), " SELECT * FROM ", function,
                      "(", SqlLiteral(body), ", ",
                      protobuf ? "'protobuf'" : "'json'", ")");
}

class ExportHandler final : public HttpHandler {
 public:
  ExportHandler(
    std::span<const std::pair<std::string_view, std::string_view>> targets,
    std::string_view rejected_field)
    : _targets{targets}, _rejected_field{rejected_field} {}

  yaclib::Task<> Handle(RequestContext& ctx, const HttpRequest& request,
                        HttpResponseWriter& writer) override {
    const bool protobuf = IsProtobufRequest(request);
    if (!protobuf && !IsJsonRequest(request)) {
      WriteStatus(writer, HttpStatus::BadRequest, 3,
                  "unsupported Content-Type; expected application/json or "
                  "application/x-protobuf",
                  /*protobuf=*/false);
      co_return {};
    }
    if (!request.Header(HttpHeader::ContentEncoding).empty()) {
      WriteStatus(writer, HttpStatus::BadRequest, 3,
                  absl::StrCat("unsupported Content-Encoding: ",
                               request.Header(HttpHeader::ContentEncoding)),
                  protobuf);
      co_return {};
    }

    const auto raw = FlattenBody(request.body);
    if (raw.empty()) {
      WriteStatus(writer, HttpStatus::BadRequest, 3, "empty request body",
                  protobuf);
      co_return {};
    }
    const auto body = protobuf ? absl::Base64Escape(raw) : raw;

    bool created = false;
    for (const auto& [table, function] : _targets) {
      auto outcome =
        co_await RunInsert(ctx, InsertSql(table, function, body, protobuf));
      if (!outcome.ok && outcome.missing_table && !created) {
        if (!co_await CreateSchema(ctx)) {
          WriteStatus(writer, HttpStatus::InternalError, 13,
                      "the OpenTelemetry schema is missing and could not be "
                      "created",
                      protobuf);
          co_return {};
        }
        created = true;
        outcome =
          co_await RunInsert(ctx, InsertSql(table, function, body, protobuf));
      }
      if (!outcome.ok) {
        WriteStatus(writer, HttpStatus::BadRequest, 3, outcome.error, protobuf);
        co_return {};
      }
    }
    WriteExportResponse(writer, _rejected_field, 0, {}, protobuf);
    co_return {};
  }

 private:
  std::span<const std::pair<std::string_view, std::string_view>> _targets;
  std::string_view _rejected_field;
};

constexpr std::array<std::pair<std::string_view, std::string_view>, 1>
  kLogTargets{{
    {connector::kOtelLogsTable, "otlp_logs"},
  }};

constexpr std::array<std::pair<std::string_view, std::string_view>, 1>
  kTraceTargets{{
    {connector::kOtelTracesTable, "otlp_traces"},
  }};

constexpr std::array<std::pair<std::string_view, std::string_view>, 5>
  kMetricTargets{{
    {connector::kOtelMetricTables[0], "otlp_metrics_gauge"},
    {connector::kOtelMetricTables[1], "otlp_metrics_sum"},
    {connector::kOtelMetricTables[2], "otlp_metrics_histogram"},
    {connector::kOtelMetricTables[3], "otlp_metrics_exponential_histogram"},
    {connector::kOtelMetricTables[4], "otlp_metrics_summary"},
  }};

}  // namespace

void Register(HttpRouter& router) {
  router.Add(
    HttpMethod::Post, "/v1/logs",
    std::make_unique<ExportHandler>(kLogTargets, "rejectedLogRecords"));
  router.Add(HttpMethod::Post, "/v1/traces",
             std::make_unique<ExportHandler>(kTraceTargets, "rejectedSpans"));
  router.Add(
    HttpMethod::Post, "/v1/metrics",
    std::make_unique<ExportHandler>(kMetricTargets, "rejectedDataPoints"));
}

}  // namespace sdb::network::http::otlp
