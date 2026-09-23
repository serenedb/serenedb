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

#include "network/http/otel/handlers.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <array>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/star_expression.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/statement/insert_statement.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <memory>
#include <protozero/pbf_writer.hpp>
#include <span>
#include <string>
#include <string_view>

#include "connector/duckdb_client_state.h"
#include "connector/functions/otel.h"
#include "network/http/common.h"
#include "network/http/handler.h"
#include "otel/model.h"
#include "otel/protobuf.h"
#include "otel/protojson.h"
#include "pg/connection_context.h"

// Endpoint paths, response shapes and status codes follow the OTLP/HTTP spec:
// https://opentelemetry.io/docs/specs/otlp/#otlphttp
//
// The failure body is google.rpc.Status:
// https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto
namespace sdb::otel {

using network::HttpHandler;
using network::HttpHeader;
using network::HttpMethod;
using network::HttpRequest;
using network::HttpRouter;
using network::RequestContext;
using network::http::FlattenBody;
using network::http::HttpResponseWriter;
using network::http::HttpStatus;
using network::http::kJsonContentType;
using network::http::SqlIdentifier;
using network::http::SqlLiteral;

namespace {

inline constexpr std::string_view kProtobufContentType =
  "application/x-protobuf";

// google.rpc.Code, the enum google.rpc.Status carries:
// https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
inline constexpr int32_t kCodeInvalidArgument = 3;
inline constexpr int32_t kCodeInternal = 13;

// google/rpc/status.proto is not vendored -- it is googleapis, not protobuf --
// and the message is two fields, so it is written directly:
//   int32 code = 1; string message = 2;
// https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto
inline constexpr protozero::pbf_tag_type kStatusCodeField = 1;
inline constexpr protozero::pbf_tag_type kStatusMessageField = 2;

std::string EncodeStatus(int32_t code, std::string_view message) {
  std::string out;
  protozero::pbf_writer status{out};
  if (code != 0) {
    status.add_int32(kStatusCodeField, code);
  }
  if (!message.empty()) {
    status.add_string(kStatusMessageField, message.data(), message.size());
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

struct InsertOutcome {
  bool ok = false;
  bool missing_table = false;
  std::string error;
};

InsertOutcome Failed(std::string message) {
  const bool missing = message.find("does not exist") != std::string::npos;
  return {.ok = false, .missing_table = missing, .error = std::move(message)};
}

yaclib::Task<InsertOutcome> RunInsert(RequestContext& ctx, std::string sql) {
  auto result = co_await ctx.RunQuery(std::move(sql), /*writes=*/true);
  if (!result->HasError()) {
    co_return InsertOutcome{.ok = true};
  }
  co_return Failed(result->GetError());
}

// INSERT INTO public.otel_logs SELECT * FROM otel_source_logs(), built as an
// AST: nothing is parsed, and it is prepared once per connection.
duckdb::unique_ptr<duckdb::SQLStatement> SourceLogsInsert() {
  auto source = duckdb::make_uniq<duckdb::TableFunctionRef>();
  source->function = duckdb::make_uniq<duckdb::FunctionExpression>(
    duckdb::Identifier{connector::kOtelSourceLogsFunction},
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>{});
  auto select_node = duckdb::make_uniq<duckdb::SelectNode>();
  select_node->select_list.push_back(
    duckdb::make_uniq<duckdb::StarExpression>());
  select_node->from_table = std::move(source);
  auto select = duckdb::make_uniq<duckdb::SelectStatement>();
  select->node = std::move(select_node);

  auto node = duckdb::make_uniq<duckdb::InsertQueryNode>();
  node->SetQualifiedName(duckdb::Identifier{},
                         duckdb::Identifier{connector::kOtelSchema},
                         duckdb::Identifier{connector::kOtelLogsTable});
  node->select_statement = std::move(select);
  auto statement = duckdb::make_uniq<duckdb::InsertStatement>();
  statement->node = std::move(node);
  return statement;
}

yaclib::Task<InsertOutcome> RunSourceLogsInsert(RequestContext& ctx) {
  auto& prepared = ctx.PreparedSlot(connector::kOtelSourceLogsFunction);
  if (prepared == nullptr) {
    auto statement = ctx.Connection().Prepare(SourceLogsInsert());
    if (statement->HasError()) {
      // Not cached: the schema may appear later.
      co_return Failed(statement->GetError());
    }
    prepared = std::move(statement);
  }
  auto result = co_await ctx.RunPrepared(*prepared);
  if (!result->HasError()) {
    co_return InsertOutcome{.ok = true};
  }
  co_return Failed(result->GetError());
}

std::string InsertSql(std::string_view table, std::string_view function,
                      std::string_view body, bool protobuf) {
  return absl::StrCat("INSERT INTO ", SqlIdentifier(connector::kOtelSchema),
                      ".", SqlIdentifier(table), " SELECT * FROM ", function,
                      "(", SqlLiteral(body), ", ",
                      protobuf ? "'protobuf'" : "'json'", ")");
}

// Answers a failed insert; false when there is nothing to answer.
bool WriteFailure(HttpResponseWriter& writer, const InsertOutcome& outcome,
                  bool protobuf) {
  if (outcome.missing_table) {
    WriteStatus(
      writer, HttpStatus::InternalError, kCodeInternal,
      absl::StrCat("the OpenTelemetry schema is missing: ", outcome.error),
      protobuf);
    return true;
  }
  if (!outcome.ok) {
    WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                outcome.error, protobuf);
    return true;
  }
  return false;
}

class ExportHandler final : public HttpHandler {
 public:
  ExportHandler(
    std::span<const std::pair<std::string_view, std::string_view>> targets,
    std::string_view rejected_field, bool prepared_logs = false)
    : _targets{targets},
      _rejected_field{rejected_field},
      _prepared_logs{prepared_logs} {}

  yaclib::Task<> Handle(RequestContext& ctx, const HttpRequest& request,
                        HttpResponseWriter& writer) override {
    const bool protobuf = IsProtobufRequest(request);
    if (!protobuf && !IsJsonRequest(request)) {
      WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                  "unsupported Content-Type; expected application/json or "
                  "application/x-protobuf",
                  /*protobuf=*/false);
      co_return {};
    }
    // TODO(mkornaukhov) content encoding
    if (!request.Header(HttpHeader::ContentEncoding).empty()) {
      WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                  absl::StrCat("unsupported Content-Encoding: ",
                               request.Header(HttpHeader::ContentEncoding)),
                  protobuf);
      co_return {};
    }

    // One copy of the body, padded so the JSON parser reads it in place.
    const auto buffer = FlattenBody(request.body, kJsonPadding);
    const std::string_view raw{buffer.data(), buffer.size() - kJsonPadding};
    if (raw.empty()) {
      WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                  "empty request body", protobuf);
      co_return {};
    }

    if (_prepared_logs) {
      DecodedLogs logs;
      try {
        if (protobuf) {
          DecodeLogsRequest(raw, logs.request);
        } else {
          ParseLogsRequest(raw, logs.request, /*padded=*/true);
        }
      } catch (const std::exception& error) {
        WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                    error.what(), protobuf);
        co_return {};
      }
      auto& logs_ctx = connector::GetSereneDBContext(*ctx.Connection().context);
      logs_ctx.SetOtelLogs(&logs);
      const absl::Cleanup clear_logs = [&] { logs_ctx.SetOtelLogs(nullptr); };
      const auto outcome = co_await RunSourceLogsInsert(ctx);
      if (!WriteFailure(writer, outcome, protobuf)) {
        WriteExportResponse(writer, _rejected_field, 0, {}, protobuf);
      }
      co_return {};
    }

    // Metrics fan out into five tables; decoding per table would walk the
    // payload five times, so it is decoded once and left on the connection.
    DecodedMetrics decoded;
    auto& sdb_ctx = connector::GetSereneDBContext(*ctx.Connection().context);
    const absl::Cleanup clear_metrics = [&] {
      sdb_ctx.SetOtelMetrics(nullptr);
    };
    std::string body = protobuf ? absl::Base64Escape(raw) : std::string{raw};
    if (_targets.size() > 1) {
      try {
        if (protobuf) {
          DecodeMetricsRequest(raw, decoded.request);
        } else {
          ParseMetricsRequest(raw, decoded.request, /*padded=*/true);
        }
      } catch (const std::exception& error) {
        WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                    error.what(), protobuf);
        co_return {};
      }
      sdb_ctx.SetOtelMetrics(&decoded);
      body.clear();
    }

    for (const auto& [table, function] : _targets) {
      const auto outcome =
        co_await RunInsert(ctx, InsertSql(table, function, body, protobuf));
      if (WriteFailure(writer, outcome, protobuf)) {
        co_return {};
      }
    }
    // TODO(mkornaukhov) implement rejected field
    WriteExportResponse(writer, _rejected_field, 0, {}, protobuf);
    co_return {};
  }

 private:
  std::span<const std::pair<std::string_view, std::string_view>> _targets;
  std::string_view _rejected_field;
  bool _prepared_logs;
};

constexpr std::array<std::pair<std::string_view, std::string_view>, 1>
  kLogTargets{{
    {connector::kOtelLogsTable, "otel_parse_logs"},
  }};

constexpr std::array<std::pair<std::string_view, std::string_view>, 1>
  kTraceTargets{{
    {connector::kOtelTracesTable, "otel_parse_traces"},
  }};

constexpr std::array<std::pair<std::string_view, std::string_view>, 5>
  kMetricTargets{{
    {connector::kOtelMetricTables[0], "otel_parse_metrics_gauge"},
    {connector::kOtelMetricTables[1], "otel_parse_metrics_sum"},
    {connector::kOtelMetricTables[2], "otel_parse_metrics_histogram"},
    {connector::kOtelMetricTables[3],
     "otel_parse_metrics_exponential_histogram"},
    {connector::kOtelMetricTables[4], "otel_parse_metrics_summary"},
  }};

}  // namespace

void RegisterHandlers(HttpRouter& router) {
  router.Add(HttpMethod::Post, "/v1/logs",
             std::make_unique<ExportHandler>(kLogTargets, "rejectedLogRecords",
                                             /*prepared_logs=*/true));
  router.Add(HttpMethod::Post, "/v1/traces",
             std::make_unique<ExportHandler>(kTraceTargets, "rejectedSpans"));
  router.Add(
    HttpMethod::Post, "/v1/metrics",
    std::make_unique<ExportHandler>(kMetricTargets, "rejectedDataPoints"));
}

}  // namespace sdb::otel
