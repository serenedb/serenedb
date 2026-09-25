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

#include <algorithm>
#include <array>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <memory>
#include <optional>
#include <protozero/pbf_writer.hpp>
#include <span>
#include <string>
#include <string_view>

#include "connector/functions/otel.h"
#include "network/http/common.h"
#include "network/http/handler.h"
#include "network/pg/wire_frames.h"
#include "otel/model.h"
#include "otel/protobuf.h"
#include "otel/protojson.h"
#include "pg/sql_utils.h"

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

namespace {

inline constexpr std::string_view kProtobufContentType =
  "application/x-protobuf";

// google.rpc.Code, the enum google.rpc.Status carries:
// https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
inline constexpr int32_t kCodeInvalidArgument = 3;
inline constexpr int32_t kCodeInternal = 13;
inline constexpr int32_t kCodeUnavailable = 14;

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
  HttpStatus status = HttpStatus::Ok;
  int32_t code = 0;
  std::string error;
};

// https://opentelemetry.io/docs/specs/otlp/#failures-1
InsertOutcome Failed(const duckdb::ErrorData& error) {
  auto sql = network::pg::DuckErrorToSqlData(error);
  if (sql.errcode == ERRCODE_UNDEFINED_TABLE) {
    return {HttpStatus::InternalError, kCodeInternal,
            absl::StrCat("the OpenTelemetry schema is missing: ", sql.errmsg)};
  }
  if (sql.errcode == ERRCODE_INVALID_TABLE_DEFINITION) {
    return {HttpStatus::InternalError, kCodeInternal, std::move(sql.errmsg)};
  }
  char state[pg::kSqlStateSize];
  pg::UnpackSqlState(state, sql.errcode);
  const std::string_view sql_class{state, 2};
  if (sql_class == "22" || sql_class == "23") {
    return {HttpStatus::BadRequest, kCodeInvalidArgument,
            std::move(sql.errmsg)};
  }
  if (sql_class == "40" || sql_class == "53" || sql_class == "57" ||
      sql_class == "58") {
    return {HttpStatus::ServiceUnavailable, kCodeUnavailable,
            std::move(sql.errmsg)};
  }
  return {HttpStatus::InternalError, kCodeInternal, std::move(sql.errmsg)};
}

template<typename Signal>
std::optional<duckdb::ErrorData> EnsurePrepared(RequestContext& ctx,
                                                size_t target,
                                                network::PreparedEntry& entry) {
  using Box = connector::OtelRequestBox<typename Signal::Request>;
  try {
    auto& connection = ctx.Connection();
    auto& context = *connection.context;
    if (entry.statement != nullptr) {
      bool stale = false;
      context.RunFunctionInTransaction([&] {
        stale = entry.statement->data->RequireRebind(context, nullptr);
      });
      if (stale) {
        entry.statement.reset();
      }
    }
    if (entry.statement == nullptr) {
      auto box = duckdb::make_shared_ptr<Box>();
      auto statement = connection.Prepare(Signal::Insert(target, box));
      if (statement->HasError()) {
        // Not cached: the schema may appear later.
        return statement->GetErrorObject();
      }
      entry.statement = std::move(statement);
      entry.info = std::move(box);
    }
  } catch (const std::exception& error) {
    return duckdb::ErrorData{error};
  }
  return std::nullopt;
}

template<typename Signal>
yaclib::Task<InsertOutcome> RunSourceInsert(
  RequestContext& ctx, size_t target, const typename Signal::Request& request) {
  using Box = connector::OtelRequestBox<typename Signal::Request>;
  auto& entry = ctx.PreparedSlot(Signal::kTargets[target]);
  if (auto error = EnsurePrepared<Signal>(ctx, target, entry)) {
    co_return Failed(*error);
  }
  auto& box = static_cast<Box&>(*entry.info);
  box.request = &request;
  const absl::Cleanup clear = [&] { box.request = nullptr; };
  auto result = co_await ctx.RunPrepared(*entry.statement);
  if (!result->HasError()) {
    co_return InsertOutcome{};
  }
  co_return Failed(result->GetErrorObject());
}

struct LogsSignal {
  using Request = ExportLogsRequest;
  static constexpr std::string_view kRejectedField = "rejectedLogRecords";
  static constexpr std::array<std::string_view, 1> kTargets{
    connector::kOtelLogsTable};

  static void Decode(std::string_view raw, bool protobuf,
                     simdjson::ondemand::parser& parser, Request& out) {
    if (protobuf) {
      DecodeLogsRequest(raw, out);
    } else {
      ParseLogsRequest(raw, parser, out, /*padded=*/true);
    }
  }

  static auto Insert(size_t, duckdb::shared_ptr<connector::OtelLogsBox> box) {
    return connector::OtelLogsInsert(std::move(box));
  }
};

struct TracesSignal {
  using Request = ExportTracesRequest;
  static constexpr std::string_view kRejectedField = "rejectedSpans";
  static constexpr std::array<std::string_view, 1> kTargets{
    connector::kOtelTracesTable};

  static void Decode(std::string_view raw, bool protobuf,
                     simdjson::ondemand::parser& parser, Request& out) {
    if (protobuf) {
      DecodeTracesRequest(raw, out);
    } else {
      ParseTracesRequest(raw, parser, out, /*padded=*/true);
    }
  }

  static auto Insert(size_t, duckdb::shared_ptr<connector::OtelTracesBox> box) {
    return connector::OtelTracesInsert(std::move(box));
  }
};

// One payload feeds five tables; it is decoded once and each table's insert
// reads it.
struct MetricsSignal {
  using Request = ExportMetricsRequest;
  static constexpr std::string_view kRejectedField = "rejectedDataPoints";
  static constexpr auto& kTargets = connector::kOtelMetricTables;

  static void Decode(std::string_view raw, bool protobuf,
                     simdjson::ondemand::parser& parser, Request& out) {
    if (protobuf) {
      DecodeMetricsRequest(raw, out);
    } else {
      ParseMetricsRequest(raw, parser, out, /*padded=*/true);
    }
  }

  static auto Insert(size_t target,
                     duckdb::shared_ptr<connector::OtelMetricsBox> box) {
    return connector::OtelMetricsInsert(target, std::move(box));
  }
};

// Answers a failed insert; false when there is nothing to answer.
bool WriteFailure(HttpResponseWriter& writer, const InsertOutcome& outcome,
                  bool protobuf) {
  if (outcome.status == HttpStatus::Ok) {
    return false;
  }
  WriteStatus(writer, outcome.status, outcome.code, outcome.error, protobuf);
  return true;
}

template<typename Signal>
class ExportHandler final : public HttpHandler {
 public:
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
    // One copy of the body, padded so the JSON parser reads it in place. The
    // decoded model's text points into it (protobuf) or into the parser.
    const std::string buffer = FlattenBody(request.body, kJsonPadding);
    const std::string_view raw{buffer.data(), buffer.size() - kJsonPadding};
    if (raw.empty()) {
      WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                  "empty request body", protobuf);
      co_return {};
    }

    simdjson::ondemand::parser parser;
    typename Signal::Request decoded;
    try {
      Signal::Decode(raw, protobuf, parser, decoded);
    } catch (const std::exception& error) {
      WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                  error.what(), protobuf);
      co_return {};
    }
    for (size_t target = 0; target < Signal::kTargets.size(); ++target) {
      const auto outcome =
        co_await RunSourceInsert<Signal>(ctx, target, decoded);
      if (WriteFailure(writer, outcome, protobuf)) {
        co_return {};
      }
    }
    // TODO(mkornaukhov) implement rejected field
    WriteExportResponse(writer, Signal::kRejectedField, 0, {}, protobuf);
    co_return {};
  }
};

}  // namespace

void RegisterHandlers(HttpRouter& router) {
  router.Add(HttpMethod::Post, "/v1/logs",
             std::make_unique<ExportHandler<LogsSignal>>());
  router.Add(HttpMethod::Post, "/v1/traces",
             std::make_unique<ExportHandler<TracesSignal>>());
  router.Add(HttpMethod::Post, "/v1/metrics",
             std::make_unique<ExportHandler<MetricsSignal>>());
}

}  // namespace sdb::otel
