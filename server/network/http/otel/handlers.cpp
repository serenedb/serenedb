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
#include <zlib.h>

#include <algorithm>
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

// INSERT INTO public.<table> SELECT * FROM <function>(), built as an AST:
// nothing is parsed, and it is prepared once per connection.
duckdb::unique_ptr<duckdb::SQLStatement> SourceInsert(
  std::string_view table, std::string_view function) {
  auto source = duckdb::make_uniq<duckdb::TableFunctionRef>();
  source->function = duckdb::make_uniq<duckdb::FunctionExpression>(
    duckdb::Identifier{std::string{function}},
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
                         duckdb::Identifier{std::string{table}});
  node->select_statement = std::move(select);
  auto statement = duckdb::make_uniq<duckdb::InsertStatement>();
  statement->node = std::move(node);
  return statement;
}

struct Target {
  std::string_view table;
  std::string_view function;
};

yaclib::Task<InsertOutcome> RunSourceInsert(RequestContext& ctx,
                                            const Target& target) {
  auto& prepared = ctx.PreparedSlot(target.function);
  if (prepared == nullptr) {
    auto statement =
      ctx.Connection().Prepare(SourceInsert(target.table, target.function));
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

struct LogsSignal {
  using Decoded = DecodedLogs;
  static constexpr std::string_view kRejectedField = "rejectedLogRecords";
  static constexpr std::array<Target, 1> kTargets{{
    {connector::kOtelLogsTable, connector::kOtelSourceLogsFunction},
  }};

  static void Decode(std::string_view raw, bool protobuf, Decoded& out) {
    if (protobuf) {
      DecodeLogsRequest(raw, out.request);
    } else {
      ParseLogsRequest(raw, out.request, /*padded=*/true);
    }
  }

  static void Publish(ConnectionContext& ctx, const Decoded* decoded) {
    ctx.SetOtelLogs(decoded);
  }
};

struct TracesSignal {
  using Decoded = DecodedTraces;
  static constexpr std::string_view kRejectedField = "rejectedSpans";
  static constexpr std::array<Target, 1> kTargets{{
    {connector::kOtelTracesTable, connector::kOtelSourceTracesFunction},
  }};

  static void Decode(std::string_view raw, bool protobuf, Decoded& out) {
    if (protobuf) {
      DecodeTracesRequest(raw, out.request);
    } else {
      ParseTracesRequest(raw, out.request, /*padded=*/true);
    }
  }

  static void Publish(ConnectionContext& ctx, const Decoded* decoded) {
    ctx.SetOtelTraces(decoded);
  }
};

// One payload feeds five tables; it is decoded once and each table's insert
// reads it off the connection.
struct MetricsSignal {
  using Decoded = DecodedMetrics;
  static constexpr std::string_view kRejectedField = "rejectedDataPoints";
  static constexpr std::array<Target, 5> kTargets{{
    {connector::kOtelMetricTables[0],
     connector::kOtelSourceMetricsFunctions[0]},
    {connector::kOtelMetricTables[1],
     connector::kOtelSourceMetricsFunctions[1]},
    {connector::kOtelMetricTables[2],
     connector::kOtelSourceMetricsFunctions[2]},
    {connector::kOtelMetricTables[3],
     connector::kOtelSourceMetricsFunctions[3]},
    {connector::kOtelMetricTables[4],
     connector::kOtelSourceMetricsFunctions[4]},
  }};

  static void Decode(std::string_view raw, bool protobuf, Decoded& out) {
    if (protobuf) {
      DecodeMetricsRequest(raw, out.request);
    } else {
      ParseMetricsRequest(raw, out.request, /*padded=*/true);
    }
  }

  static void Publish(ConnectionContext& ctx, const Decoded* decoded) {
    ctx.SetOtelMetrics(decoded);
  }
};

// A decompressed request may not exceed this: a few KB of gzip can inflate to
// gigabytes.
inline constexpr size_t kMaxInflatedBytes = size_t{256} << 20;

// Inflates a gzip body straight from the receive chunks into a buffer that
// ends with kJsonPadding zero bytes, like FlattenBody's. Empty on failure,
// with `error` set.
// https://opentelemetry.io/docs/specs/otlp/#otlphttp-request
std::string InflateGzip(const message::SequenceView& body, std::string& error) {
  z_stream stream{};
  // 16 + MAX_WBITS: a gzip wrapper, not raw deflate or zlib.
  if (inflateInit2(&stream, 16 + MAX_WBITS) != Z_OK) {
    error = "gzip: cannot initialize the decoder";
    return {};
  }
  const absl::Cleanup end = [&] { inflateEnd(&stream); };
  std::string out;
  size_t size = 0;
  int rc = Z_OK;
  for (const auto chunk : body) {
    stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(chunk.data()));
    stream.avail_in = static_cast<uInt>(chunk.size());
    while (stream.avail_in != 0 && rc != Z_STREAM_END) {
      if (out.size() - size < 64 * 1024) {
        out.resize(std::max<size_t>(out.size() * 2, 1 << 20));
      }
      stream.next_out = reinterpret_cast<Bytef*>(out.data() + size);
      stream.avail_out = static_cast<uInt>(out.size() - size);
      rc = inflate(&stream, Z_NO_FLUSH);
      size = out.size() - stream.avail_out;
      if (rc != Z_OK && rc != Z_STREAM_END) {
        error = absl::StrCat("gzip: ", stream.msg ? stream.msg : zError(rc));
        return {};
      }
      if (size > kMaxInflatedBytes) {
        error = absl::StrCat("gzip: decompressed body exceeds ",
                             kMaxInflatedBytes, " bytes");
        return {};
      }
    }
  }
  if (rc != Z_STREAM_END) {
    error = "gzip: truncated body";
    return {};
  }
  out.resize(size);
  out.append(kJsonPadding, '\0');
  return out;
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
    const auto encoding = request.Header(HttpHeader::ContentEncoding);
    const bool gzip = absl::EqualsIgnoreCase(encoding, "gzip");
    if (!encoding.empty() && !gzip &&
        !absl::EqualsIgnoreCase(encoding, "identity")) {
      WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                  absl::StrCat("unsupported Content-Encoding: ", encoding,
                               "; expected gzip"),
                  protobuf);
      co_return {};
    }

    // One copy of the body, padded so the JSON parser reads it in place. The
    // decoded model's text points into it (protobuf) or into the parser.
    std::string buffer;
    if (gzip) {
      std::string error;
      buffer = InflateGzip(request.body, error);
      if (buffer.empty()) {
        WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument, error,
                    protobuf);
        co_return {};
      }
    } else {
      buffer = FlattenBody(request.body, kJsonPadding);
    }
    const std::string_view raw{buffer.data(), buffer.size() - kJsonPadding};
    if (raw.empty()) {
      WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                  "empty request body", protobuf);
      co_return {};
    }

    typename Signal::Decoded decoded;
    try {
      Signal::Decode(raw, protobuf, decoded);
    } catch (const std::exception& error) {
      WriteStatus(writer, HttpStatus::BadRequest, kCodeInvalidArgument,
                  error.what(), protobuf);
      co_return {};
    }
    auto& sdb_ctx = connector::GetSereneDBContext(*ctx.Connection().context);
    Signal::Publish(sdb_ctx, &decoded);
    const absl::Cleanup unpublish = [&] { Signal::Publish(sdb_ctx, nullptr); };

    for (const auto& target : Signal::kTargets) {
      const auto outcome = co_await RunSourceInsert(ctx, target);
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
