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

#include "network/http/es/handlers.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <simdjson.h>

#include <chrono>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <memory>
#include <yaclib/async/make.hpp>

#include "connector/duckdb_client_state.h"
#include "connector/functions/es.h"
#include "network/http/es/common.h"
#include "network/http/es/dsl.h"
#include "network/http/handler.h"
#include "pg/connection_context.h"

namespace sdb::network::http::es {
namespace {

// One es_*() call per request, driven cooperatively (RunQuery yields the
// scheduler worker between executor slices). A failed call has already been
// written out as an ES error envelope when this resolves null.
yaclib::Future<duckdb::unique_ptr<duckdb::MaterializedQueryResult>> RunSql(
  RequestContext& ctx, std::string sql, http::HttpResponseWriter& writer,
  std::string_view index = {}, bool writes = false) {
  auto result = co_await ctx.RunQuery(std::move(sql), writes);
  if (result->HasError()) {
    WriteSqlError(writer, result->GetErrorObject(), index);
    co_return nullptr;
  }
  co_return std::move(result);
}

// ?refresh, ?refresh=true and ?refresh=wait_for all refresh synchronously
// (CommitWait IS the wait); only an explicit false is a no-op.
bool WantsRefresh(const HttpRequest& request) {
  for (const auto& [key, value] : request.query) {
    if (key == "refresh") {
      return value != "false";
    }
  }
  return false;
}

yaclib::Future<bool> MaybeRefresh(RequestContext& ctx,
                                  const HttpRequest& request,
                                  std::string_view index,
                                  http::HttpResponseWriter& writer) {
  if (!WantsRefresh(request)) {
    co_return true;
  }
  auto result = co_await RunSql(
    ctx, absl::StrCat("CALL es_refresh(", SqlLiteral(index), ")"), writer,
    index);
  co_return result != nullptr;
}

int64_t TookMs(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::steady_clock::now() - start)
    .count();
}

// The official clients post to bare /_bulk and route via per-line _index;
// peek the FIRST action line for it (es_bulk enforces that every line names
// the same index — multi-index bulks are not supported).
std::string FirstActionIndex(std::string_view body) {
  const auto line = body.substr(0, body.find('\n'));
  try {
    simdjson::padded_string padded{line};
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document doc;
    if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
      return {};
    }
    for (auto action : doc.get_object()) {
      for (auto param : action.value().get_object()) {
        std::string_view key;
        if (param.unescaped_key().get(key) != simdjson::SUCCESS) {
          return {};
        }
        if (key == "_index") {
          std::string_view index;
          if (param.value().get_string().get(index) == simdjson::SUCCESS) {
            return std::string{index};
          }
          return {};
        }
      }
    }
  } catch (const std::exception&) {
  }
  return {};
}

// POST|PUT [/{index}]/_bulk
class BulkHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    auto body = FlattenBody(request.body);
    std::string index{request.Param("index")};
    if (index.empty()) {
      index = FirstActionIndex(body);
    }
    if (index.empty()) {
      WriteError(writer, 400, "illegal_argument_exception",
                 "an index is required in the request URL or in the action "
                 "metadata");
      co_return {};
    }
    const auto start = std::chrono::steady_clock::now();

    // es_bulk fills the items array through the side channel while the
    // INSERT runs (serenedb INSERT has no RETURNING). The sink is on the
    // ConnectionContext that RunQuery drives through.
    auto& sdb_ctx = connector::GetSereneDBContext(*ctx.Connection().context);
    std::string items;
    sdb_ctx.SetResponseSink(&items);
    const absl::Cleanup clear_sink = [&] { sdb_ctx.SetResponseSink(nullptr); };

    const auto sql = absl::StrCat(
      "INSERT INTO \"es\".", SqlIdentifier(index), " SELECT * FROM es_bulk(",
      SqlLiteral(index), ", ", SqlLiteral(body), ")");
    if (!co_await RunSql(ctx, sql, writer, index, /*writes=*/true)) {
      co_return {};
    }
    if (!co_await MaybeRefresh(ctx, request, index, writer)) {
      co_return {};
    }
    WriteJson(writer, 200, absl::StrCat("{\"took\":", TookMs(start),
                                  ",\"errors\":false,\"items\":[", items,
                                  "]}"));
    co_return {};
  }
};

// POST /{index}/_doc, PUT|POST /{index}/_doc/{id}
class DocHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto index = request.Param("index");
    std::string id{request.Param("id")};
    if (id.empty()) {
      id = connector::GenerateEsDocId();
    }
    const auto sql = absl::StrCat(
      "INSERT INTO \"es\".", SqlIdentifier(index), " SELECT * FROM es_doc(",
      SqlLiteral(index), ", ", SqlLiteral(id), ", ",
      SqlLiteral(FlattenBody(request.body)), ")");
    if (!co_await RunSql(ctx, sql, writer, index, /*writes=*/true) ||
        !co_await MaybeRefresh(ctx, request, index, writer)) {
      co_return {};
    }
    simdjson::builder::string_builder sb;
    sb.append_raw(R"({"_index":)");
    sb.escape_and_append_with_quotes(index);
    sb.append_raw(R"(,"_id":)");
    sb.escape_and_append_with_quotes(std::string_view{id});
    sb.append_raw(R"(,"_version":1,"result":"created","_shards":{"total":1,)"
                  R"("successful":1,"failed":0},"_seq_no":0,)"
                  R"("_primary_term":1})");
    WriteJson(writer, 201, std::string_view{sb.view().value()});
    co_return {};
  }
};

// POST|GET /{index}/_refresh and /_refresh ('' = every ES index)
class RefreshHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto sql = absl::StrCat("CALL es_refresh(",
                                  SqlLiteral(request.Param("index")), ")");
    if (co_await RunSql(ctx, sql, writer)) {
      WriteJson(writer, 200, R"({"_shards":{"total":1,"successful":1,"failed":0}})");
    }
    co_return {};
  }
};

// GET /{index}/_doc/{id}: a missing document is 404 with a GetResult body,
// not the error envelope (a missing index IS the envelope).
class GetDocHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto index = request.Param("index");
    const auto id = request.Param("id");
    auto result = co_await RunSql(
      ctx,
      absl::StrCat("SELECT \"_source\" FROM \"es\".", SqlIdentifier(index),
                   " WHERE \"_id\" = ", SqlLiteral(id)),
      writer, index);
    if (!result) {
      co_return {};
    }
    simdjson::builder::string_builder sb;
    sb.append_raw(R"({"_index":)");
    sb.escape_and_append_with_quotes(index);
    sb.append_raw(R"(,"_id":)");
    sb.escape_and_append_with_quotes(id);
    if (result->RowCount() == 0) {
      sb.append_raw(R"(,"found":false})");
      WriteJson(writer, 404, std::string_view{sb.view().value()});
      co_return {};
    }
    const auto source = result->GetValue(0, 0).GetValue<std::string>();
    sb.append_raw(R"(,"_version":1,"_seq_no":0,"_primary_term":1,)"
                  R"("found":true,"_source":)");
    sb.append_raw(source);
    sb.append_raw("}");
    WriteJson(writer, 200, std::string_view{sb.view().value()});
    co_return {};
  }
};

// GET /{index}/_source/{id}: the raw stored document; missing doc is the
// error envelope here, unlike _doc.
class GetSourceHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto index = request.Param("index");
    const auto id = request.Param("id");
    auto result = co_await RunSql(
      ctx,
      absl::StrCat("SELECT \"_source\" FROM \"es\".", SqlIdentifier(index),
                   " WHERE \"_id\" = ", SqlLiteral(id)),
      writer, index);
    if (!result) {
      co_return {};
    }
    if (result->RowCount() == 0) {
      WriteError(writer, 404, "resource_not_found_exception",
                 absl::StrCat("Document not found [", index, "]/[", id, "]"));
      co_return {};
    }
    WriteJson(writer, 200, result->GetValue(0, 0).GetValue<std::string>());
    co_return {};
  }
};

// match queries plan only against the inverted index relation; the index
// snapshot also gives them ES refresh visibility. Filter-only queries scan
// the table.
std::string SearchRelation(std::string_view index, const SearchRequest& spec) {
  return spec.uses_match
           ? absl::StrCat("\"es\".", SqlIdentifier(absl::StrCat(
                                       index, connector::kEsTextIndexSuffix)))
           : absl::StrCat("\"es\".", SqlIdentifier(index));
}

// A sort value the way ES renders it: dates as epoch milliseconds.
void AppendSortValue(simdjson::builder::string_builder& sb,
                     const duckdb::Value& value) {
  if (value.IsNull()) {
    sb.append_raw("null");
    return;
  }
  switch (value.type().id()) {
    case duckdb::LogicalTypeId::VARCHAR:
      sb.escape_and_append_with_quotes(
        std::string_view{duckdb::StringValue::Get(value)});
      return;
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::INTEGER:
      sb.append(value.GetValue<int64_t>());
      return;
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::FLOAT:
      sb.append(value.GetValue<double>());
      return;
    case duckdb::LogicalTypeId::BOOLEAN:
      sb.append_raw(value.GetValue<bool>() ? "true" : "false");
      return;
    case duckdb::LogicalTypeId::TIMESTAMP:
      sb.append(value.GetValue<duckdb::timestamp_t>().value / 1000);
      return;
    default:
      sb.escape_and_append_with_quotes(std::string_view{value.ToString()});
      return;
  }
}

// A wildcard index pattern resolves against nothing here (no pattern
// support yet); like ES with allow_no_indices, that is an empty 200 without
// executing the query (rally telemetry searches .ml-anomalies-* this way).
bool IsIndexPattern(std::string_view index) {
  return index.find('*') != std::string_view::npos;
}

// --- scroll: a stateless keyset cursor -------------------------------------
// The scroll id is base64url of {index, raw query JSON, last "_id", size,
// exact total, flags}. Pages re-translate the embedded DSL (the id never
// carries SQL) and continue WHERE "_id" > cursor ORDER BY "_id". Unlike a
// real ES scroll context there is no frozen snapshot: rows written behind
// the cursor are skipped, ahead of it become visible.
struct ScrollState {
  std::string index;
  std::string query;
  std::string last_id;
  int64_t size = 0;
  int64_t total = 0;
  bool include_source = true;
  bool done = false;
};

std::string EncodeScrollId(const ScrollState& state) {
  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"i":)");
  sb.escape_and_append_with_quotes(std::string_view{state.index});
  sb.append_raw(R"(,"q":)");
  sb.escape_and_append_with_quotes(std::string_view{state.query});
  sb.append_raw(R"(,"a":)");
  sb.escape_and_append_with_quotes(std::string_view{state.last_id});
  sb.append_raw(R"(,"s":)");
  sb.append(state.size);
  sb.append_raw(R"(,"t":)");
  sb.append(state.total);
  sb.append_raw(R"(,"src":)");
  sb.append_raw(state.include_source ? "true" : "false");
  sb.append_raw(R"(,"d":)");
  sb.append_raw(state.done ? "true" : "false");
  sb.append_raw("}");
  return absl::WebSafeBase64Escape(std::string_view{sb.view().value()});
}

bool DecodeScrollId(std::string_view id, ScrollState& state) {
  std::string json;
  if (!absl::WebSafeBase64Unescape(id, &json)) {
    return false;
  }
  try {
    simdjson::padded_string padded{json};
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document doc;
    if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
      return false;
    }
    simdjson::ondemand::object object;
    if (doc.get_object().get(object) != simdjson::SUCCESS) {
      return false;
    }
    for (auto field : object) {
      std::string_view key;
      if (field.unescaped_key().get(key) != simdjson::SUCCESS) {
        return false;
      }
      auto value = field.value();
      bool ok = true;
      if (key == "i") {
        std::string_view v;
        ok = value.get_string().get(v) == simdjson::SUCCESS;
        state.index = v;
      } else if (key == "q") {
        std::string_view v;
        ok = value.get_string().get(v) == simdjson::SUCCESS;
        state.query = v;
      } else if (key == "a") {
        std::string_view v;
        ok = value.get_string().get(v) == simdjson::SUCCESS;
        state.last_id = v;
      } else if (key == "s") {
        ok = value.get_int64().get(state.size) == simdjson::SUCCESS;
      } else if (key == "t") {
        ok = value.get_int64().get(state.total) == simdjson::SUCCESS;
      } else if (key == "src") {
        ok = value.get_bool().get(state.include_source) == simdjson::SUCCESS;
      } else if (key == "d") {
        ok = value.get_bool().get(state.done) == simdjson::SUCCESS;
      }
      if (!ok) {
        return false;
      }
    }
  } catch (const std::exception&) {
    return false;
  }
  return !state.index.empty() && state.size >= 0;
}

// One scroll page; rows == nullptr renders the empty terminal page. Advances
// the cursor and embeds the refreshed id.
void WriteScrollPage(http::HttpResponseWriter& writer, ScrollState& state,
                     duckdb::MaterializedQueryResult* result, int64_t took) {
  const auto rows = result ? result->RowCount() : 0;
  if (result) {
    if (static_cast<int64_t>(rows) < state.size) {
      state.done = true;
    }
    if (rows > 0) {
      state.last_id =
        duckdb::StringValue::Get(result->GetValue(0, rows - 1));
    }
  }
  simdjson::builder::string_builder sb;
  sb.append_raw("{\"_scroll_id\":");
  sb.escape_and_append_with_quotes(std::string_view{EncodeScrollId(state)});
  sb.append_raw(",\"took\":");
  sb.append(took);
  sb.append_raw(R"(,"timed_out":false,"_shards":{"total":1,"successful":1,)"
                R"("skipped":0,"failed":0},"hits":{"total":{"value":)");
  sb.append(state.total);
  sb.append_raw(R"(,"relation":"eq"},"max_score":null,"hits":[)");
  for (duckdb::idx_t row = 0; row < rows; ++row) {
    if (row > 0) {
      sb.append_raw(",");
    }
    sb.append_raw(R"({"_index":)");
    sb.escape_and_append_with_quotes(std::string_view{state.index});
    sb.append_raw(R"(,"_id":)");
    sb.escape_and_append_with_quotes(
      std::string_view{duckdb::StringValue::Get(result->GetValue(0, row))});
    sb.append_raw(",\"_score\":null");
    if (state.include_source) {
      sb.append_raw(",\"_source\":");
      sb.append_raw(
        std::string_view{duckdb::StringValue::Get(result->GetValue(1, row))});
    }
    sb.append_raw("}");
  }
  sb.append_raw("]}}");
  WriteJson(writer, 200, std::string_view{sb.view().value()});
}

// The keyset page statement shared by the initial scroll search and
// continuations.
std::string ScrollPageSql(const std::string& relation,
                          const SearchRequest& spec,
                          const ScrollState& state) {
  std::string sql = "SELECT \"_id\"";
  if (state.include_source) {
    absl::StrAppend(&sql, ", \"_source\"");
  }
  absl::StrAppend(&sql, " FROM ", relation, " WHERE ",
                  spec.where.empty() ? "TRUE" : spec.where);
  if (!state.last_id.empty()) {
    absl::StrAppend(&sql, " AND \"_id\" > ", SqlLiteral(state.last_id));
  }
  absl::StrAppend(&sql, " ORDER BY \"_id\" LIMIT ", state.size);
  return sql;
}

// Appends `"name":{...}` to the aggregations fragment; each aggregation is
// one GROUP-BY/aggregate statement over the query's relation and WHERE.
// false = the error response is already written.
yaclib::Future<bool> RunAggregation(RequestContext& ctx,
                                    const Aggregation& agg,
                                    const std::string& relation,
                                    const std::string& where,
                                    std::string_view index,
                                    simdjson::builder::string_builder& sb,
                                    http::HttpResponseWriter& writer) {
  const auto field = SqlIdentifier(agg.field);
  const std::string_view filter =
    where.empty() ? std::string_view{"TRUE"} : std::string_view{where};
  std::string sql;
  switch (agg.kind) {
    case Aggregation::Kind::kTerms:
      sql = absl::StrCat("SELECT ", field,
                         " AS k, count(*) AS c, sum(count(*)) OVER () AS t "
                         "FROM ",
                         relation, " WHERE (", filter, ") AND ", field,
                         " IS NOT NULL GROUP BY 1 ORDER BY c DESC, k ASC "
                         "LIMIT ",
                         agg.size);
      break;
    case Aggregation::Kind::kDateHistogram:
      // agg.interval comes from the calendar_interval whitelist.
      sql = absl::StrCat(
        "SELECT epoch_ms(date_trunc('", agg.interval, "', ", field,
        ")) AS k, strftime(date_trunc('", agg.interval, "', ", field,
        "), '%Y-%m-%dT%H:%M:%S.000Z') AS ks, count(*) AS c FROM ", relation,
        " WHERE (", filter, ") AND ", field,
        " IS NOT NULL GROUP BY 1, 2 ORDER BY 1");
      break;
    default: {
      std::string_view fn;
      switch (agg.kind) {
        case Aggregation::Kind::kMin:
          fn = "min";
          break;
        case Aggregation::Kind::kMax:
          fn = "max";
          break;
        case Aggregation::Kind::kAvg:
          fn = "avg";
          break;
        case Aggregation::Kind::kSum:
          fn = "sum";
          break;
        case Aggregation::Kind::kValueCount:
          fn = "count";
          break;
        default:
          fn = "approx_count_distinct";
          break;
      }
      sql = absl::StrCat("SELECT ", fn, "(", field, ") FROM ", relation,
                         " WHERE ", filter);
      break;
    }
  }

  auto result = co_await RunSql(ctx, sql, writer, index);
  if (!result) {
    co_return false;
  }
  sb.escape_and_append_with_quotes(std::string_view{agg.name});
  sb.append_raw(":");
  switch (agg.kind) {
    case Aggregation::Kind::kTerms: {
      const auto rows = result->RowCount();
      int64_t shown = 0;
      const int64_t all =
        rows > 0 ? result->GetValue(2, 0).GetValue<int64_t>() : 0;
      std::string buckets;
      simdjson::builder::string_builder bucket_sb;
      for (duckdb::idx_t row = 0; row < rows; ++row) {
        if (row > 0) {
          bucket_sb.append_raw(",");
        }
        bucket_sb.append_raw(R"({"key":)");
        AppendSortValue(bucket_sb, result->GetValue(0, row));
        bucket_sb.append_raw(R"(,"doc_count":)");
        const auto count = result->GetValue(1, row).GetValue<int64_t>();
        shown += count;
        bucket_sb.append(count);
        bucket_sb.append_raw("}");
      }
      sb.append_raw(R"({"doc_count_error_upper_bound":0,)"
                    R"("sum_other_doc_count":)");
      sb.append(all - shown);
      sb.append_raw(R"(,"buckets":[)");
      sb.append_raw(std::string_view{bucket_sb.view().value()});
      sb.append_raw("]}");
      co_return true;
    }
    case Aggregation::Kind::kDateHistogram: {
      sb.append_raw(R"({"buckets":[)");
      for (duckdb::idx_t row = 0; row < result->RowCount(); ++row) {
        if (row > 0) {
          sb.append_raw(",");
        }
        sb.append_raw(R"({"key_as_string":)");
        sb.escape_and_append_with_quotes(std::string_view{
          duckdb::StringValue::Get(result->GetValue(1, row))});
        sb.append_raw(R"(,"key":)");
        sb.append(result->GetValue(0, row).GetValue<int64_t>());
        sb.append_raw(R"(,"doc_count":)");
        sb.append(result->GetValue(2, row).GetValue<int64_t>());
        sb.append_raw("}");
      }
      sb.append_raw("]}");
      co_return true;
    }
    default: {
      const auto value = result->GetValue(0, 0);
      sb.append_raw(R"({"value":)");
      if (value.IsNull()) {
        sb.append_raw("null");
      } else if (agg.kind == Aggregation::Kind::kValueCount ||
                 agg.kind == Aggregation::Kind::kCardinality) {
        sb.append(value.GetValue<int64_t>());
      } else if (agg.kind == Aggregation::Kind::kAvg ||
                 agg.kind == Aggregation::Kind::kSum) {
        sb.append(value.GetValue<double>());
      } else {
        AppendSortValue(sb, value);
      }
      sb.append_raw("}");
      co_return true;
    }
  }
}

// Type-aware query translation needs the index's field types; one catalog
// read per request (es_mapping doubles as the existence check -> 404).
// false = the error response is already written.
yaclib::Future<bool> FetchFieldTypes(RequestContext& ctx,
                                     std::string_view index,
                                     FieldTypes& fields,
                                     http::HttpResponseWriter& writer) {
  auto result = co_await RunSql(
    ctx, absl::StrCat("CALL es_mapping(", SqlLiteral(index), ")"), writer,
    index);
  if (!result) {
    co_return false;
  }
  const auto mapping = result->GetValue(0, 0).GetValue<std::string>();
  if (!ParseFieldTypes(mapping, fields)) {
    WriteError(writer, 500, "exception", "malformed index mapping");
    co_return false;
  }
  co_return true;
}

// ES sends scroll/sort as URL params on the initial scroll search; only the
// _doc order is supported (the cursor is the "_id" keyset).
bool WantsScroll(const HttpRequest& request) {
  for (const auto& [key, value] : request.query) {
    if (key == "scroll") {
      return true;
    }
  }
  return false;
}

// GET|POST /{index}/_search
class SearchHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto index = request.Param("index");
    if (IsIndexPattern(index)) {
      WriteJson(writer, 200,
                R"({"took":0,"timed_out":false,"_shards":{"total":0,)"
                R"("successful":0,"skipped":0,"failed":0},"hits":{)"
                R"("total":{"value":0,"relation":"eq"},"max_score":null,)"
                R"("hits":[]}})");
      co_return {};
    }
    const auto start = std::chrono::steady_clock::now();

    SearchRequest spec;
    for (const auto param : {"size", "from"}) {
      if (const auto value = request.Query(param); !value.empty() &&
          !absl::SimpleAtoi(value, param[0] == 's' ? &spec.size : &spec.from)) {
        WriteError(writer, 400, "illegal_argument_exception",
                   absl::StrCat("[", param, "] must be an integer"));
        co_return {};
      }
    }
    FieldTypes fields;
    if (!co_await FetchFieldTypes(ctx, index, fields, writer)) {
      co_return {};
    }
    // The body overrides URL params; no body = match_all.
    if (!ParseSearchBody(FlattenBody(request.body), fields, spec, writer)) {
      co_return {};
    }

    if (WantsScroll(request)) {
      co_await HandleScroll(ctx, request, writer, index, spec, start);
      co_return {};
    }

    const auto relation = SearchRelation(index, spec);
    // Relevance order: real BM25 over the index scan (pushes TopK down);
    // explicit field sorts render null scores like ES.
    const bool relevance = spec.uses_match && spec.order_by.empty();
    // Query-then-fetch, like ES: rank by score/sort WITHOUT projecting
    // _source (TOP_N sits above the scan, so projecting _source here would
    // materialize the stored doc for EVERY match before the limit prunes --
    // seconds for a common term). _source is fetched for the final page only.
    std::string sql = "SELECT \"_id\"";
    const duckdb::idx_t sort_base = 1;
    for (const auto& field : spec.sort_fields) {
      absl::StrAppend(&sql, ", ", SqlIdentifier(field));
    }
    if (relevance) {
      absl::StrAppend(&sql, ", BM25(t.tableoid) AS \"_score\"");
    }
    absl::StrAppend(&sql, " FROM ", relation, " AS t");
    if (!spec.where.empty()) {
      absl::StrAppend(&sql, " WHERE ", spec.where);
    }
    if (relevance) {
      absl::StrAppend(&sql, " ORDER BY \"_score\" DESC, \"_id\"");
    } else if (!spec.order_by.empty()) {
      absl::StrAppend(&sql, " ORDER BY ", spec.order_by);
    }
    absl::StrAppend(&sql, " LIMIT ", spec.size, " OFFSET ", spec.from);

    auto result = co_await RunSql(ctx, sql, writer, index);
    if (!result) {
      co_return {};
    }
    const auto rows = result->RowCount();

    // Fetch _source for just this page, by PK -- point lookups, not a scan.
    containers::FlatHashMap<std::string, std::string> source_by_id;
    if (spec.include_source && rows > 0) {
      std::string ids;
      for (duckdb::idx_t row = 0; row < rows; ++row) {
        absl::StrAppend(
          &ids, row > 0 ? "," : "",
          SqlLiteral(result->GetValue(0, row).GetValue<std::string>()));
      }
      auto source_result = co_await RunSql(
        ctx,
        absl::StrCat("SELECT \"_id\", \"_source\" FROM \"es\".",
                     SqlIdentifier(index), " WHERE \"_id\" IN (", ids, ")"),
        writer, index);
      if (!source_result) {
        co_return {};
      }
      source_by_id.reserve(source_result->RowCount());
      for (duckdb::idx_t row = 0; row < source_result->RowCount(); ++row) {
        source_by_id.emplace(
          source_result->GetValue(0, row).GetValue<std::string>(),
          source_result->GetValue(1, row).GetValue<std::string>());
      }
    }

    // The page bounds the total exactly unless it is full (or skipped past
    // the end); only then pay for the count.
    int64_t total = spec.from + static_cast<int64_t>(rows);
    if (static_cast<int64_t>(rows) == spec.size ||
        (rows == 0 && spec.from > 0)) {
      std::string count_sql = absl::StrCat("SELECT count(*) FROM ", relation);
      if (!spec.where.empty()) {
        absl::StrAppend(&count_sql, " WHERE ", spec.where);
      }
      auto count_result = co_await RunSql(ctx, count_sql, writer, index);
      if (!count_result) {
        co_return {};
      }
      total = count_result->GetValue(0, 0).GetValue<int64_t>();
    }

    // Filter-only queries score a constant 1.0; field sorts render null
    // scores like ES.
    const bool scored = spec.order_by.empty();
    const duckdb::idx_t score_column = sort_base + spec.sort_fields.size();
    auto score_of = [&](duckdb::idx_t row) {
      return result->GetValue(score_column, row).GetValue<double>();
    };
    simdjson::builder::string_builder sb;
    sb.append_raw("{\"took\":");
    sb.append(TookMs(start));
    sb.append_raw(R"(,"timed_out":false,"_shards":{"total":1,)"
                  R"("successful":1,"skipped":0,"failed":0},"hits":{)");
    if (request.Query("rest_total_hits_as_int") == "true") {
      sb.append_raw("\"total\":");
      sb.append(total);
    } else {
      sb.append_raw(R"("total":{"value":)");
      sb.append(total);
      sb.append_raw(R"(,"relation":"eq"})");
    }
    sb.append_raw(",\"max_score\":");
    if (rows == 0 || !scored) {
      sb.append_raw("null");
    } else if (relevance) {
      sb.append(score_of(0));
    } else {
      sb.append_raw("1.0");
    }
    sb.append_raw(",\"hits\":[");
    for (duckdb::idx_t row = 0; row < rows; ++row) {
      if (row > 0) {
        sb.append_raw(",");
      }
      sb.append_raw(R"({"_index":)");
      sb.escape_and_append_with_quotes(index);
      sb.append_raw(R"(,"_id":)");
      sb.escape_and_append_with_quotes(
        std::string_view{duckdb::StringValue::Get(result->GetValue(0, row))});
      sb.append_raw(",\"_score\":");
      if (!scored) {
        sb.append_raw("null");
      } else if (relevance) {
        sb.append(score_of(row));
      } else {
        sb.append_raw("1.0");
      }
      if (spec.include_source) {
        sb.append_raw(",\"_source\":");
        const auto id = result->GetValue(0, row).GetValue<std::string>();
        const auto it = source_by_id.find(id);
        // A doc deleted between rank and fetch leaves an empty object.
        sb.append_raw(it != source_by_id.end()
                        ? std::string_view{it->second}
                        : std::string_view{"{}"});
      }
      if (!spec.sort_fields.empty()) {
        sb.append_raw(",\"sort\":[");
        for (size_t i = 0; i < spec.sort_fields.size(); ++i) {
          if (i > 0) {
            sb.append_raw(",");
          }
          AppendSortValue(sb, result->GetValue(sort_base + i, row));
        }
        sb.append_raw("]");
      }
      sb.append_raw("}");
    }
    sb.append_raw("]}");
    if (!spec.aggs.empty()) {
      sb.append_raw(",\"aggregations\":{");
      for (size_t i = 0; i < spec.aggs.size(); ++i) {
        if (i > 0) {
          sb.append_raw(",");
        }
        if (!co_await RunAggregation(ctx, spec.aggs[i], relation, spec.where,
                                     index, sb, writer)) {
          co_return {};
        }
      }
      sb.append_raw("}");
    }
    sb.append_raw("}");
    WriteJson(writer, 200, std::string_view{sb.view().value()});
    co_return {};
  }

 private:
  // Initial scroll page: exact total once (it rides in the scroll id), then
  // the first keyset page.
  yaclib::Future<> HandleScroll(RequestContext& ctx,
                                const HttpRequest& request,
                                http::HttpResponseWriter& writer,
                                std::string_view index, SearchRequest& spec,
                                std::chrono::steady_clock::time_point start) {
    for (const auto& [key, value] : request.query) {
      if (key == "sort" && value != "_doc" && value != "_doc:asc") {
        WriteError(writer, 400, "illegal_argument_exception",
                   "scroll supports only the _doc sort order yet");
        co_return {};
      }
    }
    if (!spec.sort_fields.empty() || !spec.aggs.empty() || spec.from != 0) {
      WriteError(writer, 400, "illegal_argument_exception",
                 "scroll supports only plain _doc-ordered requests yet");
      co_return {};
    }

    const auto relation = SearchRelation(index, spec);
    std::string count_sql = absl::StrCat("SELECT count(*) FROM ", relation);
    if (!spec.where.empty()) {
      absl::StrAppend(&count_sql, " WHERE ", spec.where);
    }
    auto count_result = co_await RunSql(ctx, count_sql, writer, index);
    if (!count_result) {
      co_return {};
    }

    ScrollState state{
      .index = std::string{index},
      .query = std::move(spec.query_raw),
      .size = spec.size,
      .total = count_result->GetValue(0, 0).GetValue<int64_t>(),
      .include_source = spec.include_source,
    };
    auto result = co_await RunSql(ctx, ScrollPageSql(relation, spec, state), writer,
                         index);
    if (!result) {
      co_return {};
    }
    WriteScrollPage(writer, state, result.get(), TookMs(start));
    co_return {};
  }
};

// GET|POST /_search/scroll: decode the cursor, re-translate the embedded
// query, fetch the next keyset page.
class ScrollHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto start = std::chrono::steady_clock::now();
    std::string scroll_id{request.Query("scroll_id")};
    if (scroll_id.empty() &&
        !ParseScrollIdBody(FlattenBody(request.body), scroll_id)) {
      WriteError(writer, 400, "illegal_argument_exception",
                 "scroll_id is required");
      co_return {};
    }
    ScrollState state;
    if (!DecodeScrollId(scroll_id, state)) {
      WriteError(writer, 404, "search_context_missing_exception",
                 "No search context found for the given scroll id");
      co_return {};
    }
    if (state.done) {
      WriteScrollPage(writer, state, nullptr, TookMs(start));
      co_return {};
    }
    FieldTypes fields;
    if (!co_await FetchFieldTypes(ctx, state.index, fields, writer)) {
      co_return {};
    }
    SearchRequest spec;
    if (!TranslateStoredQuery(state.query, fields, spec, writer)) {
      co_return {};
    }
    spec.include_source = state.include_source;
    auto result = co_await RunSql(
      ctx, ScrollPageSql(SearchRelation(state.index, spec), spec, state),
      writer, state.index);
    if (!result) {
      co_return {};
    }
    WriteScrollPage(writer, state, result.get(), TookMs(start));
    co_return {};
  }

 private:
  static bool ParseScrollIdBody(const std::string& body,
                                std::string& scroll_id) {
    if (body.empty()) {
      return false;
    }
    try {
      simdjson::padded_string padded{body};
      simdjson::ondemand::parser parser;
      simdjson::ondemand::document doc;
      if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
        return false;
      }
      simdjson::ondemand::object object;
      if (doc.get_object().get(object) != simdjson::SUCCESS) {
        return false;
      }
      for (auto field : object) {
        std::string_view key;
        if (field.unescaped_key().get(key) != simdjson::SUCCESS) {
          return false;
        }
        if (key == "scroll_id") {
          std::string_view value;
          if (field.value().get_string().get(value) != simdjson::SUCCESS) {
            return false;
          }
          scroll_id = value;
        }
      }
    } catch (const std::exception&) {
      return false;
    }
    return !scroll_id.empty();
  }
};

// DELETE /_search/scroll: nothing server-side to free.
class ClearScrollHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext&, const HttpRequest&,
                          http::HttpResponseWriter& writer) override {
    WriteJson(writer, 200, R"({"succeeded":true,"num_freed":1})");
    co_return {};
  }
};

// GET|POST /{index}/_count; shares the _search query translation.
class CountHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto index = request.Param("index");
    if (IsIndexPattern(index)) {
      WriteJson(writer, 200,
                R"({"count":0,"_shards":{"total":0,"successful":0,)"
                R"("skipped":0,"failed":0}})");
      co_return {};
    }
    FieldTypes fields;
    if (!co_await FetchFieldTypes(ctx, index, fields, writer)) {
      co_return {};
    }
    SearchRequest spec;
    if (!ParseCountBody(FlattenBody(request.body), fields, spec, writer)) {
      co_return {};
    }
    auto sql =
      absl::StrCat("SELECT count(*) FROM ", SearchRelation(index, spec));
    if (!spec.where.empty()) {
      absl::StrAppend(&sql, " WHERE ", spec.where);
    }
    auto result = co_await RunSql(ctx, sql, writer, index);
    if (!result) {
      co_return {};
    }
    const auto count = result->GetValue(0, 0).GetValue<int64_t>();
    WriteJson(writer, 200, absl::StrCat("{\"count\":", count,
                                  ",\"_shards\":{\"total\":1,\"successful\":1,"
                                  "\"skipped\":0,\"failed\":0}}"));
    co_return {};
  }
};

class RootHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext&, const HttpRequest&,
                          http::HttpResponseWriter& writer) override {
    WriteJson(writer, 
      200,
      R"({"name":"serenedb","cluster_name":"serenedb","version":{)"
      R"("number":"8.11.0","build_flavor":"default"},)"
      R"("tagline":"You Know, for Search"})");
    co_return {};
  }
};

class HealthHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext&, const HttpRequest&,
                          http::HttpResponseWriter& writer) override {
    WriteJson(writer, 
      200,
      R"({"cluster_name":"serenedb","status":"green","timed_out":false,)"
      R"("number_of_nodes":1,"number_of_data_nodes":1,)"
      R"("active_primary_shards":0,"active_shards":0,"relocating_shards":0,)"
      R"("initializing_shards":0,"unassigned_shards":0,)"
      R"("delayed_unassigned_shards":0,"number_of_pending_tasks":0,)"
      R"("number_of_in_flight_fetch":0,"task_max_waiting_in_queue_millis":0,)"
      R"("active_shards_percent_as_number":100.0})");
    co_return {};
  }
};

// PUT /{index}
class CreateIndexHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto index = request.Param("index");
    const auto sql =
      absl::StrCat("CALL es_create_index(", SqlLiteral(index), ", ",
                   SqlLiteral(FlattenBody(request.body)), ")");
    if (co_await RunSql(ctx, sql, writer)) {
      WriteJson(writer, 
        200,
        absl::StrCat(R"({"acknowledged":true,"shards_acknowledged":true,)"
                     R"("index":")",
                     index, R"("})"));
    }
    co_return {};
  }
};

// DELETE /{index}
class DeleteIndexHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto sql =
      absl::StrCat("CALL es_drop_index(", SqlLiteral(request.Param("index")),
                   ")");
    if (co_await RunSql(ctx, sql, writer)) {
      WriteJson(writer, 200, R"({"acknowledged":true})");
    }
    co_return {};
  }
};

// HEAD /{index}
class IndexExistsHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto sql = absl::StrCat(
      "CALL es_mapping(", SqlLiteral(request.Param("index")), ")");
    if (co_await RunSql(ctx, sql, writer)) {
      WriteJson(writer, 200, "{}");
    }
    co_return {};
  }
};

// GET /{index}/_mapping
class MappingHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto index = request.Param("index");
    const auto sql =
      absl::StrCat("CALL es_mapping(", SqlLiteral(index), ")");
    auto result = co_await RunSql(ctx, sql, writer);
    if (!result) {
      co_return {};
    }
    const auto mappings = result->GetValue(0, 0).GetValue<std::string>();
    WriteJson(writer, 200, absl::StrCat(R"({")", index, R"(":{"mappings":)",
                                  mappings, "}}"));
    co_return {};
  }
};

// GET /_cat/indices -- the _cat text table by default, ?format=json for the
// structured variant (all values strings, matching ES).
class CatIndicesHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    auto result = co_await RunSql(
      ctx, "SELECT index, docs_count FROM es_cat_indices()", writer);
    if (!result) {
      co_return {};
    }
    const bool json = request.Query("format") == "json";
    std::string body;
    if (json) {
      body.push_back('[');
    }
    for (duckdb::idx_t row = 0; row < result->RowCount(); ++row) {
      const auto index = result->GetValue(0, row).GetValue<std::string>();
      const auto docs = result->GetValue(1, row).GetValue<int64_t>();
      if (json) {
        absl::StrAppend(
          &body, row > 0 ? "," : "",
          R"({"health":"green","status":"open","index":")", index,
          R"(","uuid":"_na_","pri":"1","rep":"0","docs.count":")", docs,
          R"(","docs.deleted":"0","store.size":"0b","pri.store.size":"0b"})");
      } else {
        absl::StrAppend(&body, "green open ", index, " _na_ 1 0 ", docs,
                        " 0 0b 0b\n");
      }
    }
    if (json) {
      body.push_back(']');
      WriteJson(writer, 200, body);
    } else {
      WriteText(writer, 200, body);
    }
    co_return {};
  }
};

// GET /_nodes/stats[/{metric}] — enough node introspection for rally's
// telemetry devices (empty collectors/pools iterate to nothing).
class NodesStatsHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext&, const HttpRequest&,
                          http::HttpResponseWriter& writer) override {
    WriteJson(
      writer, 200,
      R"({"_nodes":{"total":1,"successful":1,"failed":0},)"
      R"("cluster_name":"serenedb","nodes":{"sdb0":{"name":"serenedb-0",)"
      R"("jvm":{"gc":{"collectors":{}},"mem":{"pools":{}}},)"
      R"("ingest":{"total":{"count":0,"time_in_millis":0,"current":0,)"
      R"("failed":0},"pipelines":{}}}}})");
    co_return {};
  }
};

// POST [/{index}]/_forcemerge — segment merging is the engine's own
// consolidation; acknowledge.
class ForceMergeHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext&, const HttpRequest&,
                          http::HttpResponseWriter& writer) override {
    WriteJson(writer, 200,
              R"({"_shards":{"total":1,"successful":1,"failed":0}})");
    co_return {};
  }
};

// GET|PUT /_cluster/settings — nothing configurable yet; PUT acknowledges,
// GET reports empty (rally's delete-index runner reads
// action.destructive_requires_name before deleting).
class ClusterSettingsHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext&, const HttpRequest&,
                          http::HttpResponseWriter& writer) override {
    WriteJson(writer, 200,
              R"({"acknowledged":true,"persistent":{},"transient":{}})");
    co_return {};
  }
};

// GET [/{index}]/_stats — index stats with no pending merges (rally's
// wait-until-merges-finish polls _all.total.merges.current).
class IndexStatsHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext&, const HttpRequest&,
                          http::HttpResponseWriter& writer) override {
    WriteJson(
      writer, 200,
      R"({"_shards":{"total":1,"successful":1,"failed":0},)"
      R"("_all":{"total":{"merges":{"current":0}},)"
      R"("primaries":{"merges":{"current":0}}},"indices":{}})");
    co_return {};
  }
};

}  // namespace

void Register(HttpRouter& router) {
  router.Add(HttpMethod::Get, "/", std::make_unique<RootHandler>());
  router.Add(HttpMethod::Head, "/", std::make_unique<RootHandler>());
  router.Add(HttpMethod::Get, "/_cluster/health",
             std::make_unique<HealthHandler>());
  router.Add(HttpMethod::Get, "/_cat/indices",
             std::make_unique<CatIndicesHandler>());
  router.Add(HttpMethod::Post, "/_bulk", std::make_unique<BulkHandler>());
  router.Add(HttpMethod::Put, "/_bulk", std::make_unique<BulkHandler>());
  router.Add(HttpMethod::Get, "/_nodes/stats",
             std::make_unique<NodesStatsHandler>());
  router.Add(HttpMethod::Get, "/_nodes/stats/:metric",
             std::make_unique<NodesStatsHandler>());
  router.Add(HttpMethod::Put, "/_cluster/settings",
             std::make_unique<ClusterSettingsHandler>());
  router.Add(HttpMethod::Get, "/_cluster/settings",
             std::make_unique<ClusterSettingsHandler>());
  router.Add(HttpMethod::Get, "/_cluster/health/:index",
             std::make_unique<HealthHandler>());
  router.Add(HttpMethod::Get, "/_stats",
             std::make_unique<IndexStatsHandler>());
  router.Add(HttpMethod::Get, "/:index/_stats",
             std::make_unique<IndexStatsHandler>());
  router.Add(HttpMethod::Get, "/:index/_stats/:metric",
             std::make_unique<IndexStatsHandler>());
  router.Add(HttpMethod::Post, "/_forcemerge",
             std::make_unique<ForceMergeHandler>());
  router.Add(HttpMethod::Post, "/:index/_forcemerge",
             std::make_unique<ForceMergeHandler>());
  router.Add(HttpMethod::Post, "/_refresh",
             std::make_unique<RefreshHandler>());
  router.Add(HttpMethod::Get, "/_refresh", std::make_unique<RefreshHandler>());
  router.Add(HttpMethod::Put, "/:index",
             std::make_unique<CreateIndexHandler>());
  router.Add(HttpMethod::Delete, "/:index",
             std::make_unique<DeleteIndexHandler>());
  router.Add(HttpMethod::Head, "/:index",
             std::make_unique<IndexExistsHandler>());
  router.Add(HttpMethod::Get, "/:index/_mapping",
             std::make_unique<MappingHandler>());
  router.Add(HttpMethod::Post, "/:index/_bulk",
             std::make_unique<BulkHandler>());
  router.Add(HttpMethod::Put, "/:index/_bulk",
             std::make_unique<BulkHandler>());
  router.Add(HttpMethod::Post, "/:index/_doc", std::make_unique<DocHandler>());
  router.Add(HttpMethod::Post, "/:index/_doc/:id",
             std::make_unique<DocHandler>());
  router.Add(HttpMethod::Put, "/:index/_doc/:id",
             std::make_unique<DocHandler>());
  router.Add(HttpMethod::Get, "/:index/_doc/:id",
             std::make_unique<GetDocHandler>());
  router.Add(HttpMethod::Get, "/:index/_source/:id",
             std::make_unique<GetSourceHandler>());
  router.Add(HttpMethod::Post, "/:index/_refresh",
             std::make_unique<RefreshHandler>());
  router.Add(HttpMethod::Get, "/:index/_refresh",
             std::make_unique<RefreshHandler>());
  router.Add(HttpMethod::Get, "/:index/_count",
             std::make_unique<CountHandler>());
  router.Add(HttpMethod::Post, "/:index/_count",
             std::make_unique<CountHandler>());
  router.Add(HttpMethod::Get, "/:index/_search",
             std::make_unique<SearchHandler>());
  router.Add(HttpMethod::Post, "/:index/_search",
             std::make_unique<SearchHandler>());
  router.Add(HttpMethod::Get, "/_search/scroll",
             std::make_unique<ScrollHandler>());
  router.Add(HttpMethod::Post, "/_search/scroll",
             std::make_unique<ScrollHandler>());
  router.Add(HttpMethod::Delete, "/_search/scroll",
             std::make_unique<ClearScrollHandler>());
}

}  // namespace sdb::network::http::es
