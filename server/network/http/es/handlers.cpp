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

// pg-wire arms the storage transaction/snapshot per statement from prepared
// statement properties (PendingQueryEnsured); the thin handlers know
// statically which calls write. The commit/rollback at statement end comes
// from the SereneDBClientState transaction hooks.
duckdb::Connection& PrepareStatement(RequestContext& ctx, bool writes) {
  auto& conn = ctx.Connection();
  auto& sdb_ctx = connector::GetSereneDBContext(*conn.context);
  sdb_ctx.EnsureCatalogSnapshot();
  if (writes) {
    sdb_ctx.EnsureRocksDBTransaction();
  }
  sdb_ctx.EnsureRocksDBSnapshot();
  return conn;
}

// One es_*() call per request; a failed call has already been written out as
// an ES error envelope when this returns null.
duckdb::unique_ptr<duckdb::MaterializedQueryResult> RunSql(
  RequestContext& ctx, const std::string& sql,
  http::HttpResponseWriter& writer, std::string_view index = {},
  bool writes = false) {
  auto result = PrepareStatement(ctx, writes).Query(sql);
  if (result->HasError()) {
    WriteSqlError(writer, result->GetErrorObject(), index);
    return nullptr;
  }
  return result;
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

bool MaybeRefresh(RequestContext& ctx, const HttpRequest& request,
                  std::string_view index, http::HttpResponseWriter& writer) {
  if (!WantsRefresh(request)) {
    return true;
  }
  return RunSql(ctx, absl::StrCat("CALL es_refresh(", SqlLiteral(index), ")"),
                writer, index) != nullptr;
}

int64_t TookMs(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::steady_clock::now() - start)
    .count();
}

// POST|PUT /{index}/_bulk; bare /_bulk has no default index and is rejected
// (per-line _index routing is not supported).
class BulkHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto index = request.Param("index");
    if (index.empty()) {
      WriteError(writer, 400, "illegal_argument_exception",
                 "an index is required in the request URL");
      return yaclib::MakeFuture();
    }
    const auto start = std::chrono::steady_clock::now();

    // es_bulk fills the items array through the side channel while the
    // INSERT runs (serenedb INSERT has no RETURNING).
    auto& conn = PrepareStatement(ctx, /*writes=*/true);
    auto& sdb_ctx = connector::GetSereneDBContext(*conn.context);
    std::string items;
    sdb_ctx.SetResponseSink(&items);
    const absl::Cleanup clear_sink = [&] { sdb_ctx.SetResponseSink(nullptr); };

    const auto sql = absl::StrCat(
      "INSERT INTO \"es\".", SqlIdentifier(index), " SELECT * FROM es_bulk(",
      SqlLiteral(index), ", ", SqlLiteral(FlattenBody(request.body)), ")");
    auto result = conn.Query(sql);
    if (result->HasError()) {
      WriteSqlError(writer, result->GetErrorObject(), index);
      return yaclib::MakeFuture();
    }
    if (!MaybeRefresh(ctx, request, index, writer)) {
      return yaclib::MakeFuture();
    }
    WriteJson(writer, 200, absl::StrCat("{\"took\":", TookMs(start),
                                  ",\"errors\":false,\"items\":[", items,
                                  "]}"));
    return yaclib::MakeFuture();
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
    if (!RunSql(ctx, sql, writer, index, /*writes=*/true) ||
        !MaybeRefresh(ctx, request, index, writer)) {
      return yaclib::MakeFuture();
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
    return yaclib::MakeFuture();
  }
};

// POST|GET /{index}/_refresh and /_refresh ('' = every ES index)
class RefreshHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto sql = absl::StrCat("CALL es_refresh(",
                                  SqlLiteral(request.Param("index")), ")");
    if (RunSql(ctx, sql, writer)) {
      WriteJson(writer, 200, R"({"_shards":{"total":1,"successful":1,"failed":0}})");
    }
    return yaclib::MakeFuture();
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
    auto result = RunSql(
      ctx,
      absl::StrCat("SELECT \"_source\" FROM \"es\".", SqlIdentifier(index),
                   " WHERE \"_id\" = ", SqlLiteral(id)),
      writer, index);
    if (!result) {
      return yaclib::MakeFuture();
    }
    simdjson::builder::string_builder sb;
    sb.append_raw(R"({"_index":)");
    sb.escape_and_append_with_quotes(index);
    sb.append_raw(R"(,"_id":)");
    sb.escape_and_append_with_quotes(id);
    if (result->RowCount() == 0) {
      sb.append_raw(R"(,"found":false})");
      WriteJson(writer, 404, std::string_view{sb.view().value()});
      return yaclib::MakeFuture();
    }
    const auto source = result->GetValue(0, 0).GetValue<std::string>();
    sb.append_raw(R"(,"_version":1,"_seq_no":0,"_primary_term":1,)"
                  R"("found":true,"_source":)");
    sb.append_raw(source);
    sb.append_raw("}");
    WriteJson(writer, 200, std::string_view{sb.view().value()});
    return yaclib::MakeFuture();
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
    auto result = RunSql(
      ctx,
      absl::StrCat("SELECT \"_source\" FROM \"es\".", SqlIdentifier(index),
                   " WHERE \"_id\" = ", SqlLiteral(id)),
      writer, index);
    if (!result) {
      return yaclib::MakeFuture();
    }
    if (result->RowCount() == 0) {
      WriteError(writer, 404, "resource_not_found_exception",
                 absl::StrCat("Document not found [", index, "]/[", id, "]"));
      return yaclib::MakeFuture();
    }
    WriteJson(writer, 200, result->GetValue(0, 0).GetValue<std::string>());
    return yaclib::MakeFuture();
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

// GET|POST /{index}/_search
class SearchHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto index = request.Param("index");
    const auto start = std::chrono::steady_clock::now();

    SearchRequest spec;
    for (const auto param : {"size", "from"}) {
      if (const auto value = request.Query(param); !value.empty() &&
          !absl::SimpleAtoi(value, param[0] == 's' ? &spec.size : &spec.from)) {
        WriteError(writer, 400, "illegal_argument_exception",
                   absl::StrCat("[", param, "] must be an integer"));
        return yaclib::MakeFuture();
      }
    }
    // The body overrides URL params; no body = match_all.
    if (!ParseSearchBody(FlattenBody(request.body), spec, writer)) {
      return yaclib::MakeFuture();
    }

    const auto relation = SearchRelation(index, spec);
    std::string sql = "SELECT \"_id\"";
    if (spec.include_source) {
      absl::StrAppend(&sql, ", \"_source\"");
    }
    const duckdb::idx_t sort_base = spec.include_source ? 2 : 1;
    for (const auto& field : spec.sort_fields) {
      absl::StrAppend(&sql, ", ", SqlIdentifier(field));
    }
    absl::StrAppend(&sql, " FROM ", relation);
    if (!spec.where.empty()) {
      absl::StrAppend(&sql, " WHERE ", spec.where);
    }
    if (!spec.order_by.empty()) {
      absl::StrAppend(&sql, " ORDER BY ", spec.order_by);
    }
    absl::StrAppend(&sql, " LIMIT ", spec.size, " OFFSET ", spec.from);

    auto result = RunSql(ctx, sql, writer, index);
    if (!result) {
      return yaclib::MakeFuture();
    }
    const auto rows = result->RowCount();

    // The page bounds the total exactly unless it is full (or skipped past
    // the end); only then pay for the count.
    int64_t total = spec.from + static_cast<int64_t>(rows);
    if (static_cast<int64_t>(rows) == spec.size ||
        (rows == 0 && spec.from > 0)) {
      std::string count_sql = absl::StrCat("SELECT count(*) FROM ", relation);
      if (!spec.where.empty()) {
        absl::StrAppend(&count_sql, " WHERE ", spec.where);
      }
      auto count_result = RunSql(ctx, count_sql, writer, index);
      if (!count_result) {
        return yaclib::MakeFuture();
      }
      total = count_result->GetValue(0, 0).GetValue<int64_t>();
    }

    // Scores are constant 1.0 until a scorer is wired; field sorts render
    // null scores like ES.
    const bool scored = spec.order_by.empty();
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
    sb.append_raw(scored && rows > 0 ? "1.0" : "null");
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
      sb.append_raw(scored ? "1.0" : "null");
      if (spec.include_source) {
        sb.append_raw(",\"_source\":");
        sb.append_raw(
          std::string_view{duckdb::StringValue::Get(result->GetValue(1, row))});
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
    sb.append_raw("]}}");
    WriteJson(writer, 200, std::string_view{sb.view().value()});
    return yaclib::MakeFuture();
  }
};

// GET|POST /{index}/_count; shares the _search query translation.
class CountHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto index = request.Param("index");
    SearchRequest spec;
    if (!ParseCountBody(FlattenBody(request.body), spec, writer)) {
      return yaclib::MakeFuture();
    }
    auto sql =
      absl::StrCat("SELECT count(*) FROM ", SearchRelation(index, spec));
    if (!spec.where.empty()) {
      absl::StrAppend(&sql, " WHERE ", spec.where);
    }
    auto result = RunSql(ctx, sql, writer, index);
    if (!result) {
      return yaclib::MakeFuture();
    }
    const auto count = result->GetValue(0, 0).GetValue<int64_t>();
    WriteJson(writer, 200, absl::StrCat("{\"count\":", count,
                                  ",\"_shards\":{\"total\":1,\"successful\":1,"
                                  "\"skipped\":0,\"failed\":0}}"));
    return yaclib::MakeFuture();
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
    return yaclib::MakeFuture();
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
    return yaclib::MakeFuture();
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
    if (RunSql(ctx, sql, writer)) {
      WriteJson(writer, 
        200,
        absl::StrCat(R"({"acknowledged":true,"shards_acknowledged":true,)"
                     R"("index":")",
                     index, R"("})"));
    }
    return yaclib::MakeFuture();
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
    if (RunSql(ctx, sql, writer)) {
      WriteJson(writer, 200, R"({"acknowledged":true})");
    }
    return yaclib::MakeFuture();
  }
};

// HEAD /{index}
class IndexExistsHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    const auto sql = absl::StrCat(
      "CALL es_mapping(", SqlLiteral(request.Param("index")), ")");
    if (RunSql(ctx, sql, writer)) {
      WriteJson(writer, 200, "{}");
    }
    return yaclib::MakeFuture();
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
    auto result = RunSql(ctx, sql, writer);
    if (!result) {
      return yaclib::MakeFuture();
    }
    const auto mappings = result->GetValue(0, 0).GetValue<std::string>();
    WriteJson(writer, 200, absl::StrCat(R"({")", index, R"(":{"mappings":)",
                                  mappings, "}}"));
    return yaclib::MakeFuture();
  }
};

// GET /_cat/indices -- the _cat text table by default, ?format=json for the
// structured variant (all values strings, matching ES).
class CatIndicesHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext& ctx, const HttpRequest& request,
                          http::HttpResponseWriter& writer) override {
    auto result =
      RunSql(ctx, "SELECT index, docs_count FROM es_cat_indices()", writer);
    if (!result) {
      return yaclib::MakeFuture();
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
    return yaclib::MakeFuture();
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
}

}  // namespace sdb::network::http::es
