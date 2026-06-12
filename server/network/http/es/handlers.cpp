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

#include <absl/strings/str_cat.h>

#include <duckdb/main/connection.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <memory>
#include <yaclib/async/make.hpp>

#include "network/http/es/common.h"
#include "network/http/handler.h"

namespace sdb::network::http::es {
namespace {

// One es_*() call per request; a failed call has already been written out as
// an ES error envelope when this returns null.
duckdb::unique_ptr<duckdb::MaterializedQueryResult> RunSql(
  RequestContext& ctx, const std::string& sql,
  http::HttpResponseWriter& writer) {
  auto result = ctx.Connection().Query(sql);
  if (result->HasError()) {
    WriteSqlError(writer, result->GetErrorObject());
    return nullptr;
  }
  return result;
}

class RootHandler final : public HttpHandler {
 public:
  yaclib::Future<> Handle(RequestContext&, const HttpRequest&,
                          http::HttpResponseWriter& writer) override {
    writer.Json(
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
    writer.Json(
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
      writer.Json(
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
      writer.Json(200, R"({"acknowledged":true})");
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
      writer.Json(200, "{}");
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
    writer.Json(200, absl::StrCat(R"({")", index, R"(":{"mappings":)",
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
      writer.Json(200, body);
    } else {
      writer.Text(200, body);
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
  router.Add(HttpMethod::Put, "/:index",
             std::make_unique<CreateIndexHandler>());
  router.Add(HttpMethod::Delete, "/:index",
             std::make_unique<DeleteIndexHandler>());
  router.Add(HttpMethod::Head, "/:index",
             std::make_unique<IndexExistsHandler>());
  router.Add(HttpMethod::Get, "/:index/_mapping",
             std::make_unique<MappingHandler>());
}

}  // namespace sdb::network::http::es
