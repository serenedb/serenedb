////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "query/show_batch_executor.h"

#include <absl/algorithm/container.h>

#include <yaclib/async/make.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "query/config.h"
#include "query/query.h"

namespace sdb::query {
namespace {

velox::RowVectorPtr BuildBatch(
  const velox::RowTypePtr& output_type, velox::memory::MemoryPool* pool,
  std::span<const std::vector<std::string>> columns) {
  SDB_ASSERT(output_type->isRow());
  SDB_ASSERT(absl::c_all_of(output_type->children(), [](const auto& ptr) {
    return ptr == velox::VARCHAR();
  }));
  std::vector<velox::VectorPtr> vectors;
  vectors.reserve(columns.size());
  size_t batch_rows = 0;
  for (size_t i = 0; i < columns.size(); ++i) {
    auto vector =
      velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        output_type->children()[i], columns[i].size(), pool);
    for (size_t j = 0; j < columns[i].size(); ++j) {
      vector->set(j, velox::StringView(columns[i][j]));
    }
    batch_rows = std::max(batch_rows, columns[i].size());
    vectors.push_back(std::move(vector));
  }
  return std::make_shared<velox::RowVector>(pool, output_type, nullptr,
                                            batch_rows, std::move(vectors));
}

}  // namespace

void ShowBatchExecutor::SetQuery(Query& query) {
  SDB_ASSERT(
    query.GetOutputType()->equivalent(*velox::ROW({velox::VARCHAR()})));
  const auto& config = query.GetContext().velox_query_ctx->queryConfig();
  const std::string& name = query.GetOutputType()->nameOf(0);
  auto* pool = query.GetContext().query_memory_pool.get();

#ifdef SDB_FAULT_INJECTION
  if (name.starts_with(kFailPointPrefix)) {
    std::string_view point = name;
    point.remove_prefix(kFailPointPrefix.size());
    if (point == "s") {
      auto column = GetFailurePointsDebugging();
      _result = BuildBatch(query.GetOutputType(), pool, {std::move(column)});
      return;
    }
    if (!point.starts_with('_')) {
      SDB_THROW(ERROR_FAILED,
                "failure point configuration parameter must start with '",
                kFailPointPrefix, "_'");
    }
    point.remove_prefix(1);
    std::vector<std::string> column{ShouldFailDebugging(point) ? "on" : "off"};
    _result = BuildBatch(query.GetOutputType(), pool, {std::move(column)});
    return;
  }
#endif

  auto value = config.get<std::string>(name);
  if (!value) {
    SDB_THROW(ERROR_FAILED, "unrecognized configuration parameter \"", name,
              "\"");
  }
  std::vector<std::string> column{*value};
  _result = BuildBatch(query.GetOutputType(), pool, {std::move(column)});
}

yaclib::Future<velox::RowVectorPtr> ShowBatchExecutor::Execute() {
  if (!_result) {
    return {};
  }
  return yaclib::MakeFuture<velox::RowVectorPtr>(std::move(_result));
}

void ShowAllBatchExecutor::SetQuery(Query& query) {
  SDB_ASSERT(query.GetOutputType()->equivalent(
    *velox::ROW({velox::VARCHAR(), velox::VARCHAR(), velox::VARCHAR()})));
  const auto& query_config = query.GetContext().velox_query_ctx->queryConfig();
  const auto& config = basics::downCast<Config>(*query_config.config());
  auto* pool = query.GetContext().query_memory_pool.get();

  std::vector<std::string> names, values, descriptions;
  config.VisitFullDescription([&](std::string_view name, std::string_view value,
                                  std::string_view description) {
    names.emplace_back(name);
    values.push_back(std::string{value});
    descriptions.emplace_back(description);
  });

  _result = BuildBatch(query.GetOutputType(), pool,
                       {
                         std::move(names),
                         std::move(values),
                         std::move(descriptions),
                       });
}

yaclib::Future<velox::RowVectorPtr> ShowAllBatchExecutor::Execute() {
  if (!_result) {
    return {};
  }
  return yaclib::MakeFuture<velox::RowVectorPtr>(std::move(_result));
}

}  // namespace sdb::query
