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

#include "query/show_executor.h"

#include <yaclib/async/make.hpp>

#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "query/config.h"
#include "query/query.h"

namespace sdb::query {

yaclib::Future<> ShowExecutor::Execute(velox::RowVectorPtr& batch) {
  if (!_query) {  // was fired
    return {};
  }

  SDB_ASSERT(
    _query->GetOutputType()->equivalent(*velox::ROW({velox::VARCHAR()})));
  const auto& config = _query->GetContext().velox_query_ctx->queryConfig();
  const std::string& name = _query->GetOutputType()->nameOf(0);

#ifdef SDB_FAULT_INJECTION
  if (name.starts_with(kFailPointPrefix)) {
    std::string_view point = name;
    point.remove_prefix(kFailPointPrefix.size());
    if (point == "s") {
      auto column = GetFailurePointsDebugging();
      batch = _query->BuildBatch({std::move(column)});
      _query = nullptr;  // set fired
      return {};
    }
    if (!point.starts_with('_')) {
      SDB_THROW(ERROR_FAILED,
                "failure point configuration parameter must start with '",
                kFailPointPrefix, "_'");
    }
    point.remove_prefix(1);
    std::vector<std::string> column{ShouldFailDebugging(point) ? "on" : "off"};
    batch = _query->BuildBatch({std::move(column)});
    _query = nullptr;  // set fired
    return {};
  }
#endif

  auto value = config.get<std::string>(name);
  if (!value) {
    SDB_THROW(ERROR_FAILED, "unrecognized configuration parameter \"", name,
              "\"");
  }
  std::vector<std::string> column{*value};
  batch = _query->BuildBatch({std::move(column)});
  _query = nullptr;  // set fired
  return {};
}

yaclib::Future<> ShowAllExecutor::Execute(velox::RowVectorPtr& batch) {
  if (!_query) {  // was fired
    return {};
  }

  SDB_ASSERT(_query->GetOutputType()->equivalent(
    *velox::ROW({velox::VARCHAR(), velox::VARCHAR(), velox::VARCHAR()})));
  const auto& query_config =
    _query->GetContext().velox_query_ctx->queryConfig();
  const auto& config = basics::downCast<Config>(*query_config.config());

  std::vector<std::string> names, values, descriptions;
  config.VisitFullDescription([&](std::string_view name, std::string_view value,
                                  std::string_view description) {
    names.emplace_back(name);
    values.push_back(std::string{value});
    descriptions.emplace_back(description);
  });

  batch = _query->BuildBatch({
    std::move(names),
    std::move(values),
    std::move(descriptions),
  });
  _query = nullptr;  // set fired
  return {};
}

}  // namespace sdb::query
