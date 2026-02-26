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

#pragma once

#include "catalog/base_query_view.h"
#include "pg/sql_collector.h"
#include "pg/sql_utils.h"

namespace sdb {

class SqlQueryViewImpl {
 public:
  struct State {
    pg::MemoryContextPtr memory_context;
    const RawStmt* stmt = nullptr;
    pg::Objects objects;
    const Config* config = nullptr;
    // TODO(mbkkt) warnings?
  };

  static constexpr auto Type() noexcept {
    return catalog::ViewType::ViewSqlQuery;
  }

 protected:
  static std::shared_ptr<State> Create(const Config* config);

  static Result Parse(State& state, ObjectId database, std::string_view query);

  static Result Check(ObjectId database, std::string_view name,
                      const State& state);
};

using SqlQueryView = catalog::BaseQueryView<SqlQueryViewImpl>;

}  // namespace sdb
