////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vpack/serializer.h>
#include <vpack/slice.h>

#include "basics/result.h"
#include "catalog/object.h"
#include "pg/sql_collector.h"
#include "pg/sql_utils.h"
#include "query/config.h"

namespace sdb::catalog {

class View final : public SchemaObject {
 public:
  struct State {
    pg::MemoryContextPtr memory_context;
    const RawStmt* stmt = nullptr;
    pg::Objects objects;
  };

  static std::shared_ptr<View> ReadInternal(vpack::Slice slice,
                                            ReadContext ctx);
  static ResultOr<std::shared_ptr<View>> Create(ObjectId database_id,
                                                std::string_view name,
                                                std::string query,
                                                const Config* config);

  View(ObjectId database_id, ObjectId id, std::string_view name,
       std::string query, std::shared_ptr<const State> state);

  void WriteInternal(vpack::Builder& b) const final;
  std::shared_ptr<Object> Clone() const final;

  std::string_view GetQuery() const noexcept { return _query; }
  auto GetState() const noexcept { return _state; }

 private:
  std::string _query;
  std::shared_ptr<const State> _state;
};

}  // namespace sdb::catalog
