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

#include <vpack/slice.h>

#include "function.h"
#include "pg/sql_collector.h"
#include "pg/sql_utils.h"
#include "query/config.h"

namespace sdb::pg {

class FunctionImpl {
 public:
  FunctionImpl() = default;

  Result Init(ObjectId database, std::string_view name, std::string query,
              bool is_procedure, const Config* config);

  static Result FromVPack(ObjectId database, vpack::Slice slice,
                          std::unique_ptr<FunctionImpl>& implementation,
                          bool is_procedure);

  void ToVPack(vpack::Builder& builder) const;

  std::string_view GetQuery() const noexcept { return _query; }

  const RawStmt* GetStatement() const noexcept { return _stmt; }

  pg::Objects& GetObjects() noexcept { return _objects; }

 private:
  std::string _query;
  pg::MemoryContextPtr _memory_context;
  const RawStmt* _stmt{nullptr};
  pg::Objects _objects;
  // TODO(mbkkt) warnings?
};

}  // namespace sdb::pg
