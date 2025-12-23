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

#include "pg/sql_collector.h"
#include "pg/sql_utils.h"

namespace sdb {

// Column Expression which can be serialized / deserialized
class ColumnExpr {
 public:
  ColumnExpr() = default;

  Result Init(ObjectId database, Node* expr);

  static Result FromVPack(ObjectId database, vpack::Slice slice,
                          ColumnExpr& column_expr);

  void ToVPack(vpack::Builder& builder) const;

  std::string_view GetQuery() const noexcept { return _query; }

  const Node* GetExpr() const noexcept {
    SDB_ASSERT(_expr != nullptr);
    return _expr;
  }

  const pg::Objects& GetObjects() const noexcept { return _objects; }

 private:
  Result Init(ObjectId database, std::string query);

  std::string _query;
  pg::MemoryContextPtr _memory_context;
  const Node* _expr{nullptr};
  pg::Objects _objects;
};

void VPackWrite(auto ctx, const ColumnExpr& column_expr) {
  column_expr.ToVPack(ctx.vpack());
}

void VPackRead(auto ctx, ColumnExpr& column_expr) {
  auto database_id = ctx.arg().database_id;
  auto r = ColumnExpr::FromVPack(database_id, ctx.vpack(), column_expr);
  SDB_ENSURE(r.ok(), r.errorNumber(), r.errorMessage());
}

}  // namespace sdb
