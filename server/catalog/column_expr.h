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

#include <vpack/builder.h>
#include <vpack/slice.h>

#include <duckdb/parser/parsed_expression.hpp>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "basics/result.h"
#include "catalog/identifiers/object_id.h"

namespace duckdb {

class SelectStatement;
class QueryNode;
class LogicalType;

}  // namespace duckdb
namespace sdb {

struct QualifiedRef {
  std::string catalog;
  std::string schema;
  std::string name;
};

enum class RefKinds : uint8_t {
  None = 0,
  Sequences = 1U << 0,
  Relations = 1U << 1,
  Functions = 1U << 2,
  Types = 1U << 3,
  All = Sequences | Relations | Functions | Types,
};
ENABLE_BITMASK_ENUM(RefKinds);

struct Refs {
  std::vector<QualifiedRef> sequences;
  std::vector<QualifiedRef> relations;
  std::vector<QualifiedRef> functions;
  // Types named by CAST in expression bodies; caller resolves by name.
  std::vector<QualifiedRef> unbound_types;
  // Types already resolved (column types, function param/return); id is final.
  std::vector<ObjectId> types;
};

// Column expression (default value, computed column).
// In memory: DuckDB ParsedExpression.
// On disk: DuckDB BinarySerializer bytes in VPack.
class ColumnExpr {
 public:
  ColumnExpr() = default;
  explicit ColumnExpr(duckdb::unique_ptr<duckdb::ParsedExpression> expr);

  static Result FromVPack(vpack::Slice slice, ColumnExpr& column_expr);
  void ToVPack(vpack::Builder& builder) const;

  duckdb::ParsedExpression& GetExpr() const {
    SDB_ASSERT(_expr);
    return *_expr;
  }

  bool HasExpr() const { return _expr != nullptr; }

  Refs GetRefs(RefKinds kinds) const;

 private:
  duckdb::unique_ptr<duckdb::ParsedExpression> _expr;
};

Refs ExtractRefs(const duckdb::SelectStatement& stmt, RefKinds kinds);
Refs ExtractRefs(const duckdb::ParsedExpression& expr, RefKinds kinds);
Refs ExtractRefs(const duckdb::QueryNode& node, RefKinds kinds);

void CollectTypeRefs(const duckdb::LogicalType& type, Refs& out);

void VPackWrite(auto ctx, const ColumnExpr& column_expr) {
  column_expr.ToVPack(ctx.vpack());
}

void VPackRead(auto ctx, ColumnExpr& column_expr) {
  auto r = ColumnExpr::FromVPack(ctx.vpack(), column_expr);
  SDB_ENSURE(r.ok(), r.errorNumber(), r.errorMessage());
}

}  // namespace sdb
