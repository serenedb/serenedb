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

#include <memory>

#include "basics/bit_utils.hpp"
#include "basics/fwd.h"
#include "basics/type_traits.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/identifier.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "pg/sql_collector.h"
#include "pg/sql_utils.h"
#include "query/config.h"

namespace sdb {
namespace aql {

class ExpressionContext;
struct AstNode;
struct Cell;
using FunctionImpl = Cell (*)(ExpressionContext*, const AstNode&,
                              std::span<const Cell>);

}  // namespace aql
namespace search {

class AnalyzerImpl;

}  // namespace search
namespace pg {

class FunctionImpl;

}  // namespace pg
namespace catalog {

enum class FunctionLanguage : uint8_t {
  Invalid = 0,
  SQL,
  VeloxNative,
  Decorator,
};

enum class FunctionState : uint8_t {
  Invalid = 0,
  Immutable,  // deterministic and cachable
  Stable,     // deterministic but not cachable
  Volatile,   // non-deterministic and not cacheable
};

enum class FunctionParallel : uint8_t {
  Invalid = 0,
  Safe,
  Restricted,
  Unsafe,
};

enum class FunctionType : uint8_t {
  Compute = 1 << 0,
  DQL = 1 << 1,
  // DML = 1 << 2,
  DDL = 1 << 3,
};

enum class FunctionKind : uint8_t {
  Scalar = 0,
  Aggregate,
  Window,
};

struct FunctionParameter {
  enum class Mode {
    Invalid = 0,
    In,
    Out,
    InOut,
    Variadic,
  };

  Mode mode;
  std::string name;
  velox::TypePtr type;

  // for AQL functions only
  bool IsCollection() const;
  void MarkAsCollection();
};

struct FunctionOptions {
  double cost = 1;
  double rows = 0;
  FunctionLanguage language = FunctionLanguage::Invalid;
  FunctionState state = FunctionState::Invalid;
  bool strict = false;    // called on null/returns null
  bool security = false;  // invoker/definer
  FunctionParallel parallel = FunctionParallel::Invalid;
  bool table = false;  // true -- returns table or returns setof
  // internal options
  FunctionType type = FunctionType::Compute;
  bool internal = false;
  bool no_pushdown = false;
  bool no_analyzer = false;
  bool no_eval = false;

  // TODO: maybe better to use velox language types instead of separate enum
  FunctionKind kind = FunctionKind::Scalar;

  bool IsAggregate() const noexcept { return kind == FunctionKind::Aggregate; }
  bool IsWindow() const noexcept { return kind == FunctionKind::Window; }
};

struct FunctionSignature {
  std::vector<catalog::FunctionParameter> parameters;
  uint16_t required_arguments = 0;
  uint16_t max_arguments = 0;
  velox::TypePtr return_type;

  bool Matches(const std::vector<velox::TypePtr>& arg_types) const;
  bool ReturnsTable() const;
  bool ReturnsVoid() const;

  bool IsProcedure() const;
  void MarkAsProcedure();
};

template<typename V, typename F>
  requires(type::kIsOneOf<V, FunctionOptions, FunctionSignature> &&
           std::invocable<F &&, V&>)
constexpr V&& operator|(V&& v, F&& f) {
  std::forward<F>(f)(v);
  return std::move(v);
}

// NOLINTBEGIN
struct FunctionProperties {
  FunctionSignature signature;
  FunctionOptions options;
  std::string name;
  ObjectId id;
  vpack::Slice implementation;

  static Result Read(FunctionProperties& options, vpack::Slice slice);
};
// NOLINTEND

class PgSqlFunction final : public SchemaObject {
 public:
  PgSqlFunction(ObjectId database_id, ObjectId id, std::string_view name,
                std::string query, FunctionSignature signature,
                FunctionOptions options);

  static std::shared_ptr<PgSqlFunction> ReadInternal(vpack::Slice slice,
                                                     ReadContext ctx);

  void WriteInternal(vpack::Builder&) const final;
  std::shared_ptr<Object> Clone() const final;

  const FunctionSignature& Signature() const noexcept { return _signature; }
  const FunctionOptions& Options() const noexcept { return _options; }
  std::string_view GetQuery() const noexcept { return _query; }
  const RawStmt* GetStatement() const noexcept { return _stmt; }
  const pg::Objects& GetObjects() const noexcept { return _objects; }

 private:
  FunctionSignature _signature;
  FunctionOptions _options;
  std::string _query;
  pg::MemoryContextPtr _memory_context;
  const RawStmt* _stmt{nullptr};
  pg::Objects _objects;
};

}  // namespace catalog
}  // namespace sdb
