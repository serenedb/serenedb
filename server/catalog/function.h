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

#include <basics/bit_utils.hpp>
#include <memory>

#include "basics/fwd.h"
#include "basics/type_traits.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/identifier.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"

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
  AqlNative,
  SQL,
  AnalyzerJson,
  VeloxNative,
  Decorator,
  WasmHex,
  WasmBase64,
  WasmBase64Web,
  WasmText,
  // PLpgSQL,
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

  static Result Read(FunctionProperties& options, vpack::Slice slice,
                     bool is_user_request = false);
};
// NOLINTEND

class Function final : public SchemaObject {
 public:
  static Result Instantiate(std::shared_ptr<catalog::Function>& function,
                            ObjectId database_id, vpack::Slice definition,
                            bool is_user_request);

  Function(std::string_view name, FunctionSignature signature,
           FunctionOptions options, aql::FunctionImpl impl);

  Function(std::string_view name, FunctionSignature signature,
           FunctionOptions options);

  Function(FunctionProperties&& properties,
           std::unique_ptr<search::AnalyzerImpl> impl, ObjectId database_id);

  Function(FunctionProperties&& properties,
           std::unique_ptr<pg::FunctionImpl> impl, ObjectId database_id);

  ~Function() final;

  void WriteProperties(vpack::Builder& build) const final;

  void WriteInternal(vpack::Builder& build) const final;

  const FunctionSignature& Signature() const noexcept { return _signature; }

  const FunctionOptions& Options() const noexcept { return _options; }

  aql::FunctionImpl AqlFunction() const noexcept {
    SDB_ASSERT(_options.language == FunctionLanguage::AqlNative);
    SDB_ASSERT(_aql_impl);
    return _aql_impl;
  }

  search::AnalyzerImpl& Analyzer() const noexcept {
    SDB_ASSERT(_options.language == FunctionLanguage::AnalyzerJson);
    SDB_ASSERT(_analyzer_impl);
    return *_analyzer_impl;
  }

  pg::FunctionImpl& SqlFunction() const noexcept {
    SDB_ASSERT(_options.language == FunctionLanguage::SQL);
    SDB_ASSERT(_sql_impl);
    return *_sql_impl;
  }

 private:
  FunctionSignature _signature;
  FunctionOptions _options;

  // TODO: use inheritance for different function implementations?
  aql::FunctionImpl _aql_impl;
  std::unique_ptr<search::AnalyzerImpl> _analyzer_impl;
  std::unique_ptr<pg::FunctionImpl> _sql_impl;
};

}  // namespace catalog
}  // namespace sdb

namespace magic_enum {
template<>
constexpr customize::customize_t
customize::enum_name<sdb::catalog::FunctionLanguage>(
  sdb::catalog::FunctionLanguage value) noexcept {
  switch (value) {
    using enum sdb::catalog::FunctionLanguage;
    case SQL:
      return "sql";
    case AqlNative:
      return "aql-native";
    case AnalyzerJson:
      return "analyzer-json";
    // TODO(mbkkt) wasm when it will be available
    default:
      return invalid_tag;
  }
}

template<>
constexpr customize::customize_t
customize::enum_name<sdb::catalog::FunctionState>(
  sdb::catalog::FunctionState value) noexcept {
  switch (value) {
    using enum sdb::catalog::FunctionState;
    case Immutable:
      return "immutable";
    case Stable:
      return "stable";
    case Volatile:
      return "volatile";
    default:
      return invalid_tag;
  }
}

template<>
constexpr customize::customize_t
customize::enum_name<sdb::catalog::FunctionParallel>(
  sdb::catalog::FunctionParallel value) noexcept {
  switch (value) {
    using enum sdb::catalog::FunctionParallel;
    case Safe:
      return "safe";
    case Restricted:
      return "restricted";
    case Unsafe:
      return "unsafe";
    default:
      return invalid_tag;
  }
}

}  // namespace magic_enum
