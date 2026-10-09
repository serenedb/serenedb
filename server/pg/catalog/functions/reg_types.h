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

#include <array>
#include <cstdint>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <optional>
#include <string>
#include <string_view>

#include "pg/types.h"

namespace sdb::pg {

struct Session;

enum class RegKind : uint8_t {
  Proc,
  Procedure,
  Oper,
  Operator,
  Class,
  Type,
  Collation,
  Config,
  Dictionary,
  Role,
  Namespace,
};

struct RegType {
  RegKind kind;
  std::string_view alias;
  PgTypeOID oid;
  PgTypeOID array;
  std::string_view to_reg;
};

inline constexpr std::array kRegTypes{
  RegType{RegKind::Proc, kRegprocAlias, kRegproc, kRegprocArray, "to_regproc"},
  RegType{RegKind::Procedure, kRegprocedureAlias, kRegprocedure,
          kRegprocedureArray, "to_regprocedure"},
  RegType{RegKind::Oper, kRegoperAlias, kRegoper, kRegoperArray, "to_regoper"},
  RegType{RegKind::Operator, kRegoperatorAlias, kRegoperator, kRegoperatorArray,
          "to_regoperator"},
  RegType{RegKind::Class, kRegclassAlias, kRegclass, kRegclassArray,
          "to_regclass"},
  RegType{RegKind::Type, kRegtypeAlias, kRegtype, kRegtypeArray, "to_regtype"},
  RegType{RegKind::Collation, kRegcollationAlias, kRegcollation,
          kRegcollationArray, "to_regcollation"},
  RegType{RegKind::Config, kRegconfigAlias, kRegconfig, kRegconfigArray, ""},
  RegType{RegKind::Dictionary, kRegdictionaryAlias, kRegdictionary,
          kRegdictionaryArray, ""},
  RegType{RegKind::Role, kRegroleAlias, kRegrole, kRegroleArray, "to_regrole"},
  RegType{RegKind::Namespace, kRegnamespaceAlias, kRegnamespace,
          kRegnamespaceArray, "to_regnamespace"},
};

const RegType* FindRegType(const duckdb::LogicalType& type);
duckdb::LogicalType RegLogicalType(const RegType& reg);

std::string FormatTypeOut(const Session& session, uint64_t oid,
                          std::optional<int32_t> typmod);

template<RegKind Kind>
std::string_view RegOut(const Session& session, uint64_t oid);
template<RegKind Kind>
std::optional<uint64_t> RegIn(const Session& session, std::string_view text,
                              bool missing_ok);
std::optional<int32_t> RegTypmodIn(const Session& session,
                                   std::string_view text);

uint64_t ResolveRelation(duckdb::ClientContext& context,
                         const duckdb::QualifiedName& name);
std::string RelationName(const Session& session, std::string_view schema,
                         std::string_view name);

bool RelationVisible(const Session& session, std::string_view schema,
                     std::string_view name);
std::optional<bool> RelationIsVisible(const Session& session, uint64_t oid);
std::optional<bool> TypeIsVisible(const Session& session, uint64_t oid);
std::optional<bool> FunctionIsVisible(const Session& session, uint64_t oid);

}  // namespace sdb::pg
