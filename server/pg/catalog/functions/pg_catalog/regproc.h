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

#include <cstdint>
#include <duckdb/parser/qualified_name.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

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

using OutText = std::variant<std::string_view, std::string>;

inline std::string_view AsView(const OutText& text) {
  if (const auto* view = std::get_if<std::string_view>(&text)) {
    return *view;
  }
  return *std::get_if<std::string>(&text);
}

std::string QualifiedOutName(std::string_view schema, std::string_view name);

std::string_view SqlTypeName(int32_t oid);

OutText FormatTypeOut(const Session& session, uint64_t oid,
                      std::optional<int32_t> typmod);

template<RegKind Kind>
std::string_view RegOut(const Session& session, uint64_t oid);
template<RegKind Kind>
std::optional<uint64_t> RegIn(const Session& session, std::string_view text,
                              bool missing_ok);

std::optional<uint64_t> SchemaOid(const Session& session,
                                  std::string_view schema, bool missing_ok);
uint64_t ResolveRelation(const Session& session,
                         const duckdb::QualifiedName& name);
OutText RelationName(const Session& session, std::string_view schema,
                     std::string_view name);

bool RelationVisible(const Session& session, std::string_view schema,
                     std::string_view name);
std::optional<bool> RelationIsVisible(const Session& session, uint64_t oid);
std::optional<bool> TypeIsVisible(const Session& session, uint64_t oid);
std::optional<bool> FunctionIsVisible(const Session& session, uint64_t oid);

}  // namespace sdb::pg
