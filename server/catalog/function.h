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

#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <memory>
#include <string_view>
#include <utility>

#include "catalog/column_expr.h"
#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

// A SQL function is duckdb's own CreateMacroInfo -- all the overloads of one
// name share one, as duckdb's macro entry does. The ids ride
// CreateInfo::oid and parent_oid, and what the bodies resolved to rides
// CreateInfo::dependencies. Owner and ACL are not in it -- they live on the
// entry, which is their one home, and a reader wanting both takes a

inline std::string_view FunctionName(
  const duckdb::CreateMacroInfo& info) noexcept {
  return info.GetFunctionName().GetIdentifierName();
}

// The names every overload's body references, to be resolved into dependency
// edges.
Refs MacroRefs(const duckdb::CreateMacroInfo& info, RefKinds kinds);

}  // namespace sdb::catalog
