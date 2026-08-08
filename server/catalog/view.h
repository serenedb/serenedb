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

#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <memory>
#include <string_view>
#include <utility>

#include "catalog/column_expr.h"
#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

// A view is duckdb's own CreateViewInfo: the ids ride CreateInfo::oid and
// parent_oid, and what its query resolved to rides
// CreateInfo::dependencies. Owner and ACL are not in it -- they live on the

inline std::string_view ViewName(const duckdb::CreateViewInfo& info) noexcept {
  return info.GetViewName().GetIdentifierName();
}

// The names a view's query references, to be resolved into dependency edges.
Refs ViewRefs(const duckdb::CreateViewInfo& info, RefKinds kinds);

}  // namespace sdb::catalog
