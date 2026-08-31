////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <absl/functional/function_ref.h>

#include <cstddef>
#include <duckdb/catalog/dependency.hpp>
#include <duckdb/catalog/dependency_list.hpp>
#include <duckdb/catalog/dependency_manager.hpp>

#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"

namespace duckdb {

class CatalogEntry;
class ClientContext;

}  // namespace duckdb
namespace sdb::catalog {

duckdb::CatalogEntryInfo DependencyInfo(ObjectId id);

ObjectId DependencyInfoId(const duckdb::CatalogEntryInfo& info) noexcept;

duckdb::LogicalDependencyList EntryDependencies(const duckdb::CreateInfo& info);

void SetEntryDependencies(duckdb::ClientContext* context,
                          duckdb::CatalogEntry& entry,
                          const duckdb::LogicalDependencyList& deps);

duckdb::DependencyManager::Attachments EdgeAttachments(
  duckdb::ClientContext& context);

void VisitAllEdges(
  duckdb::ClientContext& context,
  absl::FunctionRef<void(ObjectId referenced, ObjectId dependent)> visitor);

}  // namespace sdb::catalog
