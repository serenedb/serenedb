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

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/planner/expression.hpp>
#include <memory>

#include "catalog1/entry/inverted_index.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

// The runtime config of a fresh CREATE INDEX, from the entry duckdb built out
// of the statement plus the binder's output: the bound key expressions and,
// for a view-backed index, the row key type the view planner derived.
std::shared_ptr<const catalog::InvertedIndexConfig> BindInvertedIndexConfig(
  duckdb::ClientContext& context, const duckdb::IndexCatalogEntry& entry,
  duckdb::CatalogEntry& relation,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>&
    bound_expressions,
  const duckdb::LogicalType& generated_pk_type);

}  // namespace sdb::connector
