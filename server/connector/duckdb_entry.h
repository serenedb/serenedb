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

#include <duckdb/common/enums/catalog_type.hpp>

namespace sdb::connector {

// The one CatalogType a kind is recorded, keyed and granted by: a function's
// two macro slots are one kind, and every other slot is its own.
constexpr duckdb::CatalogType KindOf(duckdb::CatalogType type) noexcept {
  return type == duckdb::CatalogType::TABLE_MACRO_ENTRY
           ? duckdb::CatalogType::MACRO_ENTRY
           : type;
}

}  // namespace sdb::connector
