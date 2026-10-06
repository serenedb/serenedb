////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string_view>

// TODO(mbkkt) remove it, use per constant where they used, avoid single place
// with all string constants
namespace irs::StaticStrings {  // NOLINT

// Datadir subtrees: the data DB (store tables), the catalog WAL, and the
// iresearch storages.
inline constexpr std::string_view kDataStoreRoot = "engine_duckdb";
inline constexpr std::string_view kCatalogRoot = "engine_catalog";
inline constexpr std::string_view kSearchRoot = "engine_search";

// database names
inline constexpr std::string_view kDefaultDatabase = "postgres";
// user names
inline constexpr std::string_view kDefaultUser = "postgres";
// system schema names
inline constexpr std::string_view kPublic = "public";
inline constexpr std::string_view kPgCatalogSchema = "pg_catalog";
inline constexpr std::string_view kInformationSchema = "information_schema";
inline constexpr std::string_view kDocsSchema = "sdb_docs";

}  // namespace irs::StaticStrings
