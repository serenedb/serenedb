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

#include <cstdint>

namespace sdb::catalog {

enum class PkSpec : uint8_t {
  FileRowNumber,
  FileIndexPlusRowNumber,
  FileOffset,
  FileIndexPlusOffset,
  // Native duckdb files read via read_duckdb: the duckdb rowid is the PK.
  DuckDBRowId,
  FileIndexPlusDuckDBRowId,
  // Attached external DB (pg/CH): matched rows re-fetch by value via SQL.
  //   ExternalPostgresCtid -- the postgres ctid (travels as the duckdb rowid).
  //   ExternalColumnKey    -- 1..N real columns (engine PK metadata or key_columns).
  ExternalPostgresCtid,
  ExternalColumnKey,
};

constexpr bool IsExternalPK(PkSpec spec) noexcept {
  return spec == PkSpec::ExternalPostgresCtid || spec == PkSpec::ExternalColumnKey;
}

constexpr bool IsGlobPK(PkSpec spec) noexcept {
  return spec == PkSpec::FileIndexPlusRowNumber ||
         spec == PkSpec::FileIndexPlusOffset ||
         spec == PkSpec::FileIndexPlusDuckDBRowId;
}

}  // namespace sdb::catalog
