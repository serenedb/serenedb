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

#include "pg/pg_catalog/pg_language.h"

#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

// PostgreSQL-compatible language OIDs
constexpr Oid kLangInternalOid = 12;
constexpr Oid kLangSqlOid = 14;

constexpr auto kSampleData = std::to_array<PgLanguage>({
  {
    .oid = kLangInternalOid,
    .lanname = "internal",
    .lanowner = id::kRootUser.id(),
    .lanispl = false,
    .lanpltrusted = false,
    .lanplcallfoid = 0,
    .laninline = 0,
    .lanvalidator = 0,
  },
  {
    .oid = kLangSqlOid,
    .lanname = "sql",
    .lanowner = id::kRootUser.id(),
    .lanispl = false,
    .lanpltrusted = true,
    .lanplcallfoid = 0,
    .laninline = 0,
    .lanvalidator = 0,
  },
});

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgLanguage::lanacl),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgLanguage>::GetTableData() {
  auto result = CreateColumns<PgLanguage>(kSampleData.size());
  for (size_t row = 0; row < kSampleData.size(); ++row) {
    WriteData(result, kSampleData[row], kNullMask, row,
              *_config.EnsureCatalogSnapshot());
  }
  return {std::move(result), kSampleData.size()};
}

}  // namespace sdb::pg
