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

#include "pg/pg_catalog/pg_type.h"
#include "pg/system_table.h"

namespace sdb::pg {

// https://www.postgresql.org/docs/18/catalog-pg-attribute.html
// NOLINTBEGIN
struct PgAttribute {
  static constexpr uint64_t kId = 106;
  static constexpr std::string_view kName = "pg_attribute";

  enum class Attstorage : char {
    Plain = 'p',
    External = 'e',
    Extended = 'x',
    Main = 'm',
  };

  enum class Attcompression : char {
    None = '\0',
    Pglz = 'p',
    Lz4 = 'l',
  };

  enum class Attidentity : char {
    None = '\0',
    Always = 'a',
    Default = 'd',
  };

  enum class Attgenerated : char {
    None = '\0',
    Stored = 's',
    Virtual = 'v',
  };

  Oid attrelid;
  Name attname;
  Oid atttypid;
  int16_t attlen;
  int16_t attnum;
  int32_t atttypmod;
  int16_t attndims;
  bool attbyval;
  PgType::Typalign attalign;
  Attstorage attstorage;
  Attcompression attcompression;
  bool attnotnull;
  bool atthasdef;
  bool atthasmissing;
  Attidentity attidentity;
  Attgenerated attgenerated;
  bool attisdropped;
  bool attislocal;
  int16_t attinhcount;
  Oid attcollation;
  int16_t attstattarget;
  AclColumn attacl;
  Array<Text> attoptions;
  Array<Text> attfdwoptions;
  Anyarray attmissingval;
};
// NOLINTEND

template<>
catalog::MaterializedData SystemTableSnapshot<PgAttribute>::GetTableData();

}  // namespace sdb::pg
