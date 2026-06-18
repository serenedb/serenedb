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

#include "pg/information_schema/sql_implementation_info.h"

#include "rest/version.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullIntVal = MaskFromNulls({
  GetIndex(&SqlImplementationInfo::integer_value),
});
constexpr uint64_t kNullCharVal = MaskFromNulls({
  GetIndex(&SqlImplementationInfo::character_value),
});
constexpr uint64_t kNullComments = MaskFromNulls({
  GetIndex(&SqlImplementationInfo::comments),
});

struct Row {
  SqlImplementationInfo data;
  uint64_t nulls;
};

constexpr Row kRows[] = {
  // clang-format off
  {{"10003", "CATALOG NAME",                  0, "Y",              {}                                            }, kNullIntVal | kNullComments},
  {{"10004", "COLLATING SEQUENCE",            0, "",               {}                                            }, kNullIntVal | kNullComments},
  {{"23",    "CURSOR COMMIT BEHAVIOR",        1, {},               "close cursors and retain prepared statements"}, kNullCharVal               },
  {{"2",     "DATA SOURCE NAME",              0, "",               {}                                            }, kNullIntVal | kNullComments},
  {{"17",    "DBMS NAME",                     0, "SereneDB",       {}                                            }, kNullIntVal | kNullComments},
  {{"18",    "DBMS VERSION",                  0, SERENEDB_VERSION, {}                                            }, kNullIntVal | kNullComments},
  {{"26",    "DEFAULT TRANSACTION ISOLATION", 2, {},               "READ COMMITTED; user-settable"               }, kNullCharVal               },
  {{"28",    "IDENTIFIER CASE",               3, {},               "stored in mixed case - case sensitive"       }, kNullCharVal               },
  {{"85",    "NULL COLLATION",                0, {},               "nulls higher than non-nulls"                 }, kNullCharVal               },
  {{"13",    "SERVER NAME",                   0, "",               {}                                            }, kNullIntVal | kNullComments},
  {{"94",    "SPECIAL CHARACTERS",            0, "",               "all non-ASCII characters allowed"            }, kNullIntVal                },
  {{"46",    "TRANSACTION CAPABLE",           2, {},               "both DML and DDL"                            }, kNullCharVal               },
  // clang-format on
};

}  // namespace

template<>
catalog::MaterializedData
SystemTableSnapshot<SqlImplementationInfo>::GetTableData() {
  constexpr auto kNumRows = std::size(kRows);
  auto result = CreateColumns<SqlImplementationInfo>(kNumRows);
  for (size_t row = 0; row < kNumRows; ++row) {
    WriteData(result, kRows[row].data, kRows[row].nulls, row,
              *_config.EnsureCatalogSnapshot());
  }
  return {std::move(result), kNumRows};
}

}  // namespace sdb::pg
