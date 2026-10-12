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

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "server/utils/build.h"

namespace sdb::pg {
namespace {

struct ImplementationInfo {
  std::string_view id;
  std::string_view name;
  std::optional<int32_t> number;
  std::optional<std::string_view> text;
  std::optional<std::string_view> comments;
};

constexpr std::array kInfos{
  ImplementationInfo{"10003", "CATALOG NAME", std::nullopt, "Y", std::nullopt},
  ImplementationInfo{"10004", "COLLATING SEQUENCE", std::nullopt, "C.UTF-8",
                     std::nullopt},
  ImplementationInfo{"13", "SERVER NAME", std::nullopt, "", std::nullopt},
  ImplementationInfo{"17", "DBMS NAME", std::nullopt, "SereneDB", std::nullopt},
  ImplementationInfo{"18", "DBMS VERSION", std::nullopt, SERENEDB_VERSION,
                     std::nullopt},
  ImplementationInfo{"2", "DATA SOURCE NAME", std::nullopt, "", std::nullopt},
  ImplementationInfo{"23", "CURSOR COMMIT BEHAVIOR", 1, std::nullopt,
                     "close cursors and retain prepared statements"},
  ImplementationInfo{"26", "DEFAULT TRANSACTION ISOLATION", 4, std::nullopt,
                     "REPEATABLE READ; user-settable"},
  ImplementationInfo{"28", "IDENTIFIER CASE", 3, std::nullopt,
                     "stored in mixed case - case sensitive"},
  ImplementationInfo{"46", "TRANSACTION CAPABLE", 2, std::nullopt,
                     "both DML and DDL"},
  ImplementationInfo{"85", "NULL COLLATION", 4, std::nullopt,
                     "nulls sort at the end"},
  ImplementationInfo{"94", "SPECIAL CHARACTERS", std::nullopt, "",
                     "all non-ASCII characters allowed"},
};

class InfoSqlImplementationInfo final
  : public SystemTableScan<kInfoSqlImplementationInfoSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<ImplementationInfo>{&LoadStatic<kInfos>, {}, {}}};

  static constexpr auto kInfo = Shape<kSql, const ImplementationInfo>(
    Col<"implementation_info_id">(&ImplementationInfo::id),
    Col<"implementation_info_name">(&ImplementationInfo::name),
    Col<"integer_value">(&ImplementationInfo::number),
    Col<"character_value">(&ImplementationInfo::text),
    Col<"comments">(&ImplementationInfo::comments));

  void Row(const ImplementationInfo& info) { Emit<kInfo>(info); }
};

}  // namespace

SystemTable gInfoSqlImplementationInfo =
  SystemTableOf<InfoSqlImplementationInfo>();

}  // namespace sdb::pg
