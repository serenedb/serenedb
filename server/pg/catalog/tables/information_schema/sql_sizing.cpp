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

#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr std::string_view kFullNames = "SereneDB keeps longer names in full.";

struct SqlSizing {
  int32_t id;
  std::string_view name;
  std::optional<int32_t> value;
  std::optional<std::string_view> comments;
};

constexpr std::array kSizings{
  SqlSizing{0, "MAXIMUM DRIVER CONNECTIONS", std::nullopt, std::nullopt},
  SqlSizing{1, "MAXIMUM CONCURRENT ACTIVITIES", 0, std::nullopt},
  SqlSizing{30, "MAXIMUM COLUMN NAME LENGTH", 63, kFullNames},
  SqlSizing{31, "MAXIMUM CURSOR NAME LENGTH", 63, kFullNames},
  SqlSizing{32, "MAXIMUM SCHEMA NAME LENGTH", 63, kFullNames},
  SqlSizing{34, "MAXIMUM CATALOG NAME LENGTH", 63, kFullNames},
  SqlSizing{35, "MAXIMUM TABLE NAME LENGTH", 63, kFullNames},
  SqlSizing{97, "MAXIMUM COLUMNS IN GROUP BY", 0, std::nullopt},
  SqlSizing{99, "MAXIMUM COLUMNS IN ORDER BY", 0, std::nullopt},
  SqlSizing{100, "MAXIMUM COLUMNS IN SELECT", 1664, std::nullopt},
  SqlSizing{101, "MAXIMUM COLUMNS IN TABLE", 1600, std::nullopt},
  SqlSizing{106, "MAXIMUM TABLES IN SELECT", 0, std::nullopt},
  SqlSizing{107, "MAXIMUM USER NAME LENGTH", 63, kFullNames},
  SqlSizing{10005, "MAXIMUM IDENTIFIER LENGTH", 63, kFullNames},
  SqlSizing{20000, "MAXIMUM STATEMENT OCTETS", 0, std::nullopt},
  SqlSizing{20001, "MAXIMUM STATEMENT OCTETS DATA", 0, std::nullopt},
  SqlSizing{20002, "MAXIMUM STATEMENT OCTETS SCHEMA", 0, std::nullopt},
  SqlSizing{25000, "MAXIMUM CURRENT DEFAULT TRANSFORM GROUP LENGTH",
            std::nullopt, std::nullopt},
  SqlSizing{25001, "MAXIMUM CURRENT TRANSFORM GROUP LENGTH", std::nullopt,
            std::nullopt},
  SqlSizing{25002, "MAXIMUM CURRENT PATH LENGTH", 0, std::nullopt},
  SqlSizing{25003, "MAXIMUM CURRENT ROLE LENGTH", std::nullopt, std::nullopt},
  SqlSizing{25004, "MAXIMUM SESSION USER LENGTH", 63, kFullNames},
  SqlSizing{25005, "MAXIMUM SYSTEM USER LENGTH", 63, kFullNames},
};

constexpr auto kSizing = Shape<kInfoSqlSizingSql, const SqlSizing>(
  Col<"sizing_id">(&SqlSizing::id), Col<"sizing_name">(&SqlSizing::name),
  Col<"supported_value">(&SqlSizing::value),
  Col<"comments">(&SqlSizing::comments));

}  // namespace

SystemTable gInfoSqlSizing = SystemTableOf<StaticTable<kSizing, kSizings>>();

}  // namespace sdb::pg
