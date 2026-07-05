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

#include "pg/information_schema/sql_sizing.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullComments = MaskFromNulls({
  GetIndex(&SqlSizing::comments),
});
constexpr uint64_t kNullSupportedAndComments = MaskFromNulls({
  GetIndex(&SqlSizing::supported_value),
  GetIndex(&SqlSizing::comments),
});

struct Row {
  SqlSizing data;
  uint64_t nulls;
};

constexpr std::string_view kNameComment =
  "Might be less, depending on character set.";

constexpr Row kRows[] = {
  // clang-format off
  {{34, "MAXIMUM CATALOG NAME LENGTH",                      63, kNameComment}, 0                        },
  {{30,    "MAXIMUM COLUMN NAME LENGTH",                    63, kNameComment}, 0                        },
  {{97,    "MAXIMUM COLUMNS IN GROUP BY",                    0, {}          }, kNullComments            },
  {{99,    "MAXIMUM COLUMNS IN ORDER BY",                    0, {}          }, kNullComments            },
  {{100,   "MAXIMUM COLUMNS IN SELECT",                   1664, {}          }, kNullComments            },
  {{101,   "MAXIMUM COLUMNS IN TABLE",                    1600, {}          }, kNullComments            },
  {{1,     "MAXIMUM CONCURRENT ACTIVITIES",                  0, {}          }, kNullComments            },
  {{31,    "MAXIMUM CURSOR NAME LENGTH",                    63, kNameComment}, 0                        },
  {{0,     "MAXIMUM DRIVER CONNECTIONS",                     0, {}          }, kNullSupportedAndComments},
  {{10005, "MAXIMUM IDENTIFIER LENGTH",                     63, kNameComment}, 0                        },
  {{32,    "MAXIMUM SCHEMA NAME LENGTH",                    63, kNameComment}, 0                        },
  {{20000, "MAXIMUM STATEMENT OCTETS",                       0, {}          }, kNullComments            },
  {{20001, "MAXIMUM STATEMENT OCTETS DATA",                  0, {}          }, kNullComments            },
  {{20002, "MAXIMUM STATEMENT OCTETS SCHEMA",                0, {}          }, kNullComments            },
  {{35,    "MAXIMUM TABLE NAME LENGTH",                     63, kNameComment}, 0                        },
  {{106,   "MAXIMUM TABLES IN SELECT",                       0, {}},           kNullComments            },
  {{107,   "MAXIMUM USER NAME LENGTH",                      63, kNameComment}, 0                        },
  {{25000, "MAXIMUM CURRENT DEFAULT TRANSFORM GROUP LENGTH", 0, {}          }, kNullSupportedAndComments},
  {{25001, "MAXIMUM CURRENT TRANSFORM GROUP LENGTH",         0, {}          }, kNullSupportedAndComments},
  {{25002, "MAXIMUM CURRENT PATH LENGTH",                    0, {}          }, kNullComments            },
  {{25003, "MAXIMUM CURRENT ROLE LENGTH",                    0, {}          }, kNullSupportedAndComments},
  {{25004, "MAXIMUM SESSION USER LENGTH",                   63, kNameComment}, 0                        },
  {{25005, "MAXIMUM SYSTEM USER LENGTH",                    63, kNameComment}, 0                        },
  // clang-format on
};

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<SqlSizing>::GetTableData() {
  constexpr auto kNumRows = std::size(kRows);
  auto result = CreateColumns<SqlSizing>(kNumRows);
  for (size_t row = 0; row < kNumRows; ++row) {
    WriteData(result, kRows[row].data, kRows[row].nulls, row,
              *_config.CatalogSnapshot());
  }
  return {std::move(result), kNumRows};
}

}  // namespace sdb::pg
