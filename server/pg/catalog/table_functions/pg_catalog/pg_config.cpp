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

#include <absl/strings/str_cat.h>

#include <array>
#include <span>
#include <string>
#include <string_view>

#include "pg/catalog/tables/tables.h"
#include "pg/version.h"
#include "server/utils/build.h"
#include "server/utils/build_info.h"
#include "server/utils/build_options.h"

namespace sdb::pg {
namespace {

struct ConfigSetting {
  std::string_view name;
  std::string_view setting;
};

SystemRows<ConfigSetting> LoadSettings(SystemScan&) {
  static const auto kVersion = absl::StrCat("PostgreSQL ", kPgServerVersion);
  static const std::array kSettings{
    ConfigSetting{"BINDIR", SERENEDB_BINDIR},
    ConfigSetting{"SYSCONFDIR", SERENEDB_SYSCONFDIR},
    ConfigSetting{"CONFIGURE", SERENEDB_CONFIGURE},
    ConfigSetting{"CC", SERENEDB_CC},
    ConfigSetting{"CXX", SERENEDB_CXX},
    ConfigSetting{"CFLAGS", SERENEDB_CFLAGS},
    ConfigSetting{"CXXFLAGS", SERENEDB_CXXFLAGS},
    ConfigSetting{"LDFLAGS", SERENEDB_LDFLAGS},
    ConfigSetting{"VERSION", kVersion},
    ConfigSetting{"SERENEDB_VERSION", SERENEDB_VERSION},
    ConfigSetting{"SERENEDB_COMMIT", build::Commit()},
  };
  return std::span<const ConfigSetting>{kSettings};
}

constexpr auto kSetting = Shape<kPgConfigSql, const ConfigSetting>(
  Col<"name">(&ConfigSetting::name), Col<"setting">(&ConfigSetting::setting));

}  // namespace

SystemTable gPgConfig = SystemTableOf<ArrayTable<kSetting, &LoadSettings>>();

}  // namespace sdb::pg
