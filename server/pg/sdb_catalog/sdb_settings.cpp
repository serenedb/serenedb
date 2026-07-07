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

#include "sdb_settings.h"

#include <absl/flags/commandlineflag.h>
#include <absl/flags/reflection.h>

#include <algorithm>
#include <string>
#include <vector>

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&SdbSettings::name),
  GetIndex(&SdbSettings::setting),
  GetIndex(&SdbSettings::short_desc),
  GetIndex(&SdbSettings::context),
  GetIndex(&SdbSettings::vartype),
  GetIndex(&SdbSettings::source),
  GetIndex(&SdbSettings::boot_val),
  GetIndex(&SdbSettings::reset_val),
  GetIndex(&SdbSettings::pending_restart),
});

std::string_view VarType(const absl::CommandLineFlag& flag) {
  if (flag.IsOfType<bool>()) {
    return "bool";
  }
  if (flag.IsOfType<int32_t>() || flag.IsOfType<int64_t>() ||
      flag.IsOfType<uint32_t>() || flag.IsOfType<uint64_t>()) {
    return "integer";
  }
  if (flag.IsOfType<float>() || flag.IsOfType<double>()) {
    return "real";
  }
  return "string";
}

bool IsSecret(std::string_view name) {
  return name == "auth_password" || name == "auth_api_key" ||
         name == "auth_bearer_token";
}

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<SdbSettings>::GetTableData() {
  std::vector<absl::CommandLineFlag*> flags;
  for (auto [_, flag] : absl::GetAllFlags()) {
    flags.push_back(flag);
  }
  std::ranges::sort(
    flags, {}, [](const absl::CommandLineFlag* flag) { return flag->Name(); });

  std::vector<std::string> storage;
  storage.reserve(flags.size() * 4);
  std::vector<SdbSettings> values;
  values.reserve(flags.size());
  for (const auto* flag : flags) {
    const auto& current = storage.emplace_back(flag->CurrentValue());
    const auto& help = storage.emplace_back(flag->Help());
    const auto& def = storage.emplace_back(flag->DefaultValue());
    const std::string_view setting = IsSecret(flag->Name()) && !current.empty()
                                       ? std::string_view{"***"}
                                       : std::string_view{current};
    values.push_back({
      .name = flag->Name(),
      .setting = setting,
      .short_desc = help,
      .context = "postmaster",
      .vartype = VarType(*flag),
      .source = current == def ? "default" : "command line",
      .boot_val = def,
      .reset_val = def,
      .pending_restart = false,
    });
  }

  auto result = CreateColumns<SdbSettings>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, *_config.CatalogSnapshot());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
