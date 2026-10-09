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

#include <optional>
#include <span>
#include <string_view>

#include "network/pg/hba.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

using HbaRule = network::pg::hba::RenderedRule;

SystemRows<HbaRule> LoadHbaRules(SystemScan&) {
  return network::pg::hba::RenderHbaRules();
}

class PgHbaFileRules final : public SystemTableScan<kPgHbaFileRulesSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{ArraySource<HbaRule>{&LoadHbaRules, {}}};

  static constexpr auto kRule = Shape<kSql, const HbaRule>(
    Col<"rule_number">(&HbaRule::rule_number),
    Col<"file_name">([](const auto& rule) {
      return NonEmpty<std::string_view>(rule.file_name);
    }),
    Col<"line_number">([](const auto& rule) -> std::optional<uint32_t> {
      if (rule.line_number == 0) {
        return std::nullopt;
      }
      return rule.line_number;
    }),
    Col<"type">(&HbaRule::type), Col<"database">(&HbaRule::databases),
    Col<"user_name">(&HbaRule::roles), Col<"address">([](const auto& rule) {
      return NonEmpty<std::string_view>(rule.address);
    }),
    Col<"netmask">([](const auto& rule) {
      return NonEmpty<std::string_view>(rule.netmask);
    }),
    Col<"auth_method">(&HbaRule::auth_method),
    Col<"options">(
      [](const auto& rule) { return NonEmpty(std::span{rule.options}); }));

  void Row(const HbaRule& rule) { Emit<kRule>(rule); }
};

}  // namespace

SystemTable gPgHbaFileRules = SystemTableOf<PgHbaFileRules>();

}  // namespace sdb::pg
