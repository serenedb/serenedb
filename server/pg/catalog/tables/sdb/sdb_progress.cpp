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

#include <memory>
#include <string_view>

#include "auth/role_closure.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "pg/progress_registry.h"

namespace sdb::pg {
namespace {

SystemRows<ProgressSnapshot> LoadSnapshots(SystemScan&) {
  return ProgressRegistry::Instance().GetSnapshots();
}

constexpr std::tuple kProgressBase{Col<"pid">(&ProgressSnapshot::pid),
                                   Col<"datid">(&ProgressSnapshot::datid),
                                   Col<"usename">(&ProgressSnapshot::user),
                                   Col<"datname">(&ProgressSnapshot::database)};

constexpr std::tuple kProgressSession{
  Col<"state">([](const auto& s) {
    return std::string_view{s.query_start_us != 0 ? "active" : "idle"};
  }),
  Col<"query">(&ProgressSnapshot::query),
  Col<"backend_start_us">(&ProgressSnapshot::backend_start_us)};

constexpr std::tuple kProgressActive{
  Col<"query_start_us">(&ProgressSnapshot::query_start_us),
  Col<"rows_processed">(&ProgressSnapshot::rows_processed),
  Col<"rows_total">(&ProgressSnapshot::rows_total),
  Col<"tuples_processed">(&ProgressSnapshot::tuples_processed),
  Col<"bytes_processed">(&ProgressSnapshot::bytes_processed),
  Col<"percent">([](const auto& s) -> std::optional<double> {
    if (s.percent < 0) {
      return std::nullopt;
    }
    return s.percent;
  })};

constexpr std::tuple kProgressCommand{
  Col<"command">([](const auto& s) {
    return ProgressCommandName(static_cast<ProgressCommand>(s.command));
  }),
  Col<"io_type">([](const auto& s) {
    return ProgressIoTypeName(static_cast<ProgressIoType>(s.io_type));
  }),
  Col<"relid">(&ProgressSnapshot::relid),
  Col<"current_relid">(&ProgressSnapshot::current_relid),
  Col<"phase">([](const auto& s) {
    return ProgressPhaseName(static_cast<ProgressCommand>(s.command), s.phase);
  }),
  Col<"bytes_total">(&ProgressSnapshot::bytes_total),
  Col<"tuples_total">(&ProgressSnapshot::tuples_total),
  Col<"stage">(&ProgressSnapshot::stage),
  Col<"stages_total">(&ProgressSnapshot::stages_total),
  Col<"step">(&ProgressSnapshot::step),
  Col<"steps_total">(&ProgressSnapshot::steps_total),
  Col<"items_processed">(&ProgressSnapshot::items_processed),
  Col<"items_total">(&ProgressSnapshot::items_total)};

class SdbProgress final : public SystemTableScan<kSdbProgressSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<ProgressSnapshot>{&LoadSnapshots, {}}};

  static constexpr auto kHidden = Shape<kSql, const ProgressSnapshot>(
    kProgressBase, Col<"query">([](const auto&) {
      return std::string_view{"<insufficient privilege>"};
    }));

  static constexpr auto kIdle =
    Shape<kSql, const ProgressSnapshot>(kProgressBase, kProgressSession);

  static constexpr auto kActive = Shape<kSql, const ProgressSnapshot>(
    kProgressBase, kProgressSession, kProgressActive);

  static constexpr auto kCommand = Shape<kSql, const ProgressSnapshot>(
    kProgressBase, kProgressSession, kProgressActive, kProgressCommand);

  void Row(const ProgressSnapshot& s) {
    if (_closure && !_closure->is_superuser) {
      const auto* role = _roles->FindByName(s.user);
      if (!role || !_closure->MemberOf(role->first)) {
        Emit<kHidden>(s);
        return;
      }
    }
    if (s.query_start_us == 0) {
      Emit<kIdle>(s);
    } else if (static_cast<ProgressCommand>(s.command) ==
               ProgressCommand::None) {
      Emit<kActive>(s);
    } else {
      Emit<kCommand>(s);
    }
  }

 private:
  std::shared_ptr<const auth::RoleClosure> _closure = SessionClosure();
  std::shared_ptr<const auth::RoleGraph> _roles =
    _closure && !_closure->is_superuser ? auth::RolesOf(&Context()) : nullptr;
};

}  // namespace

SystemTable gSdbProgress = SystemTableOf<SdbProgress>();

}  // namespace sdb::pg
