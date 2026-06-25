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

#include "pg/system_table.h"

namespace sdb::pg {

// https://www.postgresql.org/docs/18/catalog-pg-trigger.html
// NOLINTBEGIN
struct PgTrigger {
  static constexpr uint64_t kId = 157;
  static constexpr std::string_view kName = "pg_trigger";

  enum class Tgenabled : char {
    Origin = 'O',
    Disabled = 'D',
    Replica = 'R',
    Always = 'A',
  };

  Oid oid;
  Oid tgrelid;
  Oid tgparentid;
  Name tgname;
  Oid tgfoid;
  int16_t tgtype;
  Tgenabled tgenabled;
  bool tgisinternal;
  Oid tgconstrrelid;
  Oid tgconstrindid;
  Oid tgconstraint;
  bool tgdeferrable;
  bool tginitdeferred;
  int16_t tgnargs;
  Array<int16_t> tgattr;
  Bytea tgargs;
  PgNodeTree tgqual;
  Name tgoldtable;
  Name tgnewtable;
};
// NOLINTEND

}  // namespace sdb::pg
