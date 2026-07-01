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

// https://www.postgresql.org/docs/18/catalog-pg-statistic.html
// NOLINTBEGIN
struct PgStatistic {
  static constexpr uint64_t kId = 150;
  static constexpr std::string_view kName = "pg_statistic";
  static constexpr bool kSuperuserOnly = true;

  Oid starelid;
  int16_t staattnum;
  bool stainherit;
  float stanullfrac;
  int32_t stawidth;
  float stadistinct;

  int16_t stakind1;
  int16_t stakind2;
  int16_t stakind3;
  int16_t stakind4;
  int16_t stakind5;

  Oid staop1;
  Oid staop2;
  Oid staop3;
  Oid staop4;
  Oid staop5;

  Oid stacoll1;
  Oid stacoll2;
  Oid stacoll3;
  Oid stacoll4;
  Oid stacoll5;

  Array<float> stanumbers1;
  Array<float> stanumbers2;
  Array<float> stanumbers3;
  Array<float> stanumbers4;
  Array<float> stanumbers5;

  Anyarray stavalues1;
  Anyarray stavalues2;
  Anyarray stavalues3;
  Anyarray stavalues4;
  Anyarray stavalues5;
};
// NOLINTEND

}  // namespace sdb::pg
