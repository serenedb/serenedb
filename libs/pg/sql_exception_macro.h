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

#include "absl/strings/str_cat.h"
#include "basics/assert.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"

// Postgres style error. The carrier variable has a collision-proof name so
// expressions inside ERR_MSG(...) can safely reference a caller-side `error`.
#define ERR_CODE(code) sdb_sql_error_data_.errcode = (code)
#define CURSOR_POS(pos) sdb_sql_error_data_.cursorpos = (pos)

#define ERR_MSG(...) sdb_sql_error_data_.errmsg = absl::StrCat(__VA_ARGS__)
#define ERR_HINT(...) sdb_sql_error_data_.errhint = absl::StrCat(__VA_ARGS__)
#define ERR_DETAIL(...) \
  sdb_sql_error_data_.errdetail = absl::StrCat(__VA_ARGS__)
#define ERR_CONTEXT(...) sdb_sql_error_data_.context = absl::StrCat(__VA_ARGS__)

#define SQL_ERROR_DATA(...)                    \
  [&] {                                        \
    sdb::pg::SqlErrorData sdb_sql_error_data_; \
    ERR_CODE(ERRCODE_INTERNAL_ERROR);          \
    __VA_ARGS__;                               \
    return sdb_sql_error_data_;                \
  }()

#define THROW_SQL_ERROR(...) \
  THROW_SQL_ERROR_FROM_DATA(SQL_ERROR_DATA(__VA_ARGS__))

#define SDB_ENSURE(expr, ...)                                               \
  if (!(expr)) [[unlikely]] {                                               \
    SDB_ASSERT(false __VA_OPT__(, ) __VA_ARGS__);                           \
    THROW_SQL_ERROR_FROM_DATA([&] {                                         \
      sdb::pg::SqlErrorData sdb_sql_error_data_;                            \
      sdb_sql_error_data_.errcode = ERRCODE_INTERNAL_ERROR;                 \
      sdb_sql_error_data_.errmsg = absl::StrCat(__VA_ARGS__);               \
      if (sdb_sql_error_data_.errmsg.empty()) {                             \
        sdb_sql_error_data_.errmsg = "condition '" #expr "' not satisfied"; \
      }                                                                     \
      return sdb_sql_error_data_;                                           \
    }());                                                                   \
  }
