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
#include "pg/sql_exception.h"

// Postgres style error
#define ERR_CODE(code) error.errcode = (code)
#define CURSOR_POS(pos) error.cursorpos = (pos)

#define ERR_MSG(...) error.errmsg = absl::StrCat(__VA_ARGS__)
#define ERR_HINT(...) error.errhint = absl::StrCat(__VA_ARGS__)
#define ERR_DETAIL(...) error.errdetail = absl::StrCat(__VA_ARGS__)
#define ERR_CONTEXT(...) error.context = absl::StrCat(__VA_ARGS__)

#define SQL_ERROR_DATA(...)           \
  [&] {                               \
    sdb::pg::SqlErrorData error;      \
    ERR_CODE(ERRCODE_INTERNAL_ERROR); \
    __VA_ARGS__;                      \
    return error;                     \
  }()

#define THROW_SQL_ERROR(...) \
  THROW_SQL_ERROR_FROM_DATA(SQL_ERROR_DATA(__VA_ARGS__))
