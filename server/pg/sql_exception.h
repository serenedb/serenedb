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

#include <source_location>

#include "basics/exceptions.h"
#include "pg/sql_error.h"

namespace sdb {

class SqlException final : public std::exception {
 public:
  SqlException(pg::SqlErrorData&& err, std::source_location location)
    : _location{location}, _data{std::move(err)} {}

  const char* what() const noexcept final { return _data.errmsg.c_str(); }
  const pg::SqlErrorData& error() const noexcept { return _data; }
  auto location() const noexcept { return _location; }

 private:
  std::source_location _location;
  pg::SqlErrorData _data;
};

}  // namespace sdb

#define SQL_DATA_UNWRAP(...) {__VA_ARGS__}

#define THROW_SQL_ERROR_FROM_DATA(sqlData) \
  throw ::sdb::SqlException { (sqlData), std::source_location::current() }
