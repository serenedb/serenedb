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

#include "network/http/es/common.h"

#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <duckdb/common/error_data.hpp>
#include <utility>

#include "network/pg/wire_frames.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"

namespace sdb::network::http::es {

void WriteError(HttpResponseWriter& writer, HttpStatus status,
                std::string_view type, std::string_view reason) {
  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"error":{"type":)");
  sb.escape_and_append_with_quotes(type);
  sb.append_raw(R"(,"reason":)");
  sb.escape_and_append_with_quotes(reason);
  sb.append_raw(R"(},"status":)");
  sb.append(static_cast<int64_t>(std::to_underlying(status)));
  sb.append_raw("}");
  WriteJson(writer, status, std::string_view{sb.view().value()});
}

void WriteIndexNotFound(HttpResponseWriter& writer, std::string_view index) {
  WriteError(writer, HttpStatus::NotFound, "index_not_found_exception",
             absl::StrCat("no such index [", index, "]"));
}

void WriteSqlError(HttpResponseWriter& writer, const duckdb::ErrorData& error,
                   std::string_view index) {
  sdb::pg::SqlErrorData data;
  try {
    error.Throw();
  } catch (const SqlException& e) {
    data = e.error();
  } catch (...) {
    data = pg::DuckErrorToSqlData(error);
  }
  switch (data.errcode) {
    case ERRCODE_UNDEFINED_TABLE:
      if (!index.empty()) {
        WriteIndexNotFound(writer, index);
      } else {
        WriteError(writer, HttpStatus::NotFound, "index_not_found_exception",
                   data.errmsg);
      }
      return;
    case ERRCODE_DUPLICATE_TABLE:
      WriteError(writer, HttpStatus::BadRequest,
                 "resource_already_exists_exception", data.errmsg);
      return;
    case ERRCODE_INVALID_NAME:
      WriteError(writer, HttpStatus::BadRequest, "invalid_index_name_exception",
                 data.errmsg);
      return;
    case ERRCODE_INVALID_PARAMETER_VALUE:
      WriteError(writer, HttpStatus::BadRequest, "illegal_argument_exception",
                 data.errmsg);
      return;
    case ERRCODE_INVALID_TEXT_REPRESENTATION:
      WriteError(writer, HttpStatus::BadRequest, "mapper_parsing_exception",
                 data.errmsg);
      return;
    case ERRCODE_UNIQUE_VIOLATION:
      WriteError(writer, HttpStatus::Conflict,
                 "version_conflict_engine_exception", data.errmsg);
      return;
    case ERRCODE_UNDEFINED_COLUMN:
      // Unknown field in a query/sort (BINDER errors land here too).
      WriteError(writer, HttpStatus::BadRequest, "query_shard_exception",
                 data.errmsg);
      return;
    case ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION:
      WriteError(writer, HttpStatus::Forbidden, "security_exception",
                 data.errmsg);
      return;
    default:
      WriteError(writer, HttpStatus::InternalError, "exception", data.errmsg);
      return;
  }
}

}  // namespace sdb::network::http::es
