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

#include "network/pg/wire_frames.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"

namespace sdb::network::http::es {

void WriteError(HttpResponseWriter& writer, int status, std::string_view type,
                std::string_view reason) {
  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"error":{"type":)");
  sb.escape_and_append_with_quotes(type);
  sb.append_raw(R"(,"reason":)");
  sb.escape_and_append_with_quotes(reason);
  sb.append_raw(R"(},"status":)");
  sb.append(static_cast<int64_t>(status));
  sb.append_raw("}");
  WriteJson(writer, status, std::string_view{sb.view().value()});
}

void WriteIndexNotFound(HttpResponseWriter& writer, std::string_view index) {
  WriteError(writer, 404, "index_not_found_exception",
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
        WriteError(writer, 404, "index_not_found_exception", data.errmsg);
      }
      return;
    case ERRCODE_DUPLICATE_TABLE:
      WriteError(writer, 400, "resource_already_exists_exception", data.errmsg);
      return;
    case ERRCODE_INVALID_NAME:
      WriteError(writer, 400, "invalid_index_name_exception", data.errmsg);
      return;
    case ERRCODE_INVALID_PARAMETER_VALUE:
      WriteError(writer, 400, "illegal_argument_exception", data.errmsg);
      return;
    case ERRCODE_INVALID_TEXT_REPRESENTATION:
      WriteError(writer, 400, "mapper_parsing_exception", data.errmsg);
      return;
    case ERRCODE_UNIQUE_VIOLATION:
      WriteError(writer, 409, "version_conflict_engine_exception", data.errmsg);
      return;
    case ERRCODE_UNDEFINED_COLUMN:
      // Unknown field in a query/sort (BINDER errors land here too).
      WriteError(writer, 400, "query_shard_exception", data.errmsg);
      return;
    case ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION:
      WriteError(writer, 403, "security_exception", data.errmsg);
      return;
    default:
      WriteError(writer, 500, "exception", data.errmsg);
      return;
  }
}

std::string SqlLiteral(std::string_view text) {
  std::string out;
  out.reserve(text.size() + 2);
  out.push_back('\'');
  for (const char c : text) {
    if (c == '\'') {
      out.push_back('\'');
    }
    out.push_back(c);
  }
  out.push_back('\'');
  return out;
}

std::string SqlIdentifier(std::string_view name) {
  std::string out;
  out.reserve(name.size() + 2);
  out.push_back('"');
  for (const char c : name) {
    if (c == '"') {
      out.push_back('"');
    }
    out.push_back(c);
  }
  out.push_back('"');
  return out;
}

std::string FlattenBody(const message::SequenceView& body) {
  std::string out;
  for (const auto buffer : body) {
    out.append(reinterpret_cast<const char*>(buffer.data()), buffer.size());
  }
  return out;
}

}  // namespace sdb::network::http::es
