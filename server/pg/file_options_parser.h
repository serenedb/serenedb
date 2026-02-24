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

#include <absl/strings/str_cat.h>
#include <basics/containers/flat_hash_map.h>

#include <functional>

#include "catalog/types.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "pg/storage_options.h"

namespace sdb::pg {

using Options = containers::FlatHashMap<std::string_view, const DefElem*>;

class FileOptionsParser {
 public:
  FileOptionsParser(std::string_view operation, std::string_view query_string,
                    std::string_view file_path,
                    std::function<void(std::string)> notice = {})
    : _query_string{query_string},
      _file_path{file_path},
      _operation{operation},
      _notice{std::move(notice)} {}

 protected:
  std::unique_ptr<StorageOptions> ParseStorageOptions() {
    // TODO: S3 / hdfs etc.
    return std::make_unique<LocalStorageOptions>(std::string{_file_path});
  }

  const DefElem* EraseOption(std::string_view name) {
    auto it = _options.find(name);
    if (it == _options.end()) {
      return nullptr;
    }
    const auto* option = it->second;
    _options.erase(it);
    SDB_ASSERT(option);
    if (!option->arg) {
      THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(&option))),
                      ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG(name, " requires a parameter"));
    }
    return option;
  }

  void CheckUnrecognizedOptions() const {
    for (const auto& [name, option] : _options) {
      THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(option))),
                      ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("option \"", name, "\" not recognized"));
    }
  }

  [[noreturn]] void UnrecognizedFormat(std::string_view format, int location) {
    THROW_SQL_ERROR(
      CURSOR_POS(ErrorPosition(location)), ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG(_operation, " format \"", format, "\" not recognized"));
  }

  std::string_view TryFormatFromFile() const {
    // text format is default so detecting it here would be redundant
    const auto pos = _file_path.rfind('.');
    if (pos == std::string_view::npos) {
      return {};
    }

    const auto file_format = _file_path.substr(pos + 1);
    if (file_format == "csv" || file_format == "parquet" ||
        file_format == "dwrf" || file_format == "orc") {
      return file_format;
    }

    return {};
  }

  int ErrorPosition(int location) const {
    return ::sdb::pg::ErrorPosition(_query_string, location);
  }

  void WriteNotice(std::string msg) {
    if (_notice) {
      _notice(std::move(msg));
    }
  }

  struct ParsedFileFormat {
    FileFormat underlying_format;
    std::string_view format;
    int location;
  };

  ParsedFileFormat ParseFileFormat() {
    const containers::FlatHashMap<std::string_view, FileFormat>
      format2underlying{{"csv", FileFormat::Text},
                        {"text", FileFormat::Text},
                        {"parquet", FileFormat::Parquet},
                        {"dwrf", FileFormat::Dwrf},
                        {"orc", FileFormat::Orc}};

    int location = -1;
    std::string_view format = "text";
    if (const auto* option = EraseOption("format")) {
      location = ExprLocation(&option);
      auto maybe_format = TryGet<std::string_view>(option->arg);
      if (!maybe_format) {
        UnrecognizedFormat(DeparseValue(option->arg), location);
      }
      format = *maybe_format;
    } else if (auto maybe_format = TryFormatFromFile(); !maybe_format.empty()) {
      format = maybe_format;
      WriteNotice(absl::StrCat(
        "Format \"", format,
        "\" was auto-detected from the file extension. To override, "
        "explicitly specify the format using the WITH (FORMAT ...) clause."));
    }

    auto it = format2underlying.find(format);
    if (it == format2underlying.end()) {
      UnrecognizedFormat(format, location);
    }
    return {it->second, format, location};
  }

  std::string_view _query_string;
  std::string_view _file_path;
  containers::FlatHashMap<std::string_view, const DefElem*> _options;
  std::string _operation;
  std::function<void(std::string)> _notice;
};

}  // namespace sdb::pg
