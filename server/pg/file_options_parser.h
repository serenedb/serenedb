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
#include <type_traits>

#include "catalog/types.h"
#include "pg/format_options.h"
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
                    std::function<void(std::string)> notice)
    : _query_string{query_string},
      _file_path{file_path},
      _operation{operation},
      _notice{std::move(notice)} {}

 protected:
  static bool IsS3Path(std::string_view path) {
    return path.starts_with("s3://") || path.starts_with("s3a://") ||
           path.starts_with("s3n://") || path.starts_with("oss://") ||
           path.starts_with("cos://") || path.starts_with("cosn://");
  }

  std::unique_ptr<StorageOptions> ParseStorageOptions() {
    if (IsS3Path(_file_path)) {
      return ParseS3StorageOptions();
    }
    return std::make_unique<LocalStorageOptions>(std::string{_file_path});
  }

  std::unique_ptr<S3StorageOptions> ParseS3StorageOptions() {
    auto access_key = EraseOptionOrValue<std::string>("s3_access_key");
    auto secret_key = EraseOptionOrValue<std::string>("s3_secret_key");
    auto endpoint = EraseOptionOrValue<std::string>("s3_endpoint");
    auto region = EraseOptionOrValue<std::string>("s3_region");
    auto iam_role = EraseOptionOrValue<std::string>("s3_iam_role");
    auto path_style_access = EraseOptionOrValue<bool>("s3_path_style_access");
    auto ssl_enabled = EraseOptionOrValue<bool>("s3_ssl_enabled", true);
    auto use_creds = EraseOptionOrValue<bool>("s3_use_instance_credentials");
    return std::make_unique<S3StorageOptions>(
      std::string{_file_path}, std::move(access_key), std::move(secret_key),
      std::move(endpoint), std::move(region), std::move(iam_role),
      path_style_access, ssl_enabled, use_creds);
  }

  template<typename T>
  static constexpr std::string_view OptionTypeName() {
    if constexpr (std::is_same_v<T, std::string> ||
                  std::is_same_v<T, std::string_view>) {
      return "a string";
    } else if constexpr (std::is_same_v<T, bool>) {
      return "a boolean";
    } else if constexpr (std::is_same_v<T, int>) {
      return "an integer";
    } else if constexpr (std::is_same_v<T, double>) {
      return "a number";
    } else if constexpr (std::is_same_v<T, char>) {
      return "a character";
    }
  }

  template<typename T>
  T EraseOptionOrValue(std::string_view name, T value = {}) {
    if (const auto* option = EraseOption(name)) {
      auto value = TryGet<T>(option->arg);
      if (!value) {
        THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(option))),
                        ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG(name, " must be ", OptionTypeName<T>()));
      }
      return *value;
    }
    return value;
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

  std::shared_ptr<TextFormatOptions> ParseTextFormatOptions(bool is_csv) {
    uint8_t delim = is_csv ? ',' : '\t';
    if (const auto* option = EraseOption("delimiter")) {
      auto maybe_delim = TryGet<char>(option->arg);
      if (!maybe_delim) {
        THROW_SQL_ERROR(
          CURSOR_POS(ErrorPosition(ExprLocation(option))),
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG(_operation,
                  " delimiter must be a single one-byte character"));
      }
      delim = *maybe_delim;
    }

    uint8_t escape = is_csv ? '"' : '\\';
    if (const auto* option = EraseOption("escape")) {
      auto maybe_escape = TryGet<char>(option->arg);
      if (!maybe_escape) {
        THROW_SQL_ERROR(
          CURSOR_POS(ErrorPosition(ExprLocation(option))),
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG(_operation, " escape must be a single one-byte character"));
      }
      escape = *maybe_escape;
    }

    std::string null_string = is_csv ? "" : "\\N";
    if (const auto* option = EraseOption("null")) {
      auto maybe_null = TryGet<std::string_view>(option->arg);
      if (!maybe_null) {
        THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(option))),
                        ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG(_operation, " null must be a string"));
      }
      null_string = std::string{*maybe_null};
    }

    bool header = false;
    if (const auto* option = EraseOption("header")) {
      if (auto maybe_match = TryGet<std::string_view>(option->arg)) {
        if (*maybe_match == "match") {
          THROW_SQL_ERROR(
            CURSOR_POS(ErrorPosition(ExprLocation(option))),
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("match option for header is not supported yet"));
        }
      }
      auto maybe_header = TryGet<bool>(option->arg);
      if (!maybe_header) {
        THROW_SQL_ERROR(
          CURSOR_POS(ErrorPosition(ExprLocation(option))),
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG("header requires a Boolean value or \"match\""));
      }
      header = *maybe_header;
    }

    return std::make_shared<TextFormatOptions>(delim, escape,
                                               std::move(null_string), header);
  }

  std::shared_ptr<ParquetFormatOptions> ParseParquetFormatOptions() {
    return std::make_shared<ParquetFormatOptions>();
  }

  std::shared_ptr<DwrfFormatOptions> ParseDwrfFormatOptions() {
    return std::make_shared<DwrfFormatOptions>();
  }

  std::shared_ptr<OrcFormatOptions> ParseOrcFormatOptions() {
    return std::make_shared<OrcFormatOptions>();
  }

  std::shared_ptr<FormatOptions> ParseFormatOptions(
    std::string_view format_name, FileFormat format) {
    switch (format) {
      case FileFormat::Text:
        return ParseTextFormatOptions(format_name == "csv");
      case FileFormat::Parquet:
        return ParseParquetFormatOptions();
      case FileFormat::Dwrf:
        return ParseDwrfFormatOptions();
      case FileFormat::Orc:
        return ParseOrcFormatOptions();
      case FileFormat::None:
        SDB_UNREACHABLE();
    }
  }

  std::string_view _query_string;
  std::string_view _file_path;
  containers::FlatHashMap<std::string_view, const DefElem*> _options;
  std::string _operation;
  std::function<void(std::string)> _notice;
};

}  // namespace sdb::pg
