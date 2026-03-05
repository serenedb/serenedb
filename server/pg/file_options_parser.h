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

#include "catalog/format_options.h"
#include "catalog/storage_options.h"
#include "catalog/types.h"
#include "pg/file_option_groups.h"
#include "pg/options_parser.h"

namespace sdb::pg {

class FileOptionsParser : public OptionsParser {
 public:
  FileOptionsParser(std::string_view operation, std::string_view query_string,
                    std::string_view file_path,
                    std::function<void(std::string)> notice, Options options,
                    std::span<const OptionGroup> option_groups)
    : OptionsParser{operation, query_string, std::move(notice),
                    std::move(options), option_groups},
      _file_path{file_path} {}

 protected:
  std::unique_ptr<StorageOptions> ParseStorageOptions() {
    using namespace file_option_groups;
    std::string_view storage = kStorage.DefaultValue<std::string_view>();
    if (const auto* option = EraseOption(kStorage)) {
      auto maybe_storage = TryGet<std::string_view>(option->arg);
      if (!maybe_storage ||
          (*maybe_storage != "local" && *maybe_storage != "s3")) {
        THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(option))),
                        ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("storage must be \"local\" or \"s3\""));
      }
      storage = *maybe_storage;
    } else if (auto detected = TryStorageFromContext(); !detected.empty()) {
      storage = detected;
    }

    if (storage == "s3") {
      return ParseS3StorageOptions();
    }
    return std::make_unique<LocalStorageOptions>(std::string{_file_path});
  }

  std::unique_ptr<S3StorageOptions> ParseS3StorageOptions() {
    using namespace file_option_groups;

    if (HasOption(kS3AccessKey) != HasOption(kS3SecretKey)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG(kS3AccessKey.name, " and ", kS3SecretKey.name,
                              " must be specified together"));
    }

    if (HasOption(kS3AccessKey) && HasOption(kS3IamRole)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG(kS3AccessKey.name, "/", kS3SecretKey.name, " and ",
                kS3IamRole.name, " cannot be specified together"));
    }

    auto access_key = EraseOptionOrDefault<kS3AccessKey>();
    auto secret_key = EraseOptionOrDefault<kS3SecretKey>();
    auto iam_role = EraseOptionOrDefault<kS3IamRole>();

    auto endpoint = EraseOptionOrDefault<kS3Endpoint>();
    auto region = EraseOptionOrDefault<kS3Region>();
    auto path_style = EraseOptionOrDefault<kS3PathStyleAccess>();
    auto ssl_enabled = EraseOptionOrDefault<kS3SslEnabled>();
    auto use_creds = EraseOptionOrDefault<kS3UseInstanceCredentials>();
    return std::make_unique<S3StorageOptions>(
      std::string{_file_path}, std::move(access_key), std::move(secret_key),
      std::move(endpoint), std::move(region), std::move(iam_role), path_style,
      ssl_enabled, use_creds);
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

  static bool IsS3Path(std::string_view path) {
    return path.starts_with("s3://") || path.starts_with("s3a://") ||
           path.starts_with("s3n://") || path.starts_with("oss://") ||
           path.starts_with("cos://") || path.starts_with("cosn://");
  }

  std::string_view TryStorageFromContext() const {
    using namespace file_option_groups;
    // local is the default so detecting it here would be redundant
    if (IsS3Path(_file_path)) {
      return "s3";
    }
    for (const auto& info : kS3AuthOptions) {
      if (_options.contains(info.name)) {
        return "s3";
      }
    }
    for (const auto& info : kS3ConnectionOptions) {
      if (_options.contains(info.name)) {
        return "s3";
      }
    }
    return {};
  }

  struct ParsedFileFormat {
    FileFormat underlying_format;
    std::string_view format;
    int location;
  };

  ParsedFileFormat ParseFileFormat() {
    using namespace file_option_groups;
    const containers::FlatHashMap<std::string_view, FileFormat>
      format2underlying{{"csv", FileFormat::Text},
                        {"text", FileFormat::Text},
                        {"parquet", FileFormat::Parquet},
                        {"dwrf", FileFormat::Dwrf},
                        {"orc", FileFormat::Orc}};

    int location = -1;
    std::string_view format = kFormat.DefaultValue<std::string_view>();
    if (const auto* option = EraseOption(kFormat)) {
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
    return is_csv ? ParseTextFormatOptionsImpl<true>()
                  : ParseTextFormatOptionsImpl<false>();
  }

  template<bool IsCsv>
  std::shared_ptr<TextFormatOptions> ParseTextFormatOptionsImpl() {
    using namespace file_option_groups;

    constexpr auto& kDelim = IsCsv ? kCsvDelimiter : kTextDelimiter;
    uint8_t delim = EraseOptionOrDefault<kDelim>();

    constexpr auto& kEscape = IsCsv ? kCsvEscape : kTextEscape;
    uint8_t escape = EraseOptionOrDefault<kEscape>();

    constexpr auto& kNull = IsCsv ? kCsvNull : kTextNull;
    auto null_string = EraseOptionOrDefault<kNull>();

    auto header = kHeader.DefaultValue<bool>();
    if (const auto* option = EraseOption(kHeader)) {
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

    return std::make_shared<TextFormatOptions>(
      delim, escape, std::string{null_string}, header);
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

  std::string_view _file_path;
};

}  // namespace sdb::pg
