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
#include "pg/file_options.h"
#include "pg/options_parser.h"

namespace sdb::pg {

class FileOptionsParser : public OptionsParser {
 public:
  FileOptionsParser(std::string_view file_path, Options options,
                    std::span<const OptionGroup> option_groups,
                    OptionsContext context)
    : OptionsParser{std::move(options), option_groups, std::move(context)},
      _file_path{file_path} {}

 protected:
  std::unique_ptr<StorageOptions> ParseStorageOptions() {
    using namespace file_options;
    bool explicit_storage = HasOption(kStorage.base);
    auto storage = EraseOptionOrDefault<kStorage>();
    if (!explicit_storage) {
      storage = TryStorageFromContext();
    }

    if (storage == StorageType::S3) {
      return ParseS3StorageOptions();
    }
    return std::make_unique<LocalStorageOptions>(std::string{_file_path});
  }

  std::unique_ptr<S3StorageOptions> ParseS3StorageOptions() {
    using namespace file_options;

    if (HasOption(kS3AccessKey) != HasOption(kS3SecretKey)) {
      auto location = HasOption(kS3AccessKey) ? OptionLocation(kS3AccessKey)
                                              : OptionLocation(kS3SecretKey);
      THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(location)),
                      ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG(kS3AccessKey.name, " and ", kS3SecretKey.name,
                              " must be specified together"));
    }

    if (HasOption(kS3AccessKey) && HasOption(kS3IamRole)) {
      THROW_SQL_ERROR(
        CURSOR_POS(ErrorPosition(OptionLocation(kS3IamRole))),
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
      std::string{_file_path}, std::string{access_key}, std::string{secret_key},
      std::string{endpoint}, std::string{region}, std::string{iam_role},
      path_style, ssl_enabled, use_creds);
  }

  std::optional<file_options::FormatType> TryFormatFromFile() const {
    using namespace file_options;
    const auto pos = _file_path.rfind('.');
    if (pos == std::string_view::npos) {
      return std::nullopt;
    }
    return magic_enum::enum_cast<FormatType>(_file_path.substr(pos + 1),
                                             magic_enum::case_insensitive);
  }

  static bool IsS3Path(std::string_view path) {
    return path.starts_with("s3://") || path.starts_with("s3a://") ||
           path.starts_with("s3n://") || path.starts_with("oss://") ||
           path.starts_with("cos://") || path.starts_with("cosn://");
  }

  file_options::StorageType TryStorageFromContext() const {
    using namespace file_options;

    if (IsS3Path(_file_path) ||
        absl::c_any_of(kS3Group.FlatOptions(), [&](const OptionInfo& option) {
          return HasOption(option);
        })) {
      return StorageType::S3;
    }

    return StorageType::Local;
  }

  file_options::FormatType ParseFileFormat() {
    using namespace file_options;
    bool explicit_format = HasOption(kFormat);
    auto format = EraseOptionOrDefault<kFormat>();
    if (!explicit_format) {
      if (auto detected = TryFormatFromFile()) {
        format = *detected;
        WriteNotice(absl::StrCat(
          "Format \"", magic_enum::enum_name(format),
          "\" was auto-detected from the file extension. To override, "
          "explicitly specify the format using the WITH (FORMAT ...) clause."));
      }
    }
    return format;
  }

  std::shared_ptr<TextFormatOptions> ParseTextFormatOptions(bool is_csv) {
    return is_csv ? ParseTextFormatOptionsImpl<true>()
                  : ParseTextFormatOptionsImpl<false>();
  }

  template<bool IsCsv>
  std::shared_ptr<TextFormatOptions> ParseTextFormatOptionsImpl() {
    using namespace file_options;

    constexpr auto& kDelim = IsCsv ? kCsvDelimiter : kTextDelimiter;
    uint8_t delim = EraseOptionOrDefault<kDelim>();

    constexpr auto& kEscape = IsCsv ? kCsvEscape : kTextEscape;
    uint8_t escape = EraseOptionOrDefault<kEscape>();

    constexpr auto& kNull = IsCsv ? kCsvNull : kTextNull;
    auto null_string = EraseOptionOrDefault<kNull>();

    // TODO: make variant option info
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

      if (!option->arg) {
        header = true;
      } else {
        auto maybe_header = TryGet<bool>(option->arg);
        if (!maybe_header) {
          THROW_SQL_ERROR(
            CURSOR_POS(ErrorPosition(ExprLocation(option))),
            ERR_CODE(ERRCODE_SYNTAX_ERROR),
            ERR_MSG("header requires a Boolean value or \"match\""));
        }
        header = *maybe_header;
      }
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
    file_options::FormatType format) {
    using namespace file_options;
    switch (format) {
      case FormatType::Text:
        return ParseTextFormatOptions(false);
      case FormatType::Csv:
        return ParseTextFormatOptions(true);
      case FormatType::Parquet:
        return ParseParquetFormatOptions();
      case FormatType::Dwrf:
        return ParseDwrfFormatOptions();
      case FormatType::Orc:
        return ParseOrcFormatOptions();
    }
  }

  std::string_view _file_path;
};

}  // namespace sdb::pg
