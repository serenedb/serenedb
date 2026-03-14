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

#include "pg/option_help.h"

namespace sdb::pg::file_options {

using namespace std::string_view_literals;

enum class StorageType : uint8_t {
  Local = 0,
  S3 = 1,
};

enum class CopyOnError : uint8_t {
  Stop = 0,
  Ignore = 1,
};

enum class CopyLogVerbosity : uint8_t {
  Silent = 0,
  Default = 1,
  Verbose = 2,
};

enum class FormatType : uint8_t {
  Text,
  Csv,
  Parquet,
  Dwrf,
  Orc,
};

// Storage

inline constexpr EnumOptionInfo<StorageType> kStorage{
  "storage", StorageType::Local, "Storage backend"};

inline constexpr OptionInfo kS3AccessKey{
  "s3_access_key", ""sv, "AWS access key ID (used with s3_secret_key)"};
inline constexpr OptionInfo kS3SecretKey{
  "s3_secret_key", ""sv, "AWS secret access key (used with s3_access_key)"};
inline constexpr OptionInfo kS3IamRole{
  "s3_iam_role", ""sv, "IAM role ARN (instead of access/secret keys)"};
inline constexpr OptionInfo kS3UseInstanceCredentials{
  "s3_use_instance_credentials", false, "Use EC2 instance credentials"};

inline constexpr OptionInfo kS3AuthOptions[] = {
  kS3AccessKey, kS3SecretKey, kS3IamRole, kS3UseInstanceCredentials};

inline constexpr OptionInfo kS3Endpoint{"s3_endpoint", ""sv,
                                        "S3-compatible endpoint URL"};
inline constexpr OptionInfo kS3Region{"s3_region", ""sv, "AWS region"};
inline constexpr OptionInfo kS3PathStyleAccess{"s3_path_style_access", true,
                                               "Use path-style S3 URLs"};
inline constexpr OptionInfo kS3SslEnabled{"s3_ssl_enabled", false,
                                          "Enable SSL for S3 connections"};

inline constexpr OptionInfo kS3ConnectionOptions[] = {
  kS3Endpoint, kS3Region, kS3PathStyleAccess, kS3SslEnabled};

inline constexpr OptionInfo kStorageOptions[] = {kStorage};

inline constexpr OptionGroup kCommonStorageGroup{"Common", kStorageOptions, {}};
inline constexpr OptionGroup kS3AuthGroup{"Authentication", kS3AuthOptions, {}};
inline constexpr OptionGroup kS3ConnectionGroup{
  "Connection", kS3ConnectionOptions, {}};
inline constexpr OptionGroup kS3Subgroups[] = {kS3AuthGroup,
                                               kS3ConnectionGroup};
inline constexpr OptionGroup kS3Group{"S3", {}, kS3Subgroups};
inline constexpr OptionGroup kLocalGroup{"Local", {}, {}};
inline constexpr OptionGroup kStorageSubgroups[] = {kCommonStorageGroup,
                                                    kS3Group, kLocalGroup};
inline constexpr OptionGroup kStorageGroup{"Storage", {}, kStorageSubgroups};

inline constexpr EnumOptionInfo<FormatType> kFormat{"format", FormatType::Text,
                                                    "File format"};

inline constexpr OptionInfo kCommonFormatOptions[] = {kFormat};

inline constexpr OptionInfo kTextDelimiter{"delimiter", '\t',
                                           "Column delimiter character"};
inline constexpr OptionInfo kTextEscape{"escape", '\\', "Escape character"};
inline constexpr OptionInfo kTextNull{"null", "\\N"sv,
                                      "String representing NULL"};

inline constexpr OptionInfo kCsvDelimiter{"delimiter", ',',
                                          "Column delimiter character"};
inline constexpr OptionInfo kCsvEscape{"escape", '"', "Escape character"};
inline constexpr OptionInfo kCsvNull{"null", ""sv, "String representing NULL"};

inline constexpr OptionInfo kHeader{"header", false,
                                    "First line is a header row"};

inline constexpr OptionInfo kTextOptions[] = {kTextDelimiter, kTextEscape,
                                              kTextNull, kHeader};
inline constexpr OptionInfo kCsvOptions[] = {kCsvDelimiter, kCsvEscape,
                                             kCsvNull, kHeader};

inline constexpr OptionInfo kProgress{"progress", false,
                                      "Show progress notices during COPY"};
inline constexpr EnumOptionInfo<CopyOnError> kOnError{
  "on_error", CopyOnError::Stop, "Error handling"};

void CheckRejectLimit(int value);

inline constexpr OptionInfo kRejectLimit{
  "reject_limit", 0, "Max rows to skip (requires on_error = ignore)",
  CheckRejectLimit};
inline constexpr EnumOptionInfo<CopyLogVerbosity> kLogVerbosity{
  "log_verbosity", CopyLogVerbosity::Default, "Logging level"};

inline constexpr OptionInfo kCommonCopyFormatOptions[] = {kFormat, kProgress};

inline constexpr OptionInfo kCopyTextOptions[] = {
  kTextDelimiter, kTextEscape,  kTextNull,    kHeader,
  kOnError,       kRejectLimit, kLogVerbosity};
inline constexpr OptionInfo kCopyCsvOptions[] = {
  kCsvDelimiter, kCsvEscape,   kCsvNull,     kHeader,
  kOnError,      kRejectLimit, kLogVerbosity};

inline constexpr OptionGroup kCommonCopyFormatGroup{
  "Common", kCommonCopyFormatOptions, {}};
inline constexpr OptionGroup kCommonFormatGroup{
  "Common", kCommonFormatOptions, {}};
inline constexpr OptionGroup kTextGroup{"Text", kTextOptions, {}};
inline constexpr OptionGroup kCsvGroup{"CSV", kCsvOptions, {}};
inline constexpr OptionGroup kCopyTextGroup{"Text", kCopyTextOptions, {}};
inline constexpr OptionGroup kCopyCsvGroup{"CSV", kCopyCsvOptions, {}};
inline constexpr OptionGroup kParquetGroup{"Parquet", {}, {}};
inline constexpr OptionGroup kDwrfGroup{"DWRF", {}, {}};
inline constexpr OptionGroup kOrcGroup{"ORC", {}, {}};

inline constexpr OptionGroup kFormatSubgroups[] = {
  kCommonFormatGroup, kTextGroup, kCsvGroup,
  kParquetGroup,      kDwrfGroup, kOrcGroup};
inline constexpr OptionGroup kFormatGroup{"Format", {}, kFormatSubgroups};

inline constexpr OptionGroup kCopyFormatSubgroups[] = {
  kCommonCopyFormatGroup, kCopyTextGroup, kCopyCsvGroup,
  kParquetGroup,          kDwrfGroup,     kOrcGroup};
inline constexpr OptionGroup kCopyFormatGroup{
  "Format", {}, kCopyFormatSubgroups};

inline constexpr OptionInfo kDefault{
  "default", ""sv, "Default value for columns (not yet supported)"};
inline constexpr OptionInfo kQuote{
  "quote", '\0', "Quote character for CSV (not yet supported)"};
inline constexpr OptionInfo kForceQuote{
  "force_quote", ""sv,
  "Force quoting for specified columns (not yet supported)"};
inline constexpr OptionInfo kForceNotNull{
  "force_not_null", ""sv,
  "Do not match null string for columns (not yet supported)"};
inline constexpr OptionInfo kForceNull{
  "force_null", ""sv, "Match null string even if quoted (not yet supported)"};
inline constexpr OptionInfo kEncoding{
  "encoding", ""sv, "Character encoding of the file (not yet supported)"};

inline constexpr OptionInfo kUnsupportedTextCsvOptions[] = {
  kDefault, kQuote, kForceQuote, kForceNotNull, kForceNull, kEncoding};

inline constexpr OptionInfo kHelp{"help", false, "Show available options"};

inline constexpr OptionInfo kPath{"path", ""sv,
                                  "Path to external file (required)"};

inline constexpr OptionInfo kCreateExternalOptions[] = {kPath};

inline constexpr OptionGroup kCreateExternalGroup{
  "External", kCreateExternalOptions, {}};

inline constexpr OptionGroup kCopyParserGroups[] = {kStorageGroup,
                                                    kCopyFormatGroup};
inline constexpr OptionGroup kCreateExternalParserGroups[] = {
  kCreateExternalGroup, kStorageGroup, kFormatGroup};

}  // namespace sdb::pg::file_options
