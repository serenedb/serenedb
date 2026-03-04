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

namespace sdb::pg::file_option_groups {

// --- Storage: S3 ---

inline constexpr OptionInfo kS3AccessKey{
  "s3_access_key", "string", "AWS access key ID", ""};
inline constexpr OptionInfo kS3SecretKey{
  "s3_secret_key", "string", "AWS secret access key", ""};
inline constexpr OptionInfo kS3Endpoint{
  "s3_endpoint", "string", "S3-compatible endpoint URL", ""};
inline constexpr OptionInfo kS3Region{
  "s3_region", "string", "AWS region", ""};
inline constexpr OptionInfo kS3IamRole{
  "s3_iam_role", "string", "IAM role ARN for authentication", ""};
inline constexpr OptionInfo kS3PathStyleAccess{
  "s3_path_style_access", "boolean", "Use path-style S3 URLs", "true"};
inline constexpr OptionInfo kS3SslEnabled{
  "s3_ssl_enabled", "boolean", "Enable SSL for S3 connections", "false"};
inline constexpr OptionInfo kS3UseInstanceCredentials{
  "s3_use_instance_credentials", "boolean", "Use EC2 instance credentials",
  "false"};

inline constexpr OptionInfo kS3Options[] = {
  kS3AccessKey,     kS3SecretKey,           kS3Endpoint,
  kS3Region,        kS3IamRole,             kS3PathStyleAccess,
  kS3SslEnabled,    kS3UseInstanceCredentials,
};

inline constexpr OptionGroup kS3Group{"S3", kS3Options, {}};
inline constexpr OptionGroup kLocalGroup{"Local", {}, {}};
inline constexpr OptionGroup kStorageSubgroups[] = {kS3Group, kLocalGroup};
inline constexpr OptionGroup kStorageGroup{"Storage", {}, kStorageSubgroups};

// --- Format: common ---

inline constexpr OptionInfo kFormat{
  "format", "string", "File format: text, csv, parquet, dwrf, orc", "text"};

inline constexpr OptionInfo kCommonFormatOptions[] = {kFormat};

// --- Format: Text/CSV ---

inline constexpr OptionInfo kDelimiter{
  "delimiter", "character", "Column delimiter character",
  "\\t (text), , (csv)"};
inline constexpr OptionInfo kEscape{
  "escape", "character", "Escape character", "\\\\ (text), \" (csv)"};
inline constexpr OptionInfo kNull{
  "null", "string", "String representing NULL", "\\\\N (text), empty (csv)"};
inline constexpr OptionInfo kHeader{
  "header", "boolean", "First line is a header row", "false"};

inline constexpr OptionInfo kTextCsvOptions[] = {
  kDelimiter, kEscape, kNull, kHeader};

// --- COPY-specific (Text/CSV only) ---

inline constexpr OptionInfo kProgress{
  "progress", "boolean", "Show progress notices during COPY", "false"};
inline constexpr OptionInfo kOnError{
  "on_error", "string", "Error handling: stop, ignore", "stop"};
inline constexpr OptionInfo kRejectLimit{
  "reject_limit", "integer",
  "Max rows to skip (requires on_error = ignore)", "0"};
inline constexpr OptionInfo kLogVerbosity{
  "log_verbosity", "string", "Logging level: default, verbose, silent",
  "default"};

inline constexpr OptionInfo kCopyTextCsvOptions[] = {
  kDelimiter, kEscape, kNull, kHeader,
  kProgress, kOnError, kRejectLimit, kLogVerbosity};

inline constexpr OptionGroup kCommonFormatGroup{"Common", kCommonFormatOptions,
                                                {}};
inline constexpr OptionGroup kTextCsvGroup{"Text/CSV", kTextCsvOptions, {}};
inline constexpr OptionGroup kCopyTextCsvGroup{"Text/CSV", kCopyTextCsvOptions,
                                               {}};
inline constexpr OptionGroup kParquetGroup{"Parquet", {}, {}};
inline constexpr OptionGroup kDwrfGroup{"DWRF", {}, {}};
inline constexpr OptionGroup kOrcGroup{"ORC", {}, {}};

inline constexpr OptionGroup kFormatSubgroups[] = {
  kCommonFormatGroup, kTextCsvGroup, kParquetGroup, kDwrfGroup, kOrcGroup};
inline constexpr OptionGroup kFormatGroup{"Format", {}, kFormatSubgroups};

inline constexpr OptionGroup kCopyFormatSubgroups[] = {
  kCommonFormatGroup, kCopyTextCsvGroup, kParquetGroup, kDwrfGroup, kOrcGroup};
inline constexpr OptionGroup kCopyFormatGroup{"Format", {},
                                              kCopyFormatSubgroups};

// --- Unsupported Text/CSV options (recognized but not yet implemented) ---

inline constexpr OptionInfo kDefault{
  "default", "string", "Default value for columns (not yet supported)", ""};
inline constexpr OptionInfo kQuote{
  "quote", "character", "Quote character for CSV (not yet supported)", ""};
inline constexpr OptionInfo kForceQuote{
  "force_quote", "string",
  "Force quoting for specified columns (not yet supported)", ""};
inline constexpr OptionInfo kForceNotNull{
  "force_not_null", "string",
  "Do not match null string for columns (not yet supported)", ""};
inline constexpr OptionInfo kForceNull{
  "force_null", "string",
  "Match null string even if quoted (not yet supported)", ""};
inline constexpr OptionInfo kEncoding{
  "encoding", "string",
  "Character encoding of the file (not yet supported)", ""};

inline constexpr OptionInfo kUnsupportedTextCsvOptions[] = {
  kDefault, kQuote, kForceQuote, kForceNotNull, kForceNull, kEncoding};

// --- CREATE TABLE USING EXTERNAL specific ---

inline constexpr OptionInfo kPath{
  "path", "string", "Path to external file (required)", ""};

inline constexpr OptionInfo kCreateExternalOptions[] = {kPath};

inline constexpr OptionGroup kCreateExternalGroup{
  "External", kCreateExternalOptions, {}};

// --- Per-parser group compositions ---

inline constexpr OptionGroup kCopyParserGroups[] = {kStorageGroup,
                                                    kCopyFormatGroup};
inline constexpr OptionGroup kCreateExternalParserGroups[] = {
  kCreateExternalGroup, kStorageGroup, kFormatGroup};

}  // namespace sdb::pg::file_option_groups
