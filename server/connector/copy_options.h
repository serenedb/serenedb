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

#pragma once

#include <cstdint>
#include <duckdb/common/enums/file_compression_type.hpp>
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/copy_info.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "formats/text_encoding.h"
#include "pg/copy_in_batches.h"

namespace sdb::connector {

enum class CopyMode : uint8_t {
  Text,
  Csv,
};

struct CopyColumns {
  bool all = false;
  std::vector<std::string> names;
};

struct PgCopyOptions {
  CopyMode mode = CopyMode::Text;
  char delim = '\t';
  char quote = '"';
  char escape = '"';
  std::string null_str = "\\N";
  std::optional<std::string> default_str;
  bool header = false;
  bool header_match = false;
  bool ignore_errors = false;
  uint64_t reject_limit = 0;
  pg::CopyLogVerbosity log_verbosity = pg::CopyLogVerbosity::Default;
  bool freeze = false;
  CopyColumns force_quote;
  CopyColumns force_not_null;
  CopyColumns force_null;
  duckdb::FileCompressionType compression =
    duckdb::FileCompressionType::AUTO_DETECT;
  formats::TextEncoding encoding = formats::TextEncoding::Utf8;
  std::string new_line = "\n";
  std::string prefix;
  std::string suffix;
  std::string date_format;
  std::string timestamp_format;
  std::vector<std::string> foreign;
};

PgCopyOptions ResolveCopyOptions(
  CopyMode mode, bool from,
  const duckdb::identifier_map_t<duckdb::vector<duckdb::Value>>& options);

duckdb::unique_ptr<duckdb::CopyInfo> BindCopyOptions(
  duckdb::ClientContext& context, const duckdb::CopyInfo& info);

void CheckCopyOptions(const PgCopyOptions& options, bool from);

bool HasForeignCopyOption(const duckdb::CopyInfo& info);

bool HasEncodingOption(const duckdb::CopyInfo& info);

pg::CopyLogVerbosity ParseLogVerbosity(std::span<const duckdb::Value> values);


std::vector<bool> CopyColumnMask(const CopyColumns& columns,
                                 std::span<const std::string> names,
                                 std::string_view option);

}  // namespace sdb::connector
