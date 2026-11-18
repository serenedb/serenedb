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

#include "connector/copy_options.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <fast_float/fast_float.h>

#include <array>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/binder.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string_utils.hpp>
#include <system_error>

namespace sdb::connector {
namespace {

constexpr std::string_view kTextReservedDelims =
  "\\.abcdefghijklmnopqrstuvwxyz0123456789";

[[noreturn]] void ThrowRequiresCsv(std::string_view option) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("COPY ", option, " requires CSV mode"));
}

[[noreturn]] void ThrowNotRecognized(std::string_view option) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                  ERR_MSG("COPY option \"", option, "\" not recognized"));
}

[[noreturn]] void ThrowInvalid(std::string_view message) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE), ERR_MSG(message));
}

std::string OptionString(const duckdb::Value& value) {
  return value.ToString();
}

char OptionChar(std::span<const duckdb::Value> values, std::string_view what) {
  const auto text = values.empty() ? std::string{} : OptionString(values[0]);
  if (text.size() != 1) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("COPY ", what, " must be a single one-byte character"));
  }
  return text.front();
}

uint64_t ParseRejectLimit(std::span<const duckdb::Value> values) {
  if (values.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("reject_limit requires a numeric value"));
  }
  const auto text = OptionString(values[0]);
  int64_t limit = 0;
  const auto* end = text.data() + text.size();
  const auto [ptr, ec] = fast_float::from_chars(text.data(), end, limit);
  if (ec != std::errc{} || ptr != end) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type bigint: \"", text, "\""));
  }
  if (limit <= 0) {
    ThrowInvalid(
      absl::StrCat("REJECT_LIMIT (", limit, ") must be greater than zero"));
  }
  return static_cast<uint64_t>(limit);
}

bool ParseBoolOption(std::span<const duckdb::Value> values,
                     std::string_view option) {
  if (values.empty()) {
    return true;
  }
  const auto text = OptionString(values[0]);
  if (const auto parsed = irs::utils::ParseBool(text)) {
    return *parsed;
  }
  ThrowInvalid(absl::StrCat(option, " requires a Boolean value"));
}

void ParseHeader(PgCopyOptions& out, std::span<const duckdb::Value> values) {
  if (values.empty()) {
    out.header = true;
    return;
  }
  const auto text = OptionString(values[0]);
  if (absl::EqualsIgnoreCase(text, "match")) {
    out.header = true;
    out.header_match = true;
    return;
  }
  if (const auto parsed = irs::utils::ParseBool(text)) {
    out.header = *parsed;
    return;
  }
  ThrowInvalid("header requires a Boolean value or \"match\"");
}

CopyColumns ParseColumns(std::span<const duckdb::Value> values,
                         std::string_view option) {
  if (values.empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG(option, " expects a column list or * as parameter"));
  }
  CopyColumns columns;
  for (const auto& value : values) {
    if (value.type().id() == duckdb::LogicalTypeId::LIST) {
      for (const auto& child : duckdb::ListValue::GetChildren(value)) {
        columns.names.push_back(OptionString(child));
      }
      continue;
    }
    auto name = OptionString(value);
    if (name == "*") {
      columns.all = true;
      continue;
    }
    columns.names.push_back(std::move(name));
  }
  return columns;
}

bool IsPgDefault(const duckdb::Identifier& key,
                 std::span<const duckdb::Value> values) {
  if (values.size() != 1) {
    return false;
  }
  const auto text = OptionString(values[0]);
  if (key == "allow_quoted_nulls") {
    return irs::utils::ParseBool(text) == false;
  }
  if (key == "comment") {
    return text.empty();
  }
  if (key == "skip") {
    return text == "0";
  }
  return false;
}

bool IsDuckWriteOption(const duckdb::Identifier& key) {
  return key == "new_line" || key == "prefix" || key == "suffix" ||
         key == "dateformat" || key == "date_format" ||
         key == "timestampformat" || key == "timestamp_format";
}

std::string OptionText(std::span<const duckdb::Value> values) {
  return values.empty() ? std::string{} : OptionString(values[0]);
}

void ApplyDuckWriteOption(PgCopyOptions& out, const duckdb::Identifier& key,
                          std::span<const duckdb::Value> values) {
  if (key == "new_line") {
    out.new_line =
      absl::StrReplaceAll(OptionText(values), {{"\\r", "\r"}, {"\\n", "\n"}});
  } else if (key == "prefix") {
    out.prefix = OptionText(values);
  } else if (key == "suffix") {
    out.suffix = OptionText(values);
  } else if (key == "dateformat" || key == "date_format") {
    out.date_format = OptionText(values);
  } else {
    out.timestamp_format = OptionText(values);
  }
}

void ApplyOption(PgCopyOptions& out, const duckdb::Identifier& key,
                 std::span<const duckdb::Value> values, bool from) {
  const bool csv = out.mode == CopyMode::Csv;
  if (key == "format") {
    return;
  }
  if (csv && !from && IsDuckWriteOption(key)) {
    ApplyDuckWriteOption(out, key, values);
    return;
  }
  if (csv && !from && (key == "quote" || key == "escape") &&
      OptionText(values).empty()) {
    (key == "quote" ? out.quote : out.escape) = '\0';
    return;
  }
  if (key == "delimiter" || key == "delim" || key == "sep" ||
      key == "separator") {
    out.delim = OptionChar(values, "delimiter");
  } else if (key == "null" || key == "nullstr") {
    if (values.size() != 1) {
      ThrowInvalid("NULL requires a single string value");
    }
    out.null_str = OptionString(values[0]);
  } else if (key == "header") {
    ParseHeader(out, values);
  } else if (key == "default") {
    out.default_str = values.empty() ? std::string{} : OptionString(values[0]);
  } else if (key == "ignore_errors") {
    out.ignore_errors = ParseBoolOption(values, "ON_ERROR");
  } else if (key == "rejects_limit" || key == "reject_limit") {
    out.reject_limit = ParseRejectLimit(values);
  } else if (key == "log_verbosity") {
    out.log_verbosity = ParseLogVerbosity(values);
  } else if (key == "freeze") {
    out.freeze = ParseBoolOption(values, "freeze");
  } else if (key == "compression") {
    out.compression = duckdb::FileCompressionType{
      values.empty() ? std::string{} : OptionString(values[0])};
  } else if (key == "encoding") {
    out.encoding = formats::ParseTextEncoding(OptionText(values));
  } else if (key == "quote") {
    if (!csv) {
      ThrowRequiresCsv("QUOTE");
    }
    out.quote = OptionChar(values, "quote");
  } else if (key == "escape") {
    if (!csv) {
      ThrowRequiresCsv("ESCAPE");
    }
    out.escape = OptionChar(values, "escape");
  } else if (key == "force_quote") {
    if (!csv) {
      ThrowRequiresCsv("FORCE_QUOTE");
    }
    out.force_quote = ParseColumns(values, "FORCE_QUOTE");
  } else if (key == "force_not_null") {
    if (!csv) {
      ThrowRequiresCsv("FORCE_NOT_NULL");
    }
    out.force_not_null = ParseColumns(values, "FORCE_NOT_NULL");
  } else if (key == "force_null") {
    if (!csv) {
      ThrowRequiresCsv("FORCE_NULL");
    }
    out.force_null = ParseColumns(values, "FORCE_NULL");
  } else if (csv && IsPgDefault(key, values)) {
    return;
  } else if (csv) {
    out.foreign.push_back(key.GetIdentifierName());
  } else {
    ThrowNotRecognized(key.GetIdentifierName());
  }
}

bool IsPgCopyOption(const duckdb::Identifier& key) {
  static constexpr std::array<std::string_view, 22> kOptions{
    "format",         "delimiter",     "delim",         "sep",
    "separator",      "null",          "nullstr",       "header",
    "default",        "ignore_errors", "rejects_limit", "reject_limit",
    "compression",    "quote",         "escape",        "force_quote",
    "force_not_null", "force_null",    "on_error",      "log_verbosity",
    "freeze",         "encoding",
  };
  return absl::c_any_of(kOptions,
                        [&](std::string_view option) { return key == option; });
}

PgCopyOptions DefaultOptions(CopyMode mode) {
  PgCopyOptions options;
  options.mode = mode;
  if (mode == CopyMode::Csv) {
    options.delim = ',';
    options.null_str.clear();
  }
  return options;
}

void FinishOptions(PgCopyOptions& options, bool escape_set) {
  if (options.mode == CopyMode::Csv && !escape_set) {
    options.escape = options.quote;
  }
}

}  // namespace

PgCopyOptions ResolveCopyOptions(
  CopyMode mode, bool from,
  const duckdb::identifier_map_t<duckdb::vector<duckdb::Value>>& options) {
  auto result = DefaultOptions(mode);
  bool escape_set = false;
  for (const auto& [key, values] : options) {
    ApplyOption(result, key, values, from);
    escape_set |= key == "escape";
  }
  FinishOptions(result, escape_set);
  return result;
}

duckdb::unique_ptr<duckdb::CopyInfo> BindCopyOptions(
  duckdb::ClientContext& context, const duckdb::CopyInfo& info) {
  auto bound = info.Copy();
  context.RunFunctionInTransaction(
    [&] { duckdb::Binder::CreateBinder(context)->BindCopyOptions(*bound); });
  return bound;
}

void CheckCopyOptions(const PgCopyOptions& options, bool from) {
  const bool csv = options.mode == CopyMode::Csv;
  if (from && (options.delim == '\r' || options.delim == '\n')) {
    ThrowInvalid("COPY delimiter cannot be newline or carriage return");
  }
  if (options.prefix.empty() != options.suffix.empty()) {
    ThrowInvalid(
      "COPY ... (FORMAT CSV) must have both PREFIX and SUFFIX, or none at all");
  }
  if (!options.prefix.empty() && options.header) {
    ThrowInvalid(
      "COPY ... (FORMAT CSV)'s HEADER cannot be combined with PREFIX/SUFFIX");
  }
  if (options.null_str.find_first_of("\r\n") != std::string::npos) {
    ThrowInvalid(
      "COPY null representation cannot use newline or carriage return");
  }
  if (options.default_str &&
      options.default_str->find_first_of("\r\n") != std::string::npos) {
    ThrowInvalid(
      "COPY default representation cannot use newline or carriage return");
  }
  if (!csv && kTextReservedDelims.contains(options.delim)) {
    ThrowInvalid(absl::StrCat("COPY delimiter cannot be \"",
                              std::string_view{&options.delim, 1}, "\""));
  }
  if (options.header_match && !from) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot use \"match\" with HEADER in COPY TO"));
  }
  if (csv && options.quote != '\0' && options.delim == options.quote) {
    ThrowInvalid("COPY delimiter and quote must be different");
  }
  const auto direction = [](bool to) { return to ? "COPY TO" : "COPY FROM"; };
  const auto check_columns = [&](const CopyColumns& columns,
                                 std::string_view option, bool allowed_from) {
    if ((columns.all || !columns.names.empty()) && from != allowed_from) {
      ThrowInvalid(absl::StrCat("COPY ", option, " cannot be used with ",
                                direction(!from)));
    }
  };
  check_columns(options.force_quote, "FORCE_QUOTE", false);
  check_columns(options.force_not_null, "FORCE_NOT_NULL", true);
  check_columns(options.force_null, "FORCE_NULL", true);
  if (options.null_str.contains(options.delim)) {
    ThrowInvalid(
      "COPY delimiter character must not appear in the NULL specification");
  }
  if (csv && options.quote != '\0' &&
      options.null_str.contains(options.quote)) {
    ThrowInvalid(
      "CSV quote character must not appear in the NULL specification");
  }
  if (options.default_str) {
    if (!from) {
      ThrowInvalid("COPY DEFAULT cannot be used with COPY TO");
    }
    if (options.default_str->contains(options.delim)) {
      ThrowInvalid(
        "COPY delimiter character must not appear in the DEFAULT "
        "specification");
    }
    if (csv && options.default_str->contains(options.quote)) {
      ThrowInvalid(
        "CSV quote character must not appear in the DEFAULT specification");
    }
    if (*options.default_str == options.null_str) {
      ThrowInvalid(
        "NULL specification and DEFAULT specification cannot be the same");
    }
  }
  if (!from && options.ignore_errors) {
    ThrowInvalid("COPY ON_ERROR cannot be used with COPY TO");
  }
  if (!from && options.freeze) {
    ThrowInvalid("COPY FREEZE cannot be used with COPY TO");
  }
  if (options.reject_limit != 0 && !options.ignore_errors) {
    ThrowInvalid("COPY REJECT_LIMIT requires ON_ERROR to be set to IGNORE");
  }
  if (!from && !options.foreign.empty()) {
    ThrowNotRecognized(options.foreign.front());
  }
}

pg::CopyLogVerbosity ParseLogVerbosity(std::span<const duckdb::Value> values) {
  const auto text = OptionText(values);
  if (absl::EqualsIgnoreCase(text, "default")) {
    return pg::CopyLogVerbosity::Default;
  }
  if (absl::EqualsIgnoreCase(text, "verbose")) {
    return pg::CopyLogVerbosity::Verbose;
  }
  if (absl::EqualsIgnoreCase(text, "silent")) {
    return pg::CopyLogVerbosity::Silent;
  }
  ThrowInvalid(
    absl::StrCat("COPY LOG_VERBOSITY \"", text, "\" not recognized"));
}

bool HasEncodingOption(const duckdb::CopyInfo& info) {
  const auto encoding = [](const auto& option) {
    return option.first == "encoding";
  };
  return absl::c_any_of(info.parsed_options, encoding) ||
         absl::c_any_of(info.options, encoding);
}

bool HasForeignCopyOption(const duckdb::CopyInfo& info) {
  const auto foreign = [](const auto& option) {
    return !IsPgCopyOption(option.first);
  };
  return absl::c_any_of(info.parsed_options, foreign) ||
         absl::c_any_of(info.options, foreign);
}

std::vector<bool> CopyColumnMask(const CopyColumns& columns,
                                 std::span<const std::string> names,
                                 std::string_view option) {
  if (!columns.all && columns.names.empty()) {
    return {};
  }
  std::vector<bool> mask(names.size(), columns.all);
  for (const auto& name : columns.names) {
    auto it = absl::c_find(names, name);
    if (it == names.end()) {
      it = absl::c_find_if(names, [&](const std::string& column) {
        return absl::EqualsIgnoreCase(column, name);
      });
    }
    if (it == names.end()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_COLUMN_REFERENCE),
        ERR_MSG(option, " column \"", name, "\" not referenced by COPY"));
    }
    mask[static_cast<size_t>(it - names.begin())] = true;
  }
  return mask;
}

}  // namespace sdb::connector
