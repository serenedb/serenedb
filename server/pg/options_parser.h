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

#include <absl/strings/ascii.h>
#include <absl/strings/internal/damerau_levenshtein_distance.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <duckdb/common/named_parameter_map.hpp>
#include <functional>
#include <type_traits>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "pg/errcodes.h"
#include "pg/option_help.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg {

using OptionEntry = std::unique_ptr<duckdb::Value>;

using Options = containers::NodeHashMap<std::string, OptionEntry>;

struct OptionsContext {
  std::string_view operation;
  std::function<void(std::string)> notice;
  // Appended to "unrecognized option" errors. Empty in scalar-function
  // contexts where the WITH-syntax suggestion would be misleading.
  std::string_view help_hint;
};

class OptionsParser {
 public:
  static Options MakeOptions(std::string_view text) {
    Options out;
    for (std::string_view entry :
         absl::StrSplit(text, ',', absl::SkipWhitespace())) {
      auto kv = absl::StrSplit(entry, absl::MaxSplits('=', 1));
      auto it = kv.begin();
      auto key_raw = absl::StripAsciiWhitespace(*it++);
      if (it == kv.end()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("option \"", key_raw, "\" has no value"),
                        ERR_HINT("Use Key=Value syntax."));
      }
      auto value_raw = absl::StripAsciiWhitespace(*it);
      if (value_raw.size() >= 2 &&
          ((value_raw.front() == '"' && value_raw.back() == '"') ||
           (value_raw.front() == '\'' && value_raw.back() == '\''))) {
        value_raw = value_raw.substr(1, value_raw.size() - 2);
      }
      auto [_, inserted] = out.try_emplace(
        absl::AsciiStrToLower(key_raw),
        std::make_unique<duckdb::Value>(std::string{value_raw}));
      if (!inserted) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("conflicting or redundant options"));
      }
    }
    return out;
  }

  OptionsParser(Options options, const OptionGroup& option_group,
                OptionsContext context)
    : _operation{context.operation},
      _help_hint{context.help_hint},
      _notice{std::move(context.notice)},
      _options{std::move(options)},
      _option_group{option_group} {
    HandleHelp();
  }

  OptionsParser(duckdb::named_parameter_map_t named_params,
                const OptionGroup& option_group, OptionsContext context)
    : OptionsParser{ConvertMap(std::move(named_params)), option_group,
                    std::move(context)} {}

 private:
  static Options ConvertMap(duckdb::named_parameter_map_t named_params) {
    Options out;
    out.reserve(named_params.size());
    for (auto&& [name, value] : named_params) {
      auto [_, inserted] =
        out.try_emplace(name.GetIdentifierName(),
                        std::make_unique<duckdb::Value>(std::move(value)));
      if (!inserted) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("conflicting or redundant options"));
      }
    }
    return out;
  }

 protected:
  template<const OptionInfo& Info, typename T = OptionInfo::CppType<Info.type>>
  T EraseOptionOrDefault(std::string_view prefix = "") {
    constexpr bool kIsBool = Info.type == OptionInfo::Type::Boolean;
    constexpr bool kIsString = Info.type == OptionInfo::Type::String;
    if (const auto option = EraseOption(Info, !kIsBool, prefix)) {
      if constexpr (kIsBool) {
        if (!*option) {
          return true;
        }
      }
      auto value = TryExtract<T>(**option);
      if (!value) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG(Info.ErrorMessage(_operation, (**option).ToString())));
      }
      if constexpr (!std::holds_alternative<std::monostate>(Info.constraint)) {
        if constexpr (kIsString) {
          std::get<void (*)(std::string_view, std::string_view)>(
            Info.constraint)(Info.name, std::string_view{*value});
        } else {
          SDB_ASSERT((std::holds_alternative<void (*)(std::string_view, T)>(
            Info.constraint)));
          std::get<void (*)(std::string_view, T)>(Info.constraint)(Info.name,
                                                                   *value);
        }
      }
      return *value;
    } else if (Info.IsRequired()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("required parameter \"", Info.name, "\" was not found"));
    }
    return Info.GetDefaultValue<T>();
  }

  // requires_parameter == presence flag like ... WITH (FLAG)
  std::optional<OptionEntry> EraseOption(const OptionInfo& info,
                                         bool requires_parameter = true,
                                         std::string_view prefix = "") {
    decltype(_options)::iterator it;
    if (!prefix.empty()) {
      auto full_name = OptionInfo::AdjustPrefix(prefix, info.name);
      it = _options.find(full_name);
    } else {
      it = _options.find(info.name);
    }
    if (it == _options.end()) {
      return std::nullopt;
    }
    auto option = std::move(it->second);
    _options.erase(it);
    if (requires_parameter && !option) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG(info.name, " requires a parameter"));
    }
    return option;
  }

  bool HasOption(const OptionInfo& info, std::string_view prefix = "") const {
    return HasOption(info.name, prefix);
  }

  bool HasOption(std::string_view name, std::string_view prefix = "") const {
    if (!prefix.empty()) {
      auto full_name = OptionInfo::AdjustPrefix(prefix, name);
      return _options.contains(full_name);
    } else {
      return _options.contains(name);
    }
  }

  template<typename F>
  void ParseOptions(F&& parse) {
    parse();
    CheckUnrecognizedOptions();
  }

 private:
  // Extracts a typed C++ value from a duckdb::Value.
  template<typename T>
  static std::optional<T> TryExtract(const duckdb::Value& v) {
    try {
      if constexpr (std::is_same_v<T, std::string>) {
        return v.DefaultCastAs(duckdb::LogicalType::VARCHAR)
          .GetValue<std::string>();
      } else if constexpr (std::is_same_v<T, bool>) {
        if (v.type().id() == duckdb::LogicalTypeId::VARCHAR) {
          auto s = duckdb::StringUtil::Lower(v.GetValue<std::string>());
          if (s == "true" || s == "t" || s == "on" || s == "1") {
            return true;
          }
          if (s == "false" || s == "f" || s == "off" || s == "0") {
            return false;
          }
          return std::nullopt;
        }
        return v.DefaultCastAs(duckdb::LogicalType::BOOLEAN).GetValue<bool>();
      } else if constexpr (std::is_same_v<T, int>) {
        return static_cast<int>(
          v.DefaultCastAs(duckdb::LogicalType::BIGINT).GetValue<int64_t>());
      } else if constexpr (std::is_same_v<T, double>) {
        return v.DefaultCastAs(duckdb::LogicalType::DOUBLE).GetValue<double>();
      } else if constexpr (std::is_same_v<T, char>) {
        auto s =
          v.DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>();
        if (s.size() != 1) {
          return std::nullopt;
        }
        return s[0];
      }
    } catch (...) {
      return std::nullopt;
    }
  }

  void MakeOptions(const duckdb::named_parameter_map_t& options) {
    _options.reserve(options.size());
    for (const auto& option : options) {
      std::string_view option_name = option.first.GetIdentifierName();
      auto [_, emplaced] = _options.try_emplace(
        option_name, std::make_unique<duckdb::Value>(option.second));
      if (!emplaced) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("conflicting or redundant options"));
      }
    }
  }

  void HandleHelp() {
    auto it = _options.find("help");
    if (it == _options.end()) {
      return;
    }
    std::string help = "\n";
    absl::StrAppend(&help, FormatHelp(_option_group));
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR), ERR_MSG(help));
  }

  void CheckUnrecognizedOptions() const {
    CheckUnrecognizedOptions(_options, _option_group);
  }

  void CheckUnrecognizedOptions(const Options& options,
                                const OptionGroup& option_group) const {
    auto known_names = option_group.FlatNames();
    known_names.emplace_back("help");

    for (const auto& [name, option] : options) {
      if (absl::c_contains(known_names, name)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG("option \"", name, "\" is not applicable in this context"),
          ERR_HINT(_help_hint));
      }
      auto hint = FindClosestOption(known_names, name);
      auto msg =
        hint.empty()
          ? absl::StrCat(_operation, ": option \"", name, "\" not recognized")
          : absl::StrCat(_operation, ": option \"", name,
                         "\" not recognized, did you mean \"", hint, "\"?");
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR), ERR_MSG(msg),
                      ERR_HINT(_help_hint));
    }
  }

  std::string_view FindClosestOption(
    std::span<const std::string_view> known_names,
    std::string_view name) const {
    const uint8_t max_distance =
      static_cast<uint8_t>(std::min<size_t>(name.size() / 2 + 1, 3));
    std::string_view best;
    uint8_t best_distance = max_distance;

    for (const auto& known : known_names) {
      uint8_t distance =
        absl::strings_internal::CappedDamerauLevenshteinDistance(
          absl::string_view{name.data(), name.size()},
          absl::string_view{known.data(), known.size()}, best_distance);
      if (distance < best_distance) {
        best_distance = distance;
        best = known;
      }
    }
    return best;
  }

 protected:
  void WriteNotice(std::string msg) {
    if (_notice) {
      _notice(std::move(msg));
    }
  }

  std::string _operation;
  std::string _help_hint;
  std::function<void(std::string)> _notice;
  Options _options;
  const OptionGroup& _option_group;
};

}  // namespace sdb::pg
