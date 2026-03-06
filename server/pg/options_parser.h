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
#include <basics/containers/flat_hash_map.h>

#include <algorithm>
#include <functional>
#include <type_traits>

#include "pg/option_help.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

namespace sdb::pg {

using Options = containers::FlatHashMap<std::string_view, const DefElem*>;

class OptionsParser {
 public:
  OptionsParser(std::string_view operation, std::string_view query_string,
                std::function<void(std::string)> notice, Options options,
                std::span<const OptionGroup> option_groups)
    : _query_string{query_string},
      _operation{operation},
      _notice{std::move(notice)},
      _options{std::move(options)},
      _option_groups{option_groups} {
    HandleHelp(option_groups);
    CheckUnrecognizedOptions();
  }

 protected:
  static Options MakeOptions(const List* options,
                             std::string_view query_string) {
    Options result;
    result.reserve(list_length(options));
    VisitNodes(options, [&](const DefElem& option) {
      auto [_, emplaced] =
        result.try_emplace(std::string_view{option.defname}, &option);
      if (!emplaced) {
        THROW_SQL_ERROR(CURSOR_POS(::sdb::pg::ErrorPosition(
                          query_string, ExprLocation(&option))),
                        ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("conflicting or redundant options"));
      }
    });
    return result;
  }

  void HandleHelp(std::span<const OptionGroup> groups) {
    auto it = _options.find("help");
    if (it == _options.end()) {
      return;
    }
    auto help = absl::StrCat("\n", FormatHelp(groups));
    THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(it->second))),
                    ERR_CODE(ERRCODE_SYNTAX_ERROR), ERR_MSG(help));
  }

  template<const OptionInfo& Info, typename T = OptionInfo::CppType<Info.type>>
  T EraseOptionOrDefault() {
    static_assert(Info.type != OptionInfo::Type::Enum,
                  "Use EnumOptionInfo overload for enum options");
    constexpr bool kIsBool = Info.type == OptionInfo::Type::Boolean;
    if (const auto* option = EraseOption(Info, !kIsBool)) {
      if constexpr (kIsBool) {
        if (!option->arg) {
          return true;
        }
      }
      auto value = TryGet<T>(option->arg);
      if (!value) {
        THROW_SQL_ERROR(
          CURSOR_POS(ErrorPosition(ExprLocation(option))),
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG(Info.ErrorMessage(_operation, DeparseValue(option->arg))));
      }
      return *value;
    }

    return Info.DefaultValue<T>();
  }

  template<const auto& Info>
    requires std::is_enum_v<
      typename std::remove_cvref_t<decltype(Info)>::enum_type>
  auto EraseOptionOrDefault() {
    using E = typename std::remove_cvref_t<decltype(Info)>::enum_type;

    auto make_hint = [&] {
      return absl::StrCat(
        "Allowed values: ",
        absl::StrJoin(Info.base.enum_values, ", ",
                      [](std::string* out, std::string_view v) {
                        absl::StrAppend(out, absl::AsciiStrToUpper(v));
                      }));
    };

    if (const auto* option = EraseOption(Info.base)) {
      auto raw = TryGet<std::string_view>(option->arg);
      if (!raw) {
        THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(option))),
                        ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG(Info.base.ErrorMessage(
                          _operation, DeparseValue(option->arg))),
                        ERR_HINT(make_hint()));
      }
      auto result =
        magic_enum::enum_cast<E>(*raw, magic_enum::case_insensitive);
      if (!result) {
        THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(option))),
                        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG(Info.base.ErrorMessage(_operation, *raw)),
                        ERR_HINT(make_hint()));
      }
      return *result;
    }

    return Info.base.template DefaultValue<E>();
  }

  // requires_parameter == presence flag like ... WITH (FLAG)
  const DefElem* EraseOption(const OptionInfo& info,
                             bool requires_parameter = true) {
    auto it = _options.find(info.name);
    if (it == _options.end()) {
      return nullptr;
    }
    const auto* option = it->second;
    _options.erase(it);
    SDB_ASSERT(option);
    if (requires_parameter && !option->arg) {
      THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(option))),
                      ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG(info.name, " requires a parameter"));
    }
    return option;
  }

  bool HasOption(const OptionInfo& info) const {
    return _options.contains(info.name);
  }

  int OptionLocation(const OptionInfo& info) const {
    auto it = _options.find(info.name);
    if (it == _options.end()) {
      return -1;
    }

    SDB_ASSERT(it->second);
    return it->second->location;
  }

  void CheckUnrecognizedOptions() const {
    auto known_names = AllOptionNames(_option_groups);
    known_names.emplace_back("help");

    for (const auto& [name, option] : _options) {
      if (absl::c_contains(known_names, name)) {
        continue;
      }
      auto hint = FindClosestOption(known_names, name);
      auto msg =
        hint.empty()
          ? absl::StrCat("option \"", name, "\" not recognized")
          : absl::StrCat("option \"", name,
                         "\" not recognized, did you mean \"", hint, "\"?");
      THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(option))),
                      ERR_CODE(ERRCODE_SYNTAX_ERROR), ERR_MSG(msg),
                      ERR_HINT("Use WITH (HELP) to see available options"));
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

  int ErrorPosition(int location) const {
    return ::sdb::pg::ErrorPosition(_query_string, location);
  }

  void WriteNotice(std::string msg) {
    if (_notice) {
      _notice(std::move(msg));
    }
  }

  std::string_view _query_string;
  std::string _operation;
  std::function<void(std::string)> _notice;
  Options _options;
  std::span<const OptionGroup> _option_groups;
};

}  // namespace sdb::pg
