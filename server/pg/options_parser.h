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
#include <absl/strings/str_replace.h>
#include <basics/containers/flat_hash_map.h>

#include <algorithm>
#include <functional>
#include <type_traits>

#include "pg/option_help.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/context.h"

namespace sdb::pg {

using Options = containers::FlatHashMap<std::string_view, const DefElem*>;

namespace explain_options {

using query::ExplainWith;
using ExplainOptions = query::EnumType<ExplainWith>;

// plans
inline constexpr OptionInfo kAllPlans{"all_plans", false,
                                      "Show all plan stages"};
inline constexpr OptionInfo kLogical{"logical", false, "Show logical plan"};
inline constexpr OptionInfo kPhysical{"physical", false, "Show physical plan"};
inline constexpr OptionInfo kExecution{"execution", false,
                                       "Show execution plan"};
inline constexpr OptionInfo kInitialQueryGraph{"initial_query_graph", false,
                                               "Show initial query graph"};
inline constexpr OptionInfo kFinalQueryGraph{"final_query_graph", false,
                                             "Show final query graph"};

inline constexpr OptionInfo kPlanOptions[] = {
  kAllPlans,  kLogical,           kPhysical,
  kExecution, kInitialQueryGraph, kFinalQueryGraph};

// parameters
inline constexpr OptionInfo kAnalyze{"analyze", false,
                                     "Execute the query and show run times"};
inline constexpr OptionInfo kRegisters{"registers", false,
                                       "Show register allocation"};
inline constexpr OptionInfo kOneline{"oneline", false,
                                     "One-line output format"};
inline constexpr OptionInfo kCost{"cost", false, "Show estimated costs"};

inline constexpr OptionInfo kParamOptions[] = {kAnalyze, kRegisters, kOneline,
                                               kCost};

inline constexpr OptionGroup kPlanGroup{"Plans", kPlanOptions, {}};
inline constexpr OptionGroup kParamGroup{"Parameters", kParamOptions, {}};
inline constexpr OptionGroup kExplainSubgroups[] = {kPlanGroup, kParamGroup};
inline constexpr OptionGroup kExplainGroup{"Explain", {}, kExplainSubgroups};

inline void AddByName(std::string_view name, ExplainOptions& result) {
  if (name == kAllPlans) {
    result.Add(ExplainWith::Logical, ExplainWith::InitialQueryGraph,
               ExplainWith::FinalQueryGraph, ExplainWith::Physical,
               ExplainWith::Execution);
    return;
  }
  SDB_ASSERT(absl::c_contains(kExplainGroup.FlatNames(), name));
  // delete _ from snake case
  auto normalized = absl::StrReplaceAll(name, {{"_", ""}});
  auto flag = magic_enum::enum_cast<ExplainWith>(normalized,
                                                 magic_enum::case_insensitive);
  SDB_ASSERT(flag, "couldn't parse from string to enum: ", normalized);
  result.Add(*flag);
}

}  // namespace explain_options

struct OptionsContext {
  std::string_view operation;
  std::string_view query_string;
  std::function<void(std::string)> notice;
  explain_options::ExplainOptions* explain = nullptr;
};

class OptionsParser {
 public:
  OptionsParser(const List* options, const OptionGroup& option_group,
                OptionsContext context)
    : _query_string{context.query_string},
      _operation{context.operation},
      _notice{std::move(context.notice)},
      _explain{context.explain},
      _option_group{option_group} {
    MakeOptions(options);
    HandleHelp();
  }

 protected:
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

  template<typename F>
  void ParseOptions(F&& parse) {
    parse();
    CheckUnrecognizedOptions();
  }

 private:
  void MakeOptions(const List* options) {
    _options.reserve(list_length(options));
    Options explain;
    VisitNodes(options, [&](const DefElem& option) {
      std::string_view option_name = option.defname;
      if (_explain && option_name == "explain") {
        auto name = TryGet<std::string_view>(option.arg);
        if (!name) {
          THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(&option))),
                          ERR_CODE(ERRCODE_SYNTAX_ERROR),
                          ERR_MSG("invalid value for parameter \"explain\": \"",
                                  DeparseValue(option.arg), "\""));
        }
        explain.try_emplace(*name, &option);
        return;
      }
      auto [_, emplaced] = _options.try_emplace(option_name, &option);
      if (!emplaced) {
        THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(&option))),
                        ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("conflicting or redundant options"));
      }
    });

    if (!explain.empty()) {
      SDB_ASSERT(_explain);
      ParseExplainElems(std::move(explain));
    }
  }

  void HandleHelp() {
    auto it = _options.find("help");
    if (it == _options.end()) {
      return;
    }
    std::string help;
    if (_explain) {
      absl::StrAppend(&help, "\nExplain, use WITH (EXPLAIN 'option_name'):\n");
      absl::StrAppend(&help, FormatHelp(explain_options::kExplainGroup));
    }
    absl::StrAppend(&help, FormatHelp(_option_group));
    THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(it->second))),
                    ERR_CODE(ERRCODE_SYNTAX_ERROR), ERR_MSG(help));
  }

  void ParseExplainElems(Options explain) {
    for (const auto& info : explain_options::kExplainGroup.FlatOptions()) {
      if (auto it = explain.find(info.name); it != explain.end()) {
        explain.erase(it);
        explain_options::AddByName(info.name, *_explain);
      }
    }
    CheckUnrecognizedOptions(explain, explain_options::kExplainGroup);
  }

  void CheckUnrecognizedOptions() const {
    CheckUnrecognizedOptions(_options, _option_group);
  }

  void CheckUnrecognizedOptions(const Options& options,
                                const OptionGroup& option_group) const {
    auto known_names = AllOptionNames(option_group);
    known_names.emplace_back("help");
    if (_explain) {
      known_names.emplace_back("explain");
    }

    for (const auto& [name, option] : options) {
      if (absl::c_contains(known_names, name)) {
        THROW_SQL_ERROR(
          CURSOR_POS(ErrorPosition(ExprLocation(option))),
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG("option \"", name, "\" is not applicable in this context"),
          ERR_HINT("Use WITH (HELP) to see available options"));
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

 protected:
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
  explain_options::ExplainOptions* _explain;
  Options _options;
  const OptionGroup& _option_group;
};

}  // namespace sdb::pg
