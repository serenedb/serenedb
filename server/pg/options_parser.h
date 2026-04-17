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

#include <algorithm>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types/value.hpp>
#include <functional>
#include <optional>
#include <type_traits>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "pg/option_help.h"

namespace sdb::pg {

// Value stored per option: a typed duckdb::Value plus optional cursor location.
struct OptionEntry {
  duckdb::Value value;
  int location = -1;  // query cursor position; -1 if unavailable
  bool has_value =
    true;  // false for presence-only flags written without =value
};

using Options = containers::FlatHashMap<std::string, OptionEntry>;

/*
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
inline constexpr OptionInfo kRegisters{
  "registers", false,
  "Show internal column register names in all plan outputs"};
inline constexpr OptionInfo kOneline{
  "oneline", false, "One-line output format for physical plan"};
inline constexpr OptionInfo kCost{"cost", false,
                                  "Show estimated costs in physical plan"};

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
*/

struct OptionsContext {
  std::string_view operation;
  std::function<void(std::string)> notice;
  // explain_options::ExplainOptions* explain = nullptr;
};

class OptionsParser {
 public:
  OptionsParser(const duckdb::named_parameter_map_t& named_parameters,
                const OptionGroup& option_group, OptionsContext context)
    : _operation{context.operation},
      _notice{std::move(context.notice)},
      // _explain{context.explain},
      _option_group{option_group} {
    MakeOptions(named_parameters);
    HandleHelp();
  }

 protected:
  template<const OptionInfo& Info, typename T = OptionInfo::CppType<Info.type>>
  T EraseOptionOrDefault(std::string_view prefix = "") {
    static_assert(Info.type != OptionInfo::Type::Enum,
                  "Use EnumOptionInfo overload for enum options");
    constexpr bool kIsBool = Info.type == OptionInfo::Type::Boolean;
    constexpr bool kIsString = Info.type == OptionInfo::Type::String;
    if (auto entry = EraseOption(Info, !kIsBool, prefix)) {
      if constexpr (kIsBool) {
        if (!entry->has_value) {
          return true;
        }
      }
      auto value = TryExtract<T>(entry->value);
      if (!value) {
        throw duckdb::InvalidInputException(
          Info.ErrorMessage(_operation, entry->value.ToString()));
      }
      if constexpr (!std::holds_alternative<std::monostate>(Info.constraint)) {
        if constexpr (kIsString) {
          // ConstraintFunction stores void(*)(string_view); string converts
          // implicitly.
          std::get<void (*)(std::string_view)>(Info.constraint)(
            std::string_view{*value});
        } else {
          SDB_ASSERT(std::holds_alternative<void (*)(T)>(Info.constraint));
          std::get<void (*)(T)>(Info.constraint)(*value);
        }
      }
      return *value;
    } else if (Info.IsRequired()) {
      throw duckdb::InvalidInputException(
        "required parameter \"%s\" was not found", std::string{Info.name});
    }
    return Info.GetDefaultValue<T>();
  }

  template<const auto& Info>
    requires std::is_enum_v<
      typename std::remove_cvref_t<decltype(Info)>::enum_type>
  auto EraseOptionOrDefault(std::string_view prefix = "") {
    using E = typename std::remove_cvref_t<decltype(Info)>::enum_type;

    auto make_hint = [&] {
      return absl::StrCat(
        "Allowed values: ",
        absl::StrJoin(Info.base.enum_values, ", ",
                      [](std::string* out, std::string_view v) {
                        absl::StrAppend(out, absl::AsciiStrToUpper(v));
                      }));
    };

    if (auto entry = EraseOption(Info.base, true, prefix)) {
      auto raw = TryExtract<std::string>(entry->value);
      if (!raw) {
        throw duckdb::InvalidInputException(
          "%s\n%s", Info.base.ErrorMessage(_operation, entry->value.ToString()),
          make_hint());
      }
      auto result =
        magic_enum::enum_cast<E>(*raw, magic_enum::case_insensitive);
      if (!result) {
        throw duckdb::InvalidInputException(
          "%s\n%s", Info.base.ErrorMessage(_operation, *raw), make_hint());
      }
      return *result;
    }

    if (Info.base.IsRequired()) {
      throw duckdb::InvalidInputException(
        "required parameter \"%s\" was not found", std::string{Info.base.name});
    }

    return Info.base.template GetDefaultValue<E>();
  }

  // Returns the entry by value (moved out of the map) or nullopt if not found.
  // requires_parameter: if true and the option has no =value, throws an error.
  std::optional<OptionEntry> EraseOption(const OptionInfo& info,
                                         bool requires_parameter = true,
                                         std::string_view prefix = "") {
    decltype(_options)::iterator it;
    if (!prefix.empty()) {
      auto full_name = OptionInfo::AdjustPrefix(prefix, info.name);
      it = _options.find(full_name);
    } else {
      it = _options.find(std::string{info.name});
    }
    if (it == _options.end()) {
      return std::nullopt;
    }
    auto entry = std::move(it->second);
    _options.erase(it);
    if (requires_parameter && !entry.has_value) {
      throw duckdb::InvalidInputException("%s requires a parameter",
                                          std::string{info.name});
    }
    return entry;
  }

  bool HasOption(const OptionInfo& info, std::string_view prefix = "") const {
    return HasOption(info.name, prefix);
  }

  bool HasOption(std::string_view name, std::string_view prefix = "") const {
    if (!prefix.empty()) {
      auto full_name = OptionInfo::AdjustPrefix(prefix, name);
      return _options.contains(full_name);
    } else {
      return _options.contains(std::string{name});
    }
  }

  int OptionLocation(const OptionInfo& info) const {
    auto it = _options.find(std::string{info.name});
    if (it == _options.end()) {
      return -1;
    }
    return it->second.location;
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
          if (s == "true" || s == "on" || s == "1") {
            return true;
          }
          if (s == "false" || s == "off" || s == "0") {
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

  void MakeOptions(const duckdb::named_parameter_map_t& params) {
    _options.reserve(params.size());
    // Options explain_opts;
    for (const auto& [key, val] : params) {
      OptionEntry entry{val, -1, true};

      // if (_explain && duckdb::StringUtil::CIEquals(key, "explain")) {
      //   // Explain option: value is the stage name.
      //   auto name = TryExtract<std::string>(val);
      //   if (!name) {
      //     throw duckdb::InvalidInputException(
      //       "invalid value for parameter \"explain\": \"%s\"",
      //       val.ToString());
      //   }
      //   explain_opts.try_emplace(*name, std::move(entry));
      //   continue;
      // }

      auto [_, emplaced] =
        _options.try_emplace(std::string{key}, std::move(entry));
      if (!emplaced) {
        throw duckdb::InvalidInputException("conflicting or redundant options");
      }
    }
    // if (!explain_opts.empty()) {
    //   SDB_ASSERT(_explain);
    //   ParseExplainElems(std::move(explain_opts));
    // }
  }

  void HandleHelp() {
    auto it = _options.find("help");
    if (it == _options.end()) {
      return;
    }
    std::string help = "\n";
    // if (_explain) {
    //   absl::StrAppend(&help, "Explain, use WITH (EXPLAIN 'option_name'):\n");
    //   absl::StrAppend(&help, FormatHelp(explain_options::kExplainGroup));
    // }
    absl::StrAppend(&help, FormatHelp(_option_group));
    throw duckdb::InvalidInputException(help);
  }

  // void ParseExplainElems(Options explain) {
  //   for (const auto& name : explain_options::kExplainGroup.FlatNames()) {
  //     if (auto it = std::ranges::find_if(explain,
  //                                        [&](const auto& entry) {
  //                                          return absl::EqualsIgnoreCase(
  //                                            entry.first, name);
  //                                        });
  //         it != explain.end()) {
  //       explain.erase(it);
  //       explain_options::AddByName(name, *_explain);
  //     }
  //   }
  //   CheckUnrecognizedOptions(explain, explain_options::kExplainGroup);
  // }

  void CheckUnrecognizedOptions() const {
    CheckUnrecognizedOptions(_options, _option_group);
  }

  void CheckUnrecognizedOptions(const Options& options,
                                const OptionGroup& option_group) const {
    auto known_names = option_group.FlatNames();
    known_names.emplace_back("help");
    // if (_explain) {
    //   known_names.emplace_back("explain");
    // }

    for (const auto& [name, entry] : options) {
      if (absl::c_contains(known_names, name)) {
        throw duckdb::InvalidInputException(
          "option \"%s\" is not applicable in this context\nHINT: Use WITH "
          "(HELP) to see available options",
          name);
      }
      auto hint = FindClosestOption(known_names, name);
      auto msg =
        hint.empty()
          ? absl::StrCat("option \"", name, "\" not recognized")
          : absl::StrCat("option \"", name,
                         "\" not recognized, did you mean \"", hint, "\"?");
      throw duckdb::InvalidInputException(
        "%s\nHINT: Use WITH (HELP) to see available options", msg);
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
  std::function<void(std::string)> _notice;
  // explain_options::ExplainOptions* _explain;
  Options _options;
  const OptionGroup& _option_group;
};

}  // namespace sdb::pg
