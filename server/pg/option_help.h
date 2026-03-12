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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <cassert>
#include <magic_enum/magic_enum.hpp>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "basics/errors.h"
#include "basics/result.h"
#include "basics/system-compiler.h"
#include "pg/pg_catalog/pg_type.h"
#include "pg/sql_utils.h"

namespace sdb::pg {

namespace detail {
template<typename E>
  requires std::is_enum_v<E>
inline constexpr auto kEnumNames = magic_enum::enum_names<E>();
}  // namespace detail

struct OptionInfo {
  enum class Type : uint8_t {
    String,
    Boolean,
    Integer,
    Double,
    Character,
    Enum
  };

  template<typename T>
  struct RequiredTag {};

  template<typename T>
  static consteval Type GetType() {
    if constexpr (std::is_same_v<T, std::string_view>) {
      return Type::String;
    } else if constexpr (std::is_same_v<T, int>) {
      return Type::Integer;
    } else if constexpr (std::is_same_v<T, bool>) {
      return Type::Boolean;
    } else if constexpr (std::is_same_v<T, double>) {
      return Type::Double;
    } else if constexpr (std::is_same_v<T, char>) {
      return Type::Character;
    } else if constexpr (std::is_enum_v<T>) {
      return Type::Enum;
    } else {
      static_assert(false);
    }
  }

  using DefaultValueT = std::variant<std::string_view, bool, int, double, char>;

  std::string_view name;
  Type type;
  std::string_view description;

  bool required = false;
  std::optional<DefaultValueT> default_value;

  bool (*constraint)(const DefaultValueT&) = nullptr;

  std::span<const std::string_view> enum_values;

  consteval OptionInfo(std::string_view name, Type type,
                       std::string_view description,
                       bool (*constraint)(const DefaultValueT&) = nullptr)
    : name{name},
      type{type},
      description{description},
      constraint{constraint} {}

  template<typename T>
  consteval OptionInfo(std::string_view name, RequiredTag<T>,
                       std::string_view desc,
                       bool (*constraint)(const DefaultValueT&) = nullptr)
    : name{name},
      type{GetType<T>()},
      description{desc},
      required{true},
      constraint{constraint} {}

  template<typename T>
  consteval OptionInfo(std::string_view name, T default_value,
                       std::string_view desc,
                       bool (*constraint)(const DefaultValueT&) = nullptr)
    : name{name},
      type{GetType<T>()},
      description{desc},
      default_value{default_value},
      constraint{constraint} {}

  template<typename T>
  constexpr std::optional<T> DefaultValue() const {
    return default_value.and_then([&](const auto& def) -> std::optional<T> {
      if constexpr (std::is_enum_v<T>) {
        const auto& value_str = std::get<std::string_view>(def);
        auto value =
          magic_enum::enum_cast<T>(value_str, magic_enum::case_insensitive);
        return {value};
      } else {
        return {std::get<T>(def)};
      }
    });
  }

  // Enum type is not supported
  Result CheckAndApply(const Node* node, auto&& callback) const {
    bool correct_value = true;
    auto process_value = [&]<typename T>() {
      std::optional<T> val;
      if constexpr (std::is_same_v<T, char>) {
        val = TryGet<std::string_view>(node).and_then(
          [](const auto& str) -> std::optional<char> {
            return str.size() == 1 ? std::optional{str[0]} : std::nullopt;
          });
      } else {
        val = TryGet<T>(node);
      }
      if (val && (!constraint || constraint(*val))) {
        callback(*val);
        return true;
      }
      return false;
    };
    switch (type) {
      case OptionInfo::Type::Boolean: {
        correct_value = process_value.template operator()<bool>();
      } break;
      case OptionInfo::Type::Integer: {
        correct_value = process_value.template operator()<int>();
      } break;
      case OptionInfo::Type::Double: {
        correct_value = process_value.template operator()<double>();
      } break;
      case OptionInfo::Type::String: {
        correct_value = process_value.template operator()<std::string_view>();
      } break;
      case OptionInfo::Type::Character: {
        correct_value = process_value.template operator()<char>();
      } break;
      default:
        SDB_UNREACHABLE();
    }
    if (!correct_value) {
      return Result{ERROR_BAD_PARAMETER, "incorrect parameter"};
    }
    return {};
  }

  template<Type V>
  using CppType = std::conditional_t<
    V == Type::String || V == Type::Enum, std::string_view,
    std::conditional_t<
      V == Type::Boolean, bool,
      std::conditional_t<V == Type::Integer, int,
                         std::conditional_t<V == Type::Double, double, char>>>>;

  constexpr std::string_view TypeName() const {
    switch (type) {
      case Type::String:
        return "string";
      case Type::Boolean:
        return "boolean";
      case Type::Integer:
        return "integer";
      case Type::Double:
        return "double";
      case Type::Character:
        return "character";
      case Type::Enum:
        return "enum";
    }
  }

  std::string ErrorMessage(std::string_view operation,
                           std::string_view raw_value) const {
    switch (type) {
      case Type::Boolean:
        return absl::StrCat("invalid value for ", operation, " parameter \"",
                            name, "\": \"", raw_value, "\"");
      case Type::Integer:
        return absl::StrCat("invalid input syntax for type integer: \"",
                            raw_value, "\"");
      case Type::Double:
        return absl::StrCat("invalid input syntax for type double: \"",
                            raw_value, "\"");
      case Type::Character:
        return absl::StrCat(operation, " ", name,
                            " must be a single one-byte character");
      case Type::String:
        return absl::StrCat(operation, " ", name, " must be a string");
      case Type::Enum:
        return absl::StrCat(operation, " ", absl::AsciiStrToUpper(name), " \"",
                            raw_value, "\" not recognized");
    }
  }
};

template<typename E>
  requires std::is_enum_v<E>
struct EnumOptionInfo {
  using enum_type = E;

  OptionInfo base;

  consteval EnumOptionInfo(std::string_view name, E def, std::string_view desc)
    : base{name, magic_enum::enum_name(def), desc} {
    base.type = OptionInfo::Type::Enum;
    base.enum_values = detail::kEnumNames<E>;
  }

  consteval operator OptionInfo() const { return base; }
};

struct OptionGroup {
  std::string_view name;
  std::span<const OptionInfo> options;     // leaf options in this group
  std::span<const OptionGroup> subgroups;  // nested groups

  std::vector<OptionInfo> FlatOptions() const {
    std::vector<OptionInfo> result;
    CollectOptions(result);
    return result;
  }

  std::vector<std::string_view> FlatNames() const {
    return FlatOptions() | std::views::transform(&OptionInfo::name) |
           std::ranges::to<std::vector>();
  }

  void VisitOptions(auto&& visit) const {
    for (const auto& opt : options) {
      visit(opt);
    }
    for (const auto& group : subgroups) {
      group.VisitOptions(visit);
    }
  }

 private:
  void CollectOptions(std::vector<OptionInfo>& result) const {
    result.insert(result.end(), options.begin(), options.end());
    for (const auto& subgroup : subgroups) {
      subgroup.CollectOptions(result);
    }
  }
};

std::vector<std::string_view> AllOptionNames(
  std::span<const OptionGroup> groups);

std::string FormatHelp(std::span<const OptionGroup> groups);

}  // namespace sdb::pg
