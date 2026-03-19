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
#include <variant>
#include <vector>

#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/result.h"
#include "basics/system-compiler.h"
#include "folly/Function.h"
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

  template<typename... Args>
  static std::string AdjustPrefix(std::string_view prefix, Args&&... args) {
    return absl::StrCat(prefix, prefix.empty() ? "" : "_",
                        std::forward<Args>(args)...);
  }

  using DefaultValue =
    std::variant<std::monostate, std::string_view, bool, int, double, char>;
  using ConstraintFunction =
    std::variant<std::monostate, void (*)(std::string_view), void (*)(bool),
                 void (*)(int), void (*)(double), void (*)(char)>;

  bool operator==(std::string_view option_name) const {
    return name == option_name;
  }

  std::string_view name;
  Type type;
  std::string_view description;

  DefaultValue default_value = std::monostate{};

  ConstraintFunction constraint = std::monostate{};

  std::span<const std::string_view> enum_values;

  template<typename T>
  consteval OptionInfo(std::string_view name, RequiredTag<T>,
                       std::string_view desc,
                       void (*constraint_fn)(T) = nullptr)
    : name{name}, type{GetType<T>()}, description{desc} {
    if (constraint_fn) {
      constraint = constraint_fn;
    }
  }

  template<typename T>
  consteval OptionInfo(std::string_view name, T default_value,
                       std::string_view desc,
                       void (*constraint_fn)(T) = nullptr)
    : name{name},
      type{GetType<T>()},
      description{desc},
      default_value{default_value} {
    if (constraint_fn) {
      constraint = constraint_fn;
    }
  }

  bool IsRequired() const {
    return std::holds_alternative<std::monostate>(default_value);
  }

  template<typename T>
  constexpr T GetDefaultValue() const {
    SDB_ASSERT(!IsRequired());
    if constexpr (std::is_enum_v<T>) {
      SDB_ASSERT(std::holds_alternative<std::string_view>(default_value));
      const auto& value_str = std::get<std::string_view>(default_value);
      auto value =
        magic_enum::enum_cast<T>(value_str, magic_enum::case_insensitive);
      SDB_ASSERT(value);
      return *value;
    } else {
      SDB_ASSERT(std::holds_alternative<T>(default_value));
      return std::get<T>(default_value);
    }
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

std::string FormatHelp(const OptionGroup& group);

}  // namespace sdb::pg
