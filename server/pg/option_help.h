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
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

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

  std::string_view name;
  Type type;
  std::string_view description;

  union {
    std::string_view string_val;
    bool bool_val;
    int int_val;
    double double_val;
    char char_val;
  };

  std::span<const std::string_view> enum_values;

  consteval OptionInfo(std::string_view name, std::string_view def,
                       std::string_view desc)
    : name{name}, type{Type::String}, description{desc}, string_val{def} {}

  consteval OptionInfo(std::string_view name, bool def, std::string_view desc)
    : name{name}, type{Type::Boolean}, description{desc}, bool_val{def} {}

  consteval OptionInfo(std::string_view name, char def, std::string_view desc)
    : name{name}, type{Type::Character}, description{desc}, char_val{def} {}

  consteval OptionInfo(std::string_view name, int def, std::string_view desc)
    : name{name}, type{Type::Integer}, description{desc}, int_val{def} {}

  consteval OptionInfo(std::string_view name, double def, std::string_view desc)
    : name{name}, type{Type::Double}, description{desc}, double_val{def} {}

  template<typename T>
  constexpr T DefaultValue() const {
    if constexpr (std::is_same_v<T, std::string_view>) {
      assert(type == Type::String || type == Type::Enum);
      return string_val;
    } else if constexpr (std::is_same_v<T, std::string>) {
      assert(type == Type::String || type == Type::Enum);
      return std::string{string_val};
    } else if constexpr (std::is_same_v<T, bool>) {
      assert(type == Type::Boolean);
      return bool_val;
    } else if constexpr (std::is_same_v<T, uint8_t> ||
                         std::is_same_v<T, char>) {
      assert(type == Type::Character);
      return static_cast<T>(char_val);
    } else if constexpr (std::is_same_v<T, int>) {
      assert(type == Type::Integer);
      return int_val;
    } else if constexpr (std::is_same_v<T, double>) {
      assert(type == Type::Double);
      return double_val;
    } else if constexpr (std::is_enum_v<T>) {
      assert(type == Type::Enum);
      auto result =
        magic_enum::enum_cast<T>(string_val, magic_enum::case_insensitive);
      assert(result.has_value());
      return *result;
    } else {
      static_assert(false, "Unsupported type for DefaultValue");
    }
  }

  template<Type V>
  using CppType = std::conditional_t<
    V == Type::String || V == Type::Enum, std::string,
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

  std::vector<std::string_view> AllNames() const {
    std::vector<std::string_view> names;
    CollectOptionNames(names);
    return names;
  }

 private:
  void CollectOptionNames(std::vector<std::string_view>& names) const {
    for (const auto& opt : options) {
      names.push_back(opt.name);
    }
    for (const auto& subgroup : subgroups) {
      subgroup.CollectOptionNames(names);
    }
  }
};

std::vector<std::string_view> AllOptionNames(
  std::span<const OptionGroup> groups);

std::string FormatHelp(std::span<const OptionGroup> groups);

}  // namespace sdb::pg
