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

#include <absl/strings/str_cat.h>

#include <cassert>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace sdb::pg {

struct OptionInfo {
  enum class Type : uint8_t { String, Boolean, Integer, Character };

  std::string_view name;
  Type type;
  std::string_view description;

  union {
    std::string_view string_val;
    bool bool_val;
    int int_val;
    char char_val;
  };

  consteval OptionInfo(std::string_view name, std::string_view def,
                       std::string_view desc)
    : name{name}, type{Type::String}, description{desc}, string_val{def} {}

  consteval OptionInfo(std::string_view name, bool def, std::string_view desc)
    : name{name}, type{Type::Boolean}, description{desc}, bool_val{def} {}

  consteval OptionInfo(std::string_view name, char def, std::string_view desc)
    : name{name}, type{Type::Character}, description{desc}, char_val{def} {}

  consteval OptionInfo(std::string_view name, int def, std::string_view desc)
    : name{name}, type{Type::Integer}, description{desc}, int_val{def} {}

  template<typename T>
  constexpr T DefaultValue() const {
    if constexpr (std::is_same_v<T, std::string_view>) {
      assert(type == Type::String);
      return string_val;
    } else if constexpr (std::is_same_v<T, std::string>) {
      assert(type == Type::String);
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
    } else {
      static_assert(false, "Unsupported type for DefaultValue");
    }
  }

  template<Type V>
  using CppType = std::conditional_t<
    V == Type::String, std::string,
    std::conditional_t<V == Type::Boolean, bool,
                       std::conditional_t<V == Type::Integer, int, char>>>;

  constexpr std::string_view TypeName() const {
    switch (type) {
      case Type::String:
        return "string";
      case Type::Boolean:
        return "boolean";
      case Type::Integer:
        return "integer";
      case Type::Character:
        return "character";
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
      case Type::Character:
        return absl::StrCat(operation, " ", name,
                            " must be a single one-byte character");
      case Type::String:
        return absl::StrCat(operation, " ", name, " must be a string");
    }
  }
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
