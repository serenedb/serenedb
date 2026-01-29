////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <vpack/builder.h>

#include <atomic>
#include <cstdint>
#include <limits>
#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

#include "basics/common.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/exceptions.h"
#include "basics/sink.h"
#include "basics/string_utils.h"
#include "basics/units_helper.h"

namespace sdb::options {

// stringify a value, base version for any type
template<typename T>
inline std::string StringifyValue(const T& value) {
  return absl::StrCat(value);
}

// stringify a double value, specialized version
template<>
inline std::string StringifyValue<double>(const double& value) {
  char buf[32];
  size_t len = basics::dtoa_vpack(value, buf) - buf;
  return {buf, len};
}

// stringify a boolean value, specialized version
template<>
inline std::string StringifyValue<bool>(const bool& value) {
  return value ? "true" : "false";
}

// stringify a string value, specialized version
template<>
inline std::string StringifyValue<std::string>(const std::string& value) {
  return absl::StrCat("\"", value, "\"");
}

template<typename T>
inline std::string StringifyValues(const T& values) {
  std::string value;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      absl::StrAppend(&value, ", ");
    }
    absl::StrAppend(&value, StringifyValue(values.at(i)));
  }
  return value;
}

// abstract base parameter type struct
struct Parameter {
  Parameter() = default;
  virtual ~Parameter() = default;

  virtual void flushValue() {}

  virtual bool requiresValue() const { return true; }
  virtual std::string name() const = 0;
  virtual std::string valueString() const = 0;
  virtual std::string set(const std::string&) = 0;
  virtual std::string description() const { return std::string(); }

  virtual std::string typeDescription() const {
    return std::string("<") + name() + std::string(">");
  }

  virtual void toVPack(vpack::Builder&, bool detailed) const = 0;
};

// specialized type for boolean values
template<typename ValueType>
struct BooleanParameterBase : public Parameter {
  explicit BooleanParameterBase(ValueType* ptr, bool required = false)
    : ptr(ptr), required(required) {}

  bool requiresValue() const override { return required; }
  std::string name() const override { return "boolean"; }
  std::string valueString() const override { return StringifyValue(*ptr); }

  std::string set(const std::string& value) override {
    if (!required && value.empty()) {
      // the empty value "" is considered "true", e.g. "--force" will mean
      // "--force true"
      *ptr = true;
      return "";
    }
    if (value == "true" || value == "false" || value == "on" ||
        value == "off" || value == "1" || value == "0" || value == "yes" ||
        value == "no") {
      *ptr =
        (value == "true" || value == "on" || value == "1" || value == "yes");
      return "";
    }
    return "invalid value for type " + this->name() +
           ". expecting 'true' or 'false'";
  }

  std::string typeDescription() const override {
    return Parameter::typeDescription();
  }

  void toVPack(vpack::Builder& builder, bool detailed) const override {
    builder.add(*ptr);
    if (detailed) {
      builder.add("required", required);
    }
  }

  ValueType* ptr;
  bool required;
};

// specialized type for normal boolean values
using BooleanParameter = BooleanParameterBase<bool>;

// specialized type for atomic boolean values
using AtomicBooleanParameter = BooleanParameterBase<std::atomic<bool>>;

// specialized type for numeric values
// this templated type needs a concrete number type
template<typename T>
struct NumericParameter : public Parameter {
  using ValueType = T;

  explicit NumericParameter(
    ValueType* ptr, ValueType base = 1,
    ValueType min_value = std::numeric_limits<ValueType>::min(),
    ValueType max_value = std::numeric_limits<ValueType>::max(),
    bool min_inclusive = true, bool max_inclusive = true)
    : ptr(ptr),
      base(base),
      min_value(min_value),
      max_value(max_value),
      min_inclusive(min_inclusive),
      max_inclusive(max_inclusive) {}

  std::string valueString() const override { return StringifyValue(*ptr); }

  std::string set(const std::string& value) override {
    try {
      ValueType v = units_helper::ToNumber<ValueType>(value, base);
      if (((min_inclusive && v >= min_value) ||
           (!min_inclusive && v > min_value)) &&
          ((max_inclusive && v <= max_value) ||
           (!max_inclusive && v < max_value))) {
        *ptr = v;
        return "";
      }
      return "number '" + value + "' is outside of allowed range " +
             (min_inclusive ? "[" : "(") + std::to_string(this->min_value) +
             " - " + std::to_string(this->max_value) +
             (max_inclusive ? "]" : ")") + " for type " + this->name();
    } catch (...) {
      return "invalid numeric value '" + value + "' for type " + this->name();
    }
  }

  void toVPack(vpack::Builder& builder, bool detailed) const override {
    builder.add(*ptr);
    if (detailed) {
      builder.add("base", base);
      builder.add("minValue", min_value);
      builder.add("maxValue", max_value);
      builder.add("minInclusive", min_inclusive);
      builder.add("maxInclusive", max_inclusive);
    }
  }

  std::string name() const override {
    if constexpr (std::is_same_v<ValueType, int16_t>) {
      return "int16";
    } else if constexpr (std::is_same_v<ValueType, uint16_t>) {
      return "uint16";
    } else if constexpr (std::is_same_v<ValueType, int32_t>) {
      return "int32";
    } else if constexpr (std::is_same_v<ValueType, uint32_t>) {
      return "uint32";
    } else if constexpr (std::is_same_v<ValueType, int64_t>) {
      return "int64";
    } else if constexpr (std::is_same_v<ValueType, uint64_t>) {
      return "uint64";
    } else if constexpr (std::is_same_v<ValueType, size_t>) {
      return "size";
    } else if constexpr (std::is_same_v<ValueType, double>) {
      return "double";
    } else {
      static_assert("unsupported ValueType");
    }
  }

  ValueType* ptr;
  ValueType base = 1;
  ValueType min_value = std::numeric_limits<ValueType>::min();
  ValueType max_value = std::numeric_limits<ValueType>::max();
  bool min_inclusive = true;
  bool max_inclusive = true;
};

// concrete int16 number value type
using Int16Parameter = NumericParameter<int16_t>;

// concrete uint16 number value type
using UInt16Parameter = NumericParameter<uint16_t>;

// concrete int32 number value type
using Int32Parameter = NumericParameter<int32_t>;

// concrete uint32 number value type
using UInt32Parameter = NumericParameter<uint32_t>;

// concrete int64 number value type
using Int64Parameter = NumericParameter<int64_t>;

// concrete uint64 number value type
using UInt64Parameter = NumericParameter<uint64_t>;

// concrete size_t number value type
using SizeTParameter = NumericParameter<size_t>;

// concrete double number value type
using DoubleParameter = NumericParameter<double>;

// string value type
struct StringParameter : public Parameter {
  using ValueType = std::string;

  explicit StringParameter(ValueType* ptr) : ptr(ptr) {}

  std::string name() const override { return "string"; }
  std::string valueString() const override { return StringifyValue(*ptr); }

  std::string set(const std::string& value) override {
    *ptr = value;
    return "";
  }

  void toVPack(vpack::Builder& builder, bool /*detailed*/) const override {
    builder.add(*ptr);
  }

  ValueType* ptr;
};

// specialized type for discrete values (defined in the unordered_set)
// this templated type needs a concrete value type
template<typename T>
struct DiscreteValuesParameter : public T {
  DiscreteValuesParameter(
    typename T::ValueType* ptr,
    const containers::FlatHashSet<typename T::ValueType>& allowed)
    : T(ptr), allowed(allowed) {
    if (allowed.find(*ptr) == allowed.end()) {
      // default value is not in list of allowed values
      std::string msg("invalid default value for DiscreteValues parameter: '");
      msg.append(StringifyValue(*ptr));
      msg.append("'. ");
      msg.append(description());
      SDB_THROW(ERROR_INTERNAL, msg);
    }
  }

  std::string set(const std::string& value) override {
    auto it =
      allowed.find(units_helper::FromString<typename T::ValueType>(value));

    if (it == allowed.end()) {
      std::string msg("invalid value '");
      msg.append(value);
      msg.append("'. ");
      msg.append(description());
      return msg;
    }

    return T::set(value);
  }

  std::string description() const override {
    std::vector<std::string> values;
    for (const auto& it : allowed) {
      values.emplace_back(StringifyValue(it));
    }
    absl::c_sort(values);
    return absl::StrCat("Possible values: ", absl::StrJoin(values, ", "));
  }

  containers::FlatHashSet<typename T::ValueType> allowed;
};

// specialized type for vectors of values
// this templated type needs a concrete value type
template<typename T>
struct VectorParameter : public Parameter {
  explicit VectorParameter(std::vector<typename T::ValueType>* ptr)
    : ptr(ptr) {}
  std::string name() const override {
    typename T::ValueType dummy;
    T param(&dummy);
    return std::string(param.name()) + "...";
  }

  std::string valueString() const override { return StringifyValues(*ptr); }

  std::string set(const std::string& value) override {
    typename T::ValueType dummy;
    T param(&dummy);
    std::string result = param.set(value);
    if (result.empty()) {
      ptr->push_back(*(param.ptr));
    }
    return result;
  }

  void flushValue() override { ptr->clear(); }

  void toVPack(vpack::Builder& builder, bool /*detailed*/) const override {
    builder.openArray();
    for (size_t i = 0; i < ptr->size(); ++i) {
      builder.add(ptr->at(i));
    }
    builder.close();
  }

  std::vector<typename T::ValueType>* ptr;
};

// specialized type for a vector of discrete values (defined in the
// unordered_set) this templated type needs a concrete value type
template<typename T>
struct DiscreteValuesVectorParameter : public Parameter {
  explicit DiscreteValuesVectorParameter(
    std::vector<typename T::ValueType>* ptr,
    const containers::FlatHashSet<typename T::ValueType>& allowed)
    : ptr(ptr), allowed(allowed) {
    for (size_t i = 0; i < ptr->size(); ++i) {
      if (allowed.find(ptr->at(i)) == allowed.end()) {
        // default value is not in list of allowed values
        std::string msg(
          "invalid default value for DiscreteValues parameter: '");
        msg.append(StringifyValue(ptr->at(i)));
        msg.append("'. ");
        msg.append(description());
        SDB_THROW(ERROR_INTERNAL, msg);
      }
    }
  }

  std::string name() const override {
    typename T::ValueType dummy;
    T param(&dummy);
    return std::string(param.name()) + "...";
  }

  void flushValue() override { ptr->clear(); }

  std::string valueString() const override {
    std::string value;
    for (size_t i = 0; i < ptr->size(); ++i) {
      if (i > 0) {
        value.append(", ");
      }
      value.append(StringifyValue(ptr->at(i)));
    }
    return value;
  }

  std::string set(const std::string& value) override {
    auto it =
      allowed.find(units_helper::FromString<typename T::ValueType>(value));

    if (it == allowed.end()) {
      std::string msg("invalid value '");
      msg.append(value);
      msg.append("'. ");
      msg.append(description());
      return msg;
    }

    typename T::ValueType dummy;
    T param(&dummy);
    std::string result = param.set(value);

    if (result.empty()) {
      ptr->push_back(*(param.ptr));
    }
    return result;
  }

  void toVPack(vpack::Builder& builder, bool /*detailed*/) const override {
    builder.openArray();
    for (size_t i = 0; i < ptr->size(); ++i) {
      builder.add(ptr->at(i));
    }
    builder.close();
  }

  std::string description() const override {
    std::vector<std::string> values;
    values.reserve(allowed.size());
    for (const auto& it : allowed) {
      values.emplace_back(StringifyValue(it));
    }
    absl::c_sort(values);
    return absl::StrCat("Possible values: ", absl::StrJoin(values, ", "));
  }

  std::vector<typename T::ValueType>* ptr;
  containers::FlatHashSet<typename T::ValueType> allowed;
};

// a type that's useful for obsolete parameters that do nothing
struct ObsoleteParameter : public Parameter {
  explicit ObsoleteParameter(bool requires_value) : required(requires_value) {}
  bool requiresValue() const override { return required; }
  std::string name() const override { return "obsolete"; }
  std::string valueString() const override { return "-"; }
  std::string set(const std::string&) override { return ""; }
  void toVPack(vpack::Builder& builder, bool /*detailed*/) const override {
    builder.add(vpack::Value(vpack::ValueType::Null));
  }

  bool required;
};
}  // namespace sdb::options
