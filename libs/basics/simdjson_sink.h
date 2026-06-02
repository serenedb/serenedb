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

#include <absl/container/inlined_vector.h>
#include <simdjson.h>

#include <cstddef>
#include <cstdint>
#include <magic_enum/magic_enum.hpp>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"

namespace sdb::basics {


class JsonSink {
  using StringBuilder = simdjson::builder::string_builder;

 public:
  explicit JsonSink(StringBuilder& sb) : _sb{&sb} {}

  void OnObjectBegin() { _sb->start_object(); }
  void OnObjectEnd() { _sb->end_object(); }

  void OnListBegin() { _sb->start_array(); }
  void OnListEnd() { _sb->end_array(); }

  void OnPropertyBegin(std::string_view tag) {
    _sb->escape_and_append_with_quotes(tag);
    _sb->append_colon();
  }

  void OnSeparator() { _sb->append_comma(); }

  void WriteValue(bool v) { _sb->append_raw(v ? "true" : "false"); }
  void WriteValue(int64_t v) { _sb->append(v); }
  void WriteValue(uint64_t v) { _sb->append(v); }
  void WriteValue(double v) { _sb->append(v); }
  void WriteValue(std::string_view v) { _sb->escape_and_append_with_quotes(v); }
  void WriteNull() { _sb->append_null(); }

 private:
  StringBuilder* _sb;
};

class JsonSource {
  using Document = simdjson::ondemand::document;
  using Value = simdjson::ondemand::value;
  using Object = simdjson::ondemand::object;
  using JsonType = simdjson::ondemand::json_type;
  using ArrayIterator = simdjson::ondemand::array_iterator;
  using ArrayRange = std::pair<ArrayIterator, ArrayIterator>;

 public:
  explicit JsonSource(Document& doc) : _curr{doc.get_value().value()} {}

  bool ReadBool() { return Read(_curr.get_bool(), JsonType::boolean); }
  int64_t ReadSignedInt64() {
    return Read(_curr.get_int64(), JsonType::number);
  }
  uint64_t ReadUnsignedInt64() {
    return Read(_curr.get_uint64(), JsonType::number);
  }
  double ReadDouble() { return Read(_curr.get_double(), JsonType::number); }
  std::string ReadString() {
    return std::string{Read(_curr.get_string(), JsonType::string)};
  }

  bool OnNullableBegin() { return !_curr.is_null().value(); }

  bool OnListBegin() {
    auto arr = Read(_curr.get_array(), JsonType::array);
    auto first = arr.begin().value();
    auto last = arr.end().value();
    if (first == last) {
      return false;
    }
    _curr = (*first).value();
    _frames.emplace_back(first, last);
    return true;
  }

  void OnScopeEnd() {
    SDB_ASSERT(!_frames.empty());
    _frames.pop_back();
  }

  bool NextListElement() {
    auto& a = _frames.back();
    ++a.first;
    if (a.first == a.second) {
      return false;
    }
    _curr = (*a.first).value();
    return true;
  }

  template<typename F>
  void ForEachObjectField(F&& callback) {
    auto obj = Read(_curr.get_object(), JsonType::object);
    for (auto field : obj) {
      const std::string_view name = field.unescaped_key().value();
      _curr = field.value().value();
      callback(name);
    }
  }

  template<typename F>
  void ForEachListElement(F&& callback) {
    auto arr = Read(_curr.get_array(), JsonType::array);
    for (auto elem : arr) {
      _curr = elem.value();
      callback();
    }
  }

 private:
  template<typename T>
  T Read(simdjson::simdjson_result<T> result, JsonType expected) {
    if (result.error() != simdjson::SUCCESS) [[unlikely]] {
      ThrowTypeMismatch(expected);
    }
    return std::move(result).value_unsafe();
  }

  [[noreturn]] void ThrowTypeMismatch(JsonType expected) {
    JsonType actual;
    const std::string_view found = _curr.type().get(actual) == simdjson::SUCCESS
                                     ? magic_enum::enum_name(actual)
                                     : std::string_view{"invalid JSON"};
    SDB_THROW(ERROR_BAD_PARAMETER, "JSON: expected ",
              magic_enum::enum_name(expected), " but found ", found);
  }

  Value _curr;
  absl::InlinedVector<ArrayRange, 4> _frames;
};

}  // namespace sdb::basics
