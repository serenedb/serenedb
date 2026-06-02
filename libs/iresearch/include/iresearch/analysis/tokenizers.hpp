////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "analyzer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/numeric_utils.hpp"
#include "token_attributes.hpp"

namespace irs {

// Convenient helper implementation providing access to "increment"
// and "term_attributes" attributes
class BasicTokenizer : public analysis::Analyzer {
 public:
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  bool reset(std::string_view) final { return false; }

 protected:
  std::tuple<TermAttr, IncAttr> _attrs;
};

// analyzer implementation for boolean field, a single bool term.
class BooleanTokenizer final : public BasicTokenizer,
                               private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "boolean_token_stream";
  }

  static constexpr std::string_view value_true() noexcept {
    return {"\xFF", 1};
  }

  static constexpr std::string_view value_false() noexcept {
    return {"\x00", 1};
  }

  static constexpr std::string_view value(bool val) noexcept {
    return val ? value_true() : value_false();
  }

  bool next() noexcept final;

  void reset(bool value) noexcept {
    _value = value;
    _valid = true;
  }

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<BooleanTokenizer>::id();
  }

 private:
  using BasicTokenizer::reset;

  bool _valid = false;
  bool _value = false;
};

// Basic implementation of token_stream for simple string field.
// it does not tokenize or analyze field, just set attributes based
// on initial string length
class StringTokenizer : public analysis::TypedAnalyzer<StringTokenizer>,
                        private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept { return "keyword"; }

  struct Options {
    using Owner = StringTokenizer;
  };
  static ptr Make(Options /*opts*/) {
    return std::make_unique<StringTokenizer>();
  }

  bool next() noexcept final;

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  bool reset(std::string_view value) noexcept final {
    _value = ViewCast<byte_type>(value);
    return _valid = true;
  }

 private:
  std::tuple<OffsAttr, IncAttr, TermAttr> _attrs;
  bytes_view _value;
  bool _valid = false;
};

// analyzer implementation for numeric field. based on precision
// step it produces several terms representing ranges of the input
// term
class NumericTokenizer final : public BasicTokenizer,
                               private util::Noncopyable {
 public:
  static constexpr uint32_t kPrecisionStepDef = 16;
  static constexpr uint32_t kPrecisionStep32 = 8;

  static bytes_view value(bstring& buf, int32_t value);
  static bytes_view value(bstring& buf, int64_t value);

#ifndef FLOAT_T_IS_DOUBLE_T
  static bytes_view value(bstring& buf, float_t value);
#endif

  static bytes_view value(bstring& buf, double_t value);

  static constexpr std::string_view type_name() noexcept {
    return "numeric_token_stream";
  }

  bool next() final;

  void reset(int32_t value, uint32_t step = kPrecisionStepDef);
  void reset(int64_t value, uint32_t step = kPrecisionStepDef);

#ifndef FLOAT_T_IS_DOUBLE_T
  void reset(float_t value, uint32_t step = kPrecisionStepDef);
#endif

  void reset(double_t value, uint32_t step = kPrecisionStepDef);

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<NumericTokenizer>::id();
  }

 private:
  using BasicTokenizer::reset;

  //////////////////////////////////////////////////////////////////////////////
  /// @class numeric_term
  /// @brief TermAttr implementation for numeric_token_stream
  //////////////////////////////////////////////////////////////////////////////
  class NumericTerm final {
   public:
    static bytes_view value(bstring& buf, int32_t value) {
      decltype(_val) val;

      val.i32 = value;
      buf.resize(numeric_utils::numeric_traits<decltype(value)>::size());

      return NumericTerm::value(buf.data(), kNtInt, val, 0);
    }

    static bytes_view value(bstring& buf, int64_t value) {
      decltype(_val) val;

      val.i64 = value;
      buf.resize(numeric_utils::numeric_traits<decltype(value)>::size());

      return NumericTerm::value(buf.data(), kNtLong, val, 0);
    }

#ifndef FLOAT_T_IS_DOUBLE_T
    static bytes_view value(bstring& buf, float_t value) {
      decltype(_val) val;

      val.i32 = numeric_utils::numeric_traits<float_t>::integral(value);
      buf.resize(numeric_utils::numeric_traits<decltype(value)>::size());

      return NumericTerm::value(buf.data(), kNtFloat, val, 0);
    }
#endif

    static bytes_view value(bstring& buf, double_t value) {
      decltype(_val) val;

      val.i64 = numeric_utils::numeric_traits<double_t>::integral(value);
      buf.resize(numeric_utils::numeric_traits<decltype(value)>::size());

      return NumericTerm::value(buf.data(), kNtDbl, val, 0);
    }

    bool next(IncAttr& inc, bytes_view& out);

    void reset(int32_t value, uint32_t step) {
      _val.i32 = value;
      _type = kNtInt;
      _step = step;
      _shift = 0;
    }

    void reset(int64_t value, uint32_t step) {
      _val.i64 = value;
      _type = kNtLong;
      _step = step;
      _shift = 0;
    }

#ifndef FLOAT_T_IS_DOUBLE_T
    void reset(float_t value, uint32_t step) {
      _val.i32 = numeric_utils::numeric_traits<float_t>::integral(value);
      _type = kNtFloat;
      _step = step;
      _shift = 0;
    }
#endif

    void reset(double_t value, uint32_t step) {
      _val.i64 = numeric_utils::numeric_traits<double_t>::integral(value);
      _type = kNtDbl;
      _step = step;
      _shift = 0;
    }

   private:
    enum NumericType { kNtLong = 0, kNtDbl, kNtInt, kNtFloat };

    union ValueT {
      uint64_t i64;
      uint32_t i32;
    };

    static bytes_view value(byte_type* buf, NumericType type, ValueT val,
                            uint32_t shift);

    byte_type _data[numeric_utils::numeric_traits<double_t>::size()];
    ValueT _val;
    NumericType _type;
    uint32_t _step;
    uint32_t _shift;
  };

  NumericTerm _num;
};

// analyzer implementation for null field, a single null term.
class NullTokenizer final : public BasicTokenizer, private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "null_token_stream";
  }

  static constexpr std::string_view value_null() noexcept {
    // data pointer != nullptr or IRS_ASSERT failure in bytes_hash::insert(...)
    return {"\x00", 0};
  }

  bool next() noexcept final;

  void reset() noexcept { _valid = true; }

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<NullTokenizer>::id();
  }

 private:
  using BasicTokenizer::reset;

  bool _valid = false;
};

}  // namespace irs
