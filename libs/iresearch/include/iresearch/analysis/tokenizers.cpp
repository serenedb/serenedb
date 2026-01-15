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

#include "tokenizers.hpp"

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"

namespace irs {

bool BooleanTokenizer::next() noexcept {
  const bool valid = _valid;
  std::get<TermAttr>(_attrs).value = ViewCast<byte_type>(value(_value));
  _valid = false;
  return valid;
}

bool StringTokenizer::next() noexcept {
  const bool valid = _valid;
  std::get<TermAttr>(_attrs).value = _value;
  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(_value.size());
  _valid = false;
  return valid;
}

bytes_view NumericTokenizer::NumericTerm::value(byte_type* buf,
                                                NumericType type, ValueT val,
                                                uint32_t shift) {
  switch (type) {
    case kNtLong: {
      using TraitsT = numeric_utils::numeric_traits<int64_t>;
      static_assert(
        TraitsT::size() <=
        std::size(decltype(NumericTokenizer::NumericTerm::_data){}));

      return {buf, TraitsT::encode(val.i64, buf, shift)};
    }
    case kNtDbl: {
      using TraitsT = numeric_utils::numeric_traits<double_t>;
      static_assert(
        TraitsT::size() <=
        std::size(decltype(NumericTokenizer::NumericTerm::_data){}));

      return {buf, TraitsT::encode(val.i64, buf, shift)};
    }
    case kNtInt: {
      using TraitsT = numeric_utils::numeric_traits<int32_t>;
      static_assert(
        TraitsT::size() <=
        std::size(decltype(NumericTokenizer::NumericTerm::_data){}));

      return {buf, TraitsT::encode(val.i32, buf, shift)};
    }
    case kNtFloat: {
      using TraitsT = numeric_utils::numeric_traits<float_t>;
      static_assert(
        TraitsT::size() <=
        std::size(decltype(NumericTokenizer::NumericTerm::_data){}));

      return {buf, TraitsT::encode(val.i32, buf, shift)};
    }
  }

  return {};
}

bool NumericTokenizer::NumericTerm::next(IncAttr& inc, bytes_view& out) {
  constexpr uint32_t kIncrementValue[]{0, 1};
  constexpr uint32_t kBitsRequired[]{BitsRequired<int64_t>(),
                                     BitsRequired<int32_t>()};

  if (_shift >= kBitsRequired[_type > kNtDbl]) {
    return false;
  }

  out = value(_data, _type, _val, _shift);
  _shift += _step;
  inc.value = kIncrementValue[_step == _shift];

  return true;
}

bool NumericTokenizer::next() {
  return _num.next(std::get<IncAttr>(_attrs), std::get<TermAttr>(_attrs).value);
}

void NumericTokenizer::reset(int32_t value,
                             uint32_t step /* = PRECISION_STEP_DEF */) {
  _num.reset(value, step);
}

void NumericTokenizer::reset(int64_t value,
                             uint32_t step /* = PRECISION_STEP_DEF */) {
  _num.reset(value, step);
}

#ifndef FLOAT_T_IS_DOUBLE_T
void NumericTokenizer::reset(float_t value,
                             uint32_t step /* = PRECISION_STEP_DEF */) {
  _num.reset(value, step);
}
#endif

void NumericTokenizer::reset(double_t value,
                             uint32_t step /* = PRECISION_STEP_DEF */) {
  _num.reset(value, step);
}

bytes_view NumericTokenizer::value(bstring& buf, int32_t value) {
  return NumericTerm::value(buf, value);
}

bytes_view NumericTokenizer::value(bstring& buf, int64_t value) {
  return NumericTerm::value(buf, value);
}

#ifndef FLOAT_T_IS_DOUBLE_T
bytes_view NumericTokenizer::value(bstring& buf, float_t value) {
  return NumericTerm::value(buf, value);
}
#endif

bytes_view NumericTokenizer::value(bstring& buf, double_t value) {
  return NumericTerm::value(buf, value);
}

bool NullTokenizer::next() noexcept {
  const bool valid = _valid;
  std::get<TermAttr>(_attrs).value = ViewCast<byte_type>(value_null());
  _valid = false;
  return valid;
}

}  // namespace irs
