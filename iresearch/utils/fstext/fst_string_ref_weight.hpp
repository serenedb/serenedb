////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <fst/string-weight.h>

#include "iresearch/utils/string.hpp"

namespace fst {
namespace fstext {

template<typename Label>
class StringRefWeight;

template<typename Label>
struct StringRefWeightTraits {
  static const StringRefWeight<Label> Zero();

  static const StringRefWeight<Label> One();

  static const StringRefWeight<Label> NoWeight();

  static bool Member(const StringRefWeight<Label>& weight);
};

template<typename Label>
class StringRefWeight : public StringRefWeightTraits<Label> {
 public:
  using str_t = irs::basic_string_view<Label>;

  static const std::string& Type() {
    static const std::string kType = "left_string";
    return kType;
  }

  friend bool operator==(StringRefWeight lhs, StringRefWeight rhs) noexcept {
    return lhs._str == rhs._str;
  }

  StringRefWeight() = default;

  template<typename Iterator>
  StringRefWeight(Iterator begin, Iterator end) noexcept : _str(begin, end) {}

  StringRefWeight(const StringRefWeight&) = default;
  StringRefWeight(StringRefWeight&&) = default;

  explicit StringRefWeight(irs::basic_string_view<Label> rhs) noexcept
    : _str{rhs} {}

  StringRefWeight& operator=(StringRefWeight&&) = default;
  StringRefWeight& operator=(const StringRefWeight&) = default;

  StringRefWeight& operator=(irs::basic_string_view<Label> rhs) noexcept {
    _str = rhs;
    return *this;
  }

  bool Member() const noexcept {
    return StringRefWeightTraits<Label>::Member(*this);
  }

  const auto& Impl() const noexcept { return _str; }

  StringRefWeight Quantize(float /*delta*/ = kDelta) const noexcept {
    return *this;
  }

  static uint64_t Properties() noexcept {
    static constexpr auto kProps = kLeftSemiring | kIdempotent;
    return kProps;
  }

  Label& operator[](size_t i) noexcept { return _str[i]; }

  const Label& operator[](size_t i) const noexcept { return _str[i]; }

  const Label* c_str() const noexcept { return _str.c_str(); }

  bool Empty() const noexcept { return _str.empty(); }

  void Clear() noexcept { _str.clear(); }

  size_t Size() const noexcept { return _str.size(); }

  const Label* begin() const noexcept { return _str.data(); }
  const Label* end() const noexcept { return _str.data() + _str.size(); }

  // intentionally implicit
  operator irs::basic_string_view<Label>() const noexcept { return _str; }

 private:
  str_t _str;
};

template<typename Label>
inline std::ostream& operator<<(std::ostream& strm,
                                StringRefWeight<Label> weight) {
  if (weight.Empty()) {
    return strm << "Epsilon";
  }

  auto begin = weight.begin();
  const auto& first = *begin;

  if (first == kStringInfinity) {
    return strm << "Infinity";
  } else if (first == kStringBad) {
    return strm << "BadString";
  }

  const auto end = weight.end();
  if (begin != end) {
    strm << *begin;

    for (++begin; begin != end; ++begin) {
      strm << kStringSeparator << *begin;
    }
  }

  return strm;
}

template<>
struct StringRefWeightTraits<irs::byte_type> {
  static constexpr StringRefWeight<irs::byte_type> Zero() noexcept {
    return {};
  }

  static constexpr StringRefWeight<irs::byte_type> One() noexcept {
    return Zero();
  }

  static constexpr StringRefWeight<irs::byte_type> NoWeight() noexcept {
    return Zero();
  }

  static constexpr bool Member(
    StringRefWeight<irs::byte_type> /*weight*/) noexcept {
    // always a member
    return true;
  }
};

inline std::ostream& operator<<(std::ostream& strm,
                                StringRefWeight<irs::byte_type> weight) {
  if (weight.Empty()) {
    return strm << "Epsilon";
  }

  auto begin = weight.begin();

  const auto end = weight.end();
  if (begin != end) {
    strm << *begin;

    for (++begin; begin != end; ++begin) {
      strm << kStringSeparator << *begin;
    }
  }

  return strm;
}

}  // namespace fstext
}  // namespace fst
