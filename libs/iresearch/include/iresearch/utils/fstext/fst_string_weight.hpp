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

#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>
#include <fst/string-weight.h>

#include <string>

#include "basics/shared.hpp"
#include "basics/std.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace fst {

template<typename Label>
class StringLeftWeight;

template<typename Label>
struct StringLeftWeightTraits {
  inline static const StringLeftWeight<Label>& Zero();

  inline static const StringLeftWeight<Label>& One();

  inline static const StringLeftWeight<Label>& NoWeight();

  inline static bool Member(const StringLeftWeight<Label>& weight);
};

// String semiring: (longest_common_prefix/suffix, ., Infinity, Epsilon)
template<typename Label>
class StringLeftWeight : public StringLeftWeightTraits<Label> {
 public:
  using ReverseWeight = StringLeftWeight<Label>;
  using str_t = irs::basic_string<Label>;
  using iterator = typename str_t::const_iterator;

  static const std::string& Type() {
    static const std::string kType = "left_string";
    return kType;
  }

  friend bool operator==(const StringLeftWeight& lhs,
                         const StringLeftWeight& rhs) noexcept {
    return lhs._str == rhs._str;
  }

  StringLeftWeight() = default;

  explicit StringLeftWeight(Label label) : _str(1, label) {}

  template<typename Iterator>
  StringLeftWeight(Iterator begin, Iterator end) : _str(begin, end) {}

  StringLeftWeight(const StringLeftWeight&) = default;
  StringLeftWeight(StringLeftWeight&&) = default;

  explicit StringLeftWeight(irs::basic_string_view<Label> rhs)
    : _str(rhs.data(), rhs.size()) {}

  StringLeftWeight& operator=(StringLeftWeight&&) = default;
  StringLeftWeight& operator=(const StringLeftWeight&) = default;

  StringLeftWeight& operator=(irs::basic_string_view<Label> rhs) {
    _str.assign(rhs.data(), rhs.size());
    return *this;
  }

  bool Member() const noexcept {
    return StringLeftWeightTraits<Label>::Member(*this);
  }

  std::istream& Read(std::istream& strm) {
    // read size
    // use varlen encoding since weights are usually small
    char buf[sizeof(uint32_t)];
    strm.read(buf, sizeof(buf));

    const uint32_t size = absl::little_endian::Load32(&buf);

    // read content
    _str.resize(size);
    strm.read(reinterpret_cast<char*>(_str.data()), size * sizeof(Label));

    return strm;
  }

  std::ostream& Write(std::ostream& strm) const {
    // write size
    // use varlen encoding since weights are usually small
    const uint32_t size = static_cast<uint32_t>(Size());

    char buf[sizeof(uint32_t)];
    absl::little_endian::Store32(&buf, size);
    strm.write(buf, sizeof(buf));
    strm.write(reinterpret_cast<const char*>(_str.c_str()),
               size * sizeof(Label));

    return strm;
  }

  const auto& Impl() const noexcept { return _str; }

  StringLeftWeight Quantize(float /*delta*/ = kDelta) const noexcept {
    return *this;
  }

  ReverseWeight Reverse() const {
    return ReverseWeight(_str.rbegin(), _str.rend());
  }

  static uint64_t Properties() noexcept {
    static constexpr auto kProps = kLeftSemiring | kIdempotent;
    return kProps;
  }

  Label& operator[](size_t i) noexcept { return _str[i]; }

  const Label& operator[](size_t i) const noexcept { return _str[i]; }

  const Label* c_str() const noexcept { return _str.c_str(); }

  void Resize(size_t size) noexcept { _str.resize(size); }

  bool Empty() const noexcept { return _str.empty(); }

  void Clear() noexcept { _str.clear(); }

  size_t Size() const noexcept { return _str.size(); }

  void PushBack(Label label) { _str.push_back(label); }

  template<typename Iterator>
  void PushBack(Iterator begin, Iterator end) {
    _str.append(begin, end);
  }

  void PushBack(const StringLeftWeight& w) { PushBack(w.begin(), w.end()); }

  void Reserve(size_t capacity) { _str.reserve(capacity); }

  iterator begin() const noexcept { return _str.begin(); }
  iterator end() const noexcept { return _str.end(); }

  // intentionally implicit
  operator irs::basic_string_view<Label>() const noexcept { return _str; }

  // intentionally implicit
  operator irs::basic_string<Label>() && noexcept { return std::move(_str); }

 private:
  str_t _str;
};

template<typename Label>
const StringLeftWeight<Label>& StringLeftWeightTraits<Label>::Zero() {
  static const StringLeftWeight<Label> kZero(
    static_cast<Label>(kStringInfinity));  // cast same as in FST
  return kZero;
}

template<typename Label>
const StringLeftWeight<Label>& StringLeftWeightTraits<Label>::One() {
  static const StringLeftWeight<Label> kOne;
  return kOne;
}

template<typename Label>
const StringLeftWeight<Label>& StringLeftWeightTraits<Label>::NoWeight() {
  static const StringLeftWeight<Label> kNoWeight(
    static_cast<Label>(kStringBad));  // cast same as in FST
  return kNoWeight;
}

template<typename Label>
bool StringLeftWeightTraits<Label>::Member(
  const StringLeftWeight<Label>& weight) {
  return weight != NoWeight();
}

template<typename Label>
inline std::ostream& operator<<(std::ostream& strm,
                                const StringLeftWeight<Label>& weight) {
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

template<typename Label>
inline std::istream& operator>>(std::istream& strm,
                                StringLeftWeight<Label>& weight) {
  std::string str;
  strm >> str;
  if (str == "Infinity") {
    weight = StringLeftWeight<Label>::Zero();
  } else if (str == "Epsilon") {
    weight = StringLeftWeight<Label>::One();
  } else {
    weight.Clear();
    char* p = nullptr;
    for (const char* cs = str.c_str(); !p || *p != '\0'; cs = p + 1) {
      const Label label = strtoll(cs, &p, 10);
      if (p == cs || (*p != 0 && *p != kStringSeparator)) {
        strm.clear(std::ios::badbit);
        break;
      }
      weight.PushBack(label);
    }
  }
  return strm;
}

// Longest common prefix for left string semiring.
template<typename Empty = void, typename Label = char>
inline StringLeftWeight<Label> Plus(const StringLeftWeight<Label>& lhs,
                                    const StringLeftWeight<Label>& rhs) {
  typedef StringLeftWeight<Label> Weight;

  if (!lhs.Member() || !rhs.Member()) {
    return Weight::NoWeight();
  }

  if (lhs == Weight::Zero()) {
    return rhs;
  }

  if (rhs == Weight::Zero()) {
    return lhs;
  }

  const auto* plhs = &lhs;
  const auto* prhs = &rhs;

  if (rhs.Size() > lhs.Size()) {
    // enusre that 'prhs' is shorter than 'plhs'
    // The behavior is undefined if the second range is shorter than the first
    // range. (http://en.cppreference.com/w/cpp/algorithm/mismatch)
    std::swap(plhs, prhs);
  }

  SDB_ASSERT(prhs->Size() <= plhs->Size());

  return Weight(prhs->begin(),
                std::mismatch(prhs->begin(), prhs->end(), plhs->begin()).first);
}

template<typename Empty = void, typename Label = char>
inline StringLeftWeight<Label> Times(const StringLeftWeight<Label>& lhs,
                                     const StringLeftWeight<Label>& rhs) {
  typedef StringLeftWeight<Label> Weight;

  if (!lhs.Member() || !rhs.Member()) {
    return Weight::NoWeight();
  }

  if (lhs == Weight::Zero() || rhs == Weight::Zero()) {
    return Weight::Zero();
  }

  Weight product;
  product.Reserve(lhs.Size() + rhs.Size());
  product.PushBack(lhs.begin(), lhs.end());
  product.PushBack(rhs.begin(), rhs.end());
  return product;
}

// Left division in a left string semiring.
template<typename Label>
inline StringLeftWeight<Label> DivideLeft(const StringLeftWeight<Label>& lhs,
                                          const StringLeftWeight<Label>& rhs) {
  typedef StringLeftWeight<Label> Weight;

  if (!lhs.Member() || !rhs.Member()) {
    return Weight::NoWeight();
  }

  if (rhs == Weight::Zero()) {
    return Weight::NoWeight();
  }
  if (lhs == Weight::Zero()) {
    return Weight::Zero();
  }

  if (rhs.Size() > lhs.Size()) {
    return Weight();
  }

  SDB_ASSERT(irs::basic_string_view<Label>(lhs).starts_with(rhs));

  return Weight(lhs.begin() + rhs.Size(), lhs.end());
}

template<typename Empty = void, typename Label = char>
inline StringLeftWeight<Label> Divide(const StringLeftWeight<Label>& lhs,
                                      const StringLeftWeight<Label>& rhs,
                                      DivideType typ) {
  SDB_ASSERT(DIVIDE_LEFT == typ);
  return DivideLeft(lhs, rhs);
}

template<>
struct StringLeftWeightTraits<irs::byte_type> {
  static const StringLeftWeight<irs::byte_type>& Zero() noexcept {
    static const StringLeftWeight<irs::byte_type> kZero;
    return kZero;
  }

  static const StringLeftWeight<irs::byte_type>& One() noexcept {
    return Zero();
  }

  static const StringLeftWeight<irs::byte_type>& NoWeight() noexcept {
    return Zero();
  }

  static bool Member(const StringLeftWeight<irs::byte_type>&) noexcept {
    // always member
    return true;
  }
};

inline std::ostream& operator<<(
  std::ostream& strm, const StringLeftWeight<irs::byte_type>& weight) {
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

inline std::istream& operator>>(std::istream& strm,
                                StringLeftWeight<irs::byte_type>& weight) {
  std::string str;
  strm >> str;
  if (str == "Epsilon") {
    weight = StringLeftWeight<irs::byte_type>::One();
  } else {
    weight.Clear();
    char* p = nullptr;
    for (const char* cs = str.c_str(); !p || *p != '\0'; cs = p + 1) {
      const irs::byte_type label = strtoll(cs, &p, 10);
      if (p == cs || (*p != 0 && *p != kStringSeparator)) {
        strm.clear(std::ios::badbit);
        break;
      }
      weight.PushBack(label);
    }
  }
  return strm;
}

// Longest common prefix for left string semiring.
// For binary strings that's impossible to use
// Zero() or NoWeight() as they may interfere
// with real values
inline irs::bytes_view PlusImpl(irs::bytes_view lhs, irs::bytes_view rhs) {
  const auto* plhs = &lhs;
  const auto* prhs = &rhs;

  if (rhs.size() > lhs.size()) {
    // enusre that 'prhs' is shorter than 'plhs'
    // The behavior is undefined if the second range is shorter than the first
    // range. (http://en.cppreference.com/w/cpp/algorithm/mismatch)
    std::swap(plhs, prhs);
  }

  auto pair =
    std::mismatch(prhs->data(), prhs->data() + prhs->size(), plhs->data());
  return {prhs->data(), static_cast<size_t>(pair.first - prhs->data())};
}

inline irs::bytes_view Plus(const StringLeftWeight<irs::byte_type>& lhs,
                            const StringLeftWeight<irs::byte_type>& rhs) {
  return PlusImpl(lhs, rhs);
}

inline irs::bytes_view Plus(irs::bytes_view lhs,
                            const StringLeftWeight<irs::byte_type>& rhs) {
  return PlusImpl(lhs, rhs);
}

inline irs::bytes_view Plus(const StringLeftWeight<irs::byte_type>& lhs,
                            irs::bytes_view rhs) {
  return PlusImpl(lhs, rhs);
}

// For binary strings that's impossible to use
// Zero() or NoWeight() as they may interfere
// with real values
inline StringLeftWeight<irs::byte_type> TimesImpl(irs::bytes_view lhs,
                                                  irs::bytes_view rhs) {
  using Weight = StringLeftWeight<irs::byte_type>;

  Weight product;
  product.Reserve(lhs.size() + rhs.size());
  product.PushBack(lhs.begin(), lhs.end());
  product.PushBack(rhs.begin(), rhs.end());
  return product;
}

inline StringLeftWeight<irs::byte_type> Times(
  const StringLeftWeight<irs::byte_type>& lhs,
  const StringLeftWeight<irs::byte_type>& rhs) {
  return TimesImpl(lhs, rhs);
}

inline StringLeftWeight<irs::byte_type> Times(
  irs::bytes_view lhs, const StringLeftWeight<irs::byte_type>& rhs) {
  return TimesImpl(lhs, rhs);
}

inline StringLeftWeight<irs::byte_type> Times(
  const StringLeftWeight<irs::byte_type>& lhs, irs::bytes_view rhs) {
  return TimesImpl(lhs, rhs);
}

// Left division in a left string semiring.
// For binary strings that's impossible to use
// Zero() or NoWeight() as they may interfere
// with real values
inline irs::bytes_view DivideLeftImpl(irs::bytes_view lhs,
                                      irs::bytes_view rhs) {
  if (rhs.size() > lhs.size()) {
    return {};
  }
  SDB_ASSERT(lhs.starts_with(rhs), irs::ViewCast<char>(rhs),
             " is not prefix of ", irs::ViewCast<char>(lhs));
  return {lhs.data() + rhs.size(), lhs.size() - rhs.size()};
}

inline irs::bytes_view DivideLeft(const StringLeftWeight<irs::byte_type>& lhs,
                                  const StringLeftWeight<irs::byte_type>& rhs) {
  return DivideLeftImpl(lhs, rhs);
}

inline irs::bytes_view DivideLeft(irs::bytes_view lhs,
                                  const StringLeftWeight<irs::byte_type>& rhs) {
  return DivideLeftImpl(lhs, rhs);
}

inline irs::bytes_view DivideLeft(const StringLeftWeight<irs::byte_type>& lhs,
                                  irs::bytes_view rhs) {
  return DivideLeftImpl(lhs, rhs);
}

inline irs::bytes_view DivideLeft(irs::bytes_view lhs, irs::bytes_view rhs) {
  return DivideLeftImpl(lhs, rhs);
}

inline StringLeftWeight<irs::byte_type> Divide(
  const StringLeftWeight<irs::byte_type>& lhs,
  const StringLeftWeight<irs::byte_type>& rhs, DivideType typ) {
  SDB_ASSERT(DIVIDE_LEFT == typ);
  return StringLeftWeight<irs::byte_type>(DivideLeft(lhs, rhs));
}

}  // namespace fst
