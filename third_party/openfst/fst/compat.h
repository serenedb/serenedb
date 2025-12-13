// Copyright 2005-2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.

#ifndef FST_COMPAT_H_
#define FST_COMPAT_H_

#include <algorithm>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <absl/base/casts.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#if defined(__GNUC__) || defined(__clang__)
#define OPENFST_DEPRECATED(message) __attribute__((deprecated(message)))
#elif defined(_MSC_VER)
#define OPENFST_DEPRECATED(message) [[deprecated(message)]]
#else
#define OPENFST_DEPRECATED(message)
#endif

namespace fst {

// Downcasting.

template <typename To, typename From>
inline To down_cast(From *f) {
  return static_cast<To>(f);
}

template <typename To, typename From>
inline To down_cast(From &f) {
  return static_cast<To>(f);
}

// Bitcasting.
using std::bit_cast;

template <typename T>
T UnalignedLoad(const void *p) {
  T t;
  std::memcpy(&t, p, sizeof t);
  return t;
}

using absl::implicit_cast;

// Checksums.
class CheckSummer {
 public:
  CheckSummer();

  void Reset();

  void Update(std::string_view data);

  std::string Digest() { return check_sum_; }

 private:
  static constexpr int kCheckSumLength = 32;
  int count_;
  std::string check_sum_;

  CheckSummer(const CheckSummer &) = delete;
  CheckSummer &operator=(const CheckSummer &) = delete;
};

using std::make_unique_for_overwrite;

template <typename T>
std::unique_ptr<T> WrapUnique(T *ptr) {
  return std::unique_ptr<T>(ptr);
}

// String munging.

using absl::ByAnyChar;
using absl::ConsumePrefix;
using absl::SkipEmpty;
using absl::StartsWith;
using absl::StrCat;
using absl::StrContains;
using absl::StripTrailingAsciiWhitespace;
using absl::StrJoin;
using absl::StrSplit;

}  // namespace fst

#endif  // FST_COMPAT_H_
