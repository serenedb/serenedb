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
#include <vpack/builder.h>
#include <vpack/exception.h>
#include <vpack/iterator.h>
#include <vpack/options.h>
#include <vpack/parser.h>
#include <vpack/slice.h>
#include <vpack/value_type.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>

#include "basics/buffer.h"
#include "basics/common.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/system-compiler.h"

namespace vpack {
struct AttributeExcludeHandler;
}  // namespace vpack

namespace sdb {
namespace log {
class Stream;
}

namespace basics {

struct NormalizedSlice {
  struct NormalizedObjectIterator : vpack::ObjectIterator {
    using ObjectIterator::ObjectIterator;

    std::pair<std::string_view, NormalizedSlice> operator*() const {
      auto [k, v] = ObjectIterator::operator*();
      SDB_ASSERT(k.isString());
      return {k.stringViewUnchecked(), NormalizedSlice{v}};
    }

    auto end() const {
      auto other = *this;
      other._position = other._size;
      return other;
    }

    bool operator==(const NormalizedObjectIterator& other) const noexcept {
      return _position == other._position;
    }
  };

  template<typename H>
  friend H AbslHashValue(H h, NormalizedSlice n) {
    // We don't account the type of the slice here, because equal hash for
    // slices of different type anyway will be possible and we don't need to
    // make hash computation slower
    if (n.s.isNumber()) {
      return H::combine(std::move(h), n.s.getNumber<double>());
    } else if (n.s.isString()) {
      return H::combine(std::move(h), n.s.stringViewUnchecked());
    } else if (n.s.isArray()) {
      vpack::ArrayIterator it{n.s};
      for (auto s : it) {
        h = H::combine(std::move(h), NormalizedSlice{s});
      }
      return H::combine(std::move(h),
                        absl::hash_internal::WeaklyMixedInteger{it.size()});
    } else if (n.s.isObject()) {
      NormalizedObjectIterator it{n.s, true};
      return H::combine(H::combine_unordered(std::move(h), it, it.end()),
                        absl::hash_internal::WeaklyMixedInteger{it.size()});
    }
    return H::combine(std::move(h), n.s.head());
  }

  vpack::Slice s;
};

class VPackHelper {
 public:
  VPackHelper() = delete;
  ~VPackHelper() = delete;

  /// static initializer for all VPack values
  static void initialize();

  struct VPackHash {
    size_t operator()(vpack::Slice slice) const {
      return VPackHelper::hash(slice);
    }
  };

  struct VPackEqual {
    bool operator()(vpack::Slice lhs, vpack::Slice rhs) const {
      return VPackHelper::equals(lhs, rhs);
    }
  };

  struct VPackLess {
    bool operator()(vpack::Slice lhs, vpack::Slice rhs) const {
      return VPackHelper::compare(lhs, rhs) < 0;
    }
  };

  template<typename T>
  static consteval std::string_view numberTypeName() {
    if constexpr (std::is_enum_v<T>) {
      return numberTypeName<std::underlying_type_t<T>>();
    } else if constexpr (std::is_floating_point_v<T>) {
      if constexpr (sizeof(T) == 8) {
        return "f64";
      } else {
        static_assert(sizeof(T) == 4);
        return "f32";
      }
    } else if constexpr (std::is_signed_v<T>) {
      if constexpr (sizeof(T) == 8) {
        return "i64";
      } else if constexpr (sizeof(T) == 4) {
        return "i32";
      } else if constexpr (sizeof(T) == 2) {
        return "i16";
      } else {
        static_assert(sizeof(T) == 1);
        return "i8";
      }
    } else {
      static_assert(std::is_unsigned_v<T>);
      if constexpr (sizeof(T) == 8) {
        return "u64";
      } else if constexpr (sizeof(T) == 4) {
        return "u32";
      } else if constexpr (sizeof(T) == 2) {
        return "u16";
      } else {
        static_assert(sizeof(T) == 1);
        return "u8";
      }
    }
  }

  template<typename T>
  static T getNumberImpl(vpack::Slice slice, T default_value, auto&& error) {
    static_assert(!numberTypeName<T>().empty());
    if (!slice.isNumber()) [[unlikely]] {
      return default_value;
    }
    try {
      return slice.getNumber<T>();
    } catch (...) {
      error();
    }
    SDB_UNREACHABLE();
  }

  template<typename T>
  static T getNumber(vpack::Slice slice, T default_value) {
    return getNumberImpl(slice, default_value, [&] {
      SDB_THROW(ERROR_BAD_PARAMETER,
                absl::StrCat("invalid value ", slice.getNumber<double>(),
                             " for ", numberTypeName<T>()));
    });
  }

  template<typename T>
  static T getNumber(vpack::Slice slice, std::string_view name,
                     T default_value) {
    if (!slice.isObject()) [[unlikely]] {
      return default_value;
    }
    slice = slice.get(name);
    return getNumberImpl(slice, default_value, [&] {
      SDB_THROW(
        ERROR_BAD_PARAMETER,
        absl::StrCat("invalid value ", slice.getNumber<double>(), " for ",
                     numberTypeName<T>(), " attribute '", name, "'"));
    });
  }

  static bool getBool(vpack::Slice slice, std::string_view name,
                      bool default_value) {
    if (!slice.isObject()) [[unlikely]] {
      return default_value;
    }
    slice = slice.get(name);
    return default_value ? !slice.isFalse() : slice.isTrue();
  }

  static vpack::Slice getSlice(vpack::Slice slice,
                               std::span<const std::string_view> names,
                               vpack::Slice default_value) {
    if (!slice.isObject()) [[unlikely]] {
      return default_value;
    }
    slice = slice.get(names);
    return slice.isNone() ? default_value : slice;
  }

  static std::string_view checkAndGetString(vpack::Slice slice,
                                            std::string_view name);

  template<typename T>
  static T checkAndGetNumber(vpack::Slice slice, std::string_view name) {
    SDB_ASSERT(slice.isObject());
    slice = slice.get(name);
    if (!slice.isNumber()) [[unlikely]] {
      SDB_THROW(
        ERROR_BAD_PARAMETER,
        absl::StrCat("attribute '", name,
                     slice.isNone() ? "' was not found" : "' is not a number"));
    }
    return getNumberImpl(slice, T{}, [&] {
      SDB_THROW(
        ERROR_BAD_PARAMETER,
        absl::StrCat("invalid value ", slice.getNumber<double>(), " for ",
                     numberTypeName<T>(), " attribute '", name, "'"));
    });
  }

  static std::string_view getString(vpack::Slice slice,
                                    std::string_view default_value) {
    if (!slice.isString()) [[unlikely]] {
      return default_value;
    }
    return slice.stringViewUnchecked();
  }

  static std::string_view getString(vpack::Slice slice, std::string_view name,
                                    std::string_view default_value) {
    if (!slice.isObject()) [[unlikely]] {
      return default_value;
    }
    return getString(slice.get(name), default_value);
  }

  static uint64_t stringUInt64(vpack::Slice slice);

  static uint64_t stringUInt64(vpack::Slice slice, std::string_view name) {
    return stringUInt64(slice.get(name));
  }

  /// parses a json file to VPack
  static vpack::Builder vpackFromFile(const char* filename);

  /// writes a VPack to a file
  static bool vpackToFile(const std::string& filename, vpack::Slice slice,
                          bool sync_file);

  template<typename T>
  static constexpr int compareSame(T l, T r) {
    if (l < r) {
      return -1;
    }
    if (l > r) {
      return 1;
    }
    return 0;
  }
  static int compareDouble(double l, double r) noexcept;
  static int compareIntUInt(int64_t l, uint64_t r) noexcept;
  static int compareIntDouble(int64_t l, double r) noexcept;
  static int compareUIntDouble(uint64_t l, double r) noexcept;

  static int compareNumbers(vpack::Slice lhs, vpack::Slice rhs);
  static int compareStrings(vpack::Slice lhs, vpack::Slice rhs);
  static int compareArrays(vpack::Slice lhs, vpack::Slice rhs);
  static int compareObjects(vpack::Slice lhs, vpack::Slice rhs);

  static int compare(vpack::Slice lhs, vpack::Slice rhs);

  static bool equalsDouble(double l, double r) noexcept;
  static bool equalsIntUInt(int64_t l, uint64_t r) noexcept;
  static bool equalsIntDouble(int64_t l, double r) noexcept;
  static bool equalsUIntDouble(uint64_t l, double r) noexcept;

  static bool equalsNumbers(vpack::Slice lhs, vpack::Slice rhs);
  static bool equalsStrings(vpack::Slice lhs, vpack::Slice rhs,
                            bool utf8 = false);
  static bool equalsArrays(vpack::Slice lhs, vpack::Slice rhs,
                           bool utf8 = false);
  static bool equalsObjects(vpack::Slice lhs, vpack::Slice rhs,
                            bool utf8 = false);

  static bool equals(vpack::Slice lhs, vpack::Slice rhs, bool utf8 = false);

  static size_t hash(vpack::Slice s);

  static uint64_t extractIdValue(vpack::Slice slice);

  static inline vpack::Options gStrictRequestValidationOptions;
  static inline vpack::Options gLooseRequestValidationOptions;

  // Just None with different weight in compare
  static constexpr uint8_t kMinKeySliceData = 0x15;
  static constexpr uint8_t kIllegalSliceData = 0x16;
  static constexpr uint8_t kMaxKeySliceData = 0x17;

  static constexpr vpack::Slice illegalSlice() noexcept {
    return vpack::Slice{&kIllegalSliceData};
  }
  static constexpr vpack::Slice minKeySlice() noexcept {
    return vpack::Slice{&kMinKeySliceData};
  }
  static constexpr vpack::Slice maxKeySlice() noexcept {
    return vpack::Slice{&kMaxKeySliceData};
  }
};

/// specializations for ErrorCode, shortcut to avoid back-and-forth
/// casts from and to int.
template<>
inline ErrorCode VPackHelper::getNumber<ErrorCode>(vpack::Slice slice,
                                                   std::string_view name,
                                                   ErrorCode default_value) {
  return ErrorCode{getNumber<int>(slice, name, default_value.value())};
}

template<size_t N>
class StringFromParts final : public vpack::IStringFromParts {
 public:
  template<typename... Args>
  StringFromParts(Args&&... args) noexcept
    : _parts{std::forward<Args>(args)...} {
    static_assert(sizeof...(Args) == N);
  }

  size_t numberOfParts() const final { return N; }

  size_t totalLength() const final {
    size_t length = 0;
    for (size_t index = 0; index != N; ++index) {
      length += _parts[index].length();
    }
    return length;
  }

  std::string_view operator()(size_t index) const final {
    SDB_ASSERT(index < numberOfParts());
    return _parts[index];
  }

 private:
  std::array<std::string_view, N> _parts;
};

template<typename... Args>
auto MakeStringFromParts(Args&&... args) noexcept {
  return StringFromParts<sizeof...(Args)>{std::forward<Args>(args)...};
}

}  // namespace basics
}  // namespace sdb
