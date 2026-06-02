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

#include <absl/algorithm/container.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <optional>
#include <ranges>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

#define BOOST_PFR_USE_STD_MAKE_INTEGRAL_SEQUENCE 1
#include <boost/pfr.hpp>
#include <boost/pfr/detail/for_each_field.hpp>
#include <boost/pfr/tuple_size.hpp>

static_assert(BOOST_PFR_USE_CPP17);
static_assert(BOOST_PFR_USE_LOOPHOLE == 0);
static_assert(BOOST_PFR_USE_STD_MAKE_INTEGRAL_SEQUENCE);
static_assert(BOOST_PFR_HAS_GUARANTEED_COPY_ELISION);
static_assert(BOOST_PFR_ENABLE_IMPLICIT_REFLECTION);
static_assert(BOOST_PFR_CORE_NAME_ENABLED);
#ifndef BOOST_PFR_FUNCTION_SIGNATURE
static_assert(false);
#endif
#ifndef BOOST_PFR_CORE_NAME_PARSING
static_assert(false);
#endif
static_assert(BOOST_PFR_ENABLED);

#include <duckdb/common/types/string_type.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/type_traits.h"

namespace sdb::basics {
namespace detail {

struct Empty {};

template<size_t N>
struct FixedString {
  constexpr FixedString() = default;
  constexpr FixedString(const char (&part)[N + 1]) noexcept {
    std::copy_n(part, N, std::begin(value));
  }

  constexpr operator std::string_view() const noexcept { return {value, N}; }

  char value[N + 1]{};
};

template<size_t N>
FixedString(const char (&part)[N]) -> FixedString<N - 1>;

}  // namespace detail

struct ObjectFormat {};
struct TupleFormat {};

namespace detail {

template<typename F, typename V, typename A, bool Writing>
class Context {
 public:
  using Format = F;
  using Arg = A;
  using Sink = V;

  explicit Context(V io, const Arg& arg) : _io{io}, _arg{&arg} {}

  decltype(auto) io() const {
    if constexpr (std::is_pointer_v<V>) {
      return *_io;
    } else {
      return _io;
    }
  }

  auto& arg() const noexcept { return *_arg; }

 private:
  static_assert(sizeof(V) == sizeof(void*));
  V _io;
  const Arg* _arg;
};

template<typename T>
concept IsMap =
  std::same_as<std::pair<const typename T::key_type, typename T::mapped_type>,
               typename T::value_type>;

template<typename T>
concept IsSet = std::same_as<typename T::key_type, typename T::value_type>;

template<typename T>
struct IsString : std::false_type {};
template<typename C, typename T>
struct IsString<std::basic_string_view<C, T>> : std::true_type {};
template<typename C, typename T, typename A>
struct IsString<std::basic_string<C, T, A>> : std::true_type {};

template<typename T>
struct IsArray : std::false_type {};
template<typename T, size_t N>
struct IsArray<std::array<T, N>> : std::true_type {};

// There is no tests for this and looks like it doesn't work
// So disable for now
// template<typename T>
// struct IsArray<std::span<T>> : std::false_type {};
// template<typename T, size_t N>
// struct IsArray<std::span<T, N>> : std::true_type {};
// template<typename T, size_t N>
// struct IsArray<T[N]> : std::true_type {};

template<typename T>
struct IsTuple : std::false_type {};
template<typename... Args>
struct IsTuple<std::tuple<Args...>> : std::true_type {};
template<typename... Args>
struct IsTuple<std::pair<Args...>> : std::true_type {};

template<typename T>
struct IsOptional : std::false_type {};
template<typename... Args>
struct IsOptional<std::optional<Args...>> : std::true_type {};

template<typename T>
struct IsUniquePtr : std::false_type {};
template<typename... Args>
struct IsUniquePtr<std::unique_ptr<Args...>> : std::true_type {};

template<typename T>
struct IsSharedPtr : std::false_type {};
template<typename... Args>
struct IsSharedPtr<std::shared_ptr<Args...>> : std::true_type {};

template<typename... Args>
struct IsVariant : std::false_type {};
template<typename... Args>
struct IsVariant<std::variant<Args...>> : std::true_type {};

template<typename T>
concept IsRange = requires(std::enable_if_t<!IsString<T>::value, T> t) {
  t.begin();
  t.end();
};

template<typename T>
concept IsContainer = requires(T t) {
  t.clear();
  t.emplace_back();
};

template<typename T>
concept HasReserve = requires(T t) { t.reserve(1); };

template<typename T>
concept HasResize = requires(T t) { t.resize(1); };

template<typename T>
concept HasAssign = requires(T t) { t.assign(std::string_view{}); };

template<typename Sink, typename F, typename T, typename A>
concept HasWriteOverload = requires(T&& t, Sink& b, const A& arg) {
  SerdeWrite(Context<F, Sink*, A, true>{&b, arg}, t);
};

template<typename Source, typename F, typename T, typename A>
concept HasSourceReadOverload = requires(T& t, Source& src, const A& arg) {
  SerdeRead(Context<F, Source*, A, false>{&src, arg}, t);
};

template<typename Sink, typename T>
concept HasMemberSerialize = requires(const T& t, Sink& b) { t.Serialize(b); };

template<typename Source, typename T>
concept HasStaticDeserialize = requires(Source& src) {
  { T::Deserialize(src) } -> std::same_as<T>;
};

template<typename Sink>
void WriteString(Sink& sink, std::string_view v) {
  if constexpr (requires { sink.WriteValue(v); }) {
    sink.WriteValue(v);
  } else {
    sink.WriteValue(
      duckdb::string_t{v.data(), static_cast<uint32_t>(v.size())});
  }
}

template<typename Source, typename String>
void ReadString(Source& src, String& out) {
  if constexpr (std::is_same_v<typename String::value_type, char>) {
    out = src.ReadString();
  } else {
    const auto s = src.ReadString();
    out.assign(reinterpret_cast<const typename String::value_type*>(s.data()),
               s.size());
  }
}

template<typename T>
inline constexpr bool kIsSerializableArithmetic =
  std::is_same_v<T, char> || std::is_same_v<T, int8_t> ||
  std::is_same_v<T, uint8_t> || std::is_same_v<T, int16_t> ||
  std::is_same_v<T, uint16_t> || std::is_same_v<T, int32_t> ||
  std::is_same_v<T, uint32_t> || std::is_same_v<T, int64_t> ||
  std::is_same_v<T, uint64_t> || std::is_same_v<T, float> ||
  std::is_same_v<T, double>;

template<typename T>
[[noreturn]] void ThrowInvalidEnum(int64_t raw) {
  SDB_THROW(ERROR_BAD_PARAMETER, "Invalid enum value ", raw, " for ",
            magic_enum::enum_type_name<T>());
}

}  // namespace detail

template<typename T>
inline constexpr bool kIsString = detail::IsString<std::decay_t<T>>::value;

template<typename T>
inline constexpr bool kIsArray = detail::IsArray<std::decay_t<T>>::value;

template<typename T>
inline constexpr bool kIsTuple = detail::IsTuple<std::decay_t<T>>::value;

template<typename T>
inline constexpr bool kIsOptional = detail::IsOptional<T>::value;

template<typename T>
inline constexpr bool kIsUniquePtr = detail::IsUniquePtr<T>::value;

template<typename T>
inline constexpr bool kIsSharedPtr = detail::IsSharedPtr<T>::value;

template<typename T>
inline constexpr bool kIsPointer =
  kIsUniquePtr<T> || kIsSharedPtr<T> || std::is_pointer_v<T>;

template<typename T>
inline constexpr bool kIsNullable =
  std::is_empty_v<T> || kIsOptional<T> || kIsPointer<T>;

template<typename T>
inline constexpr bool kIsPrimitive =
  std::is_arithmetic_v<T> || kIsString<T> || std::is_enum_v<T>;

template<typename T>
inline constexpr bool kIsVariant = detail::IsVariant<T>::value;

template<typename T>
inline constexpr bool kIsSupportedBuiltIn =
  std::is_enum_v<T> || detail::IsContainer<T> || detail::IsRange<T> ||
  detail::IsMap<T> || detail::IsSet<T> || kIsArray<T> || kIsTuple<T> ||
  kIsNullable<T> || kIsPrimitive<T> || std::is_aggregate_v<T>;

template<typename T, typename F, typename A, typename Sink>
inline constexpr bool kIsWritable = kIsSupportedBuiltIn<T> || kIsVariant<T> ||
                                    detail::HasWriteOverload<Sink, F, T, A> ||
                                    detail::HasMemberSerialize<Sink, T>;

template<typename T, typename F, typename A, typename Source>
inline constexpr bool kIsReadable =
  kIsSupportedBuiltIn<T> || kIsVariant<T> ||
  detail::HasSourceReadOverload<Source, F, T, A> ||
  detail::HasStaticDeserialize<Source, T>;

namespace detail {

template<typename Variant, typename ReadInto>
bool ReadVariantIndex(Variant& v, size_t index, ReadInto&& read_into) {
  return [&]<size_t... I>(std::index_sequence<I...>) {
    bool matched = false;
    (
      [&] {
        if (!matched && index == I) {
          read_into(v.template emplace<I>());
          matched = true;
        }
      }(),
      ...);
    return matched;
  }(std::make_index_sequence<std::variant_size_v<Variant>>{});
}

}  // namespace detail

template<typename Source, typename U, typename A = detail::Empty>
void ReadTuple(Source& src, U& out, const A& arg = {}) {
  auto impl = [&arg]<typename T>(this auto& self, T& value,
                                 Source& src) -> void {
    static_assert(kIsReadable<T, TupleFormat, A, Source>,
                  "Type is not readable");

    if constexpr (detail::HasSourceReadOverload<Source, TupleFormat, T, A>) {
      SerdeRead(detail::Context<TupleFormat, Source*, A, false>{&src, arg},
                value);
    } else if constexpr (detail::HasStaticDeserialize<Source, T>) {
      src.OnObjectBegin();
      value = T::Deserialize(src);
      src.OnObjectEnd();
    } else if constexpr (std::is_same_v<T, bool>) {
      value = src.ReadBool();
    } else if constexpr (std::is_enum_v<T>) {
      std::underlying_type_t<T> raw;
      self(raw, src);
      const auto v = magic_enum::enum_cast<T>(raw);
      if (!v) [[unlikely]] {
        detail::ThrowInvalidEnum<T>(static_cast<int64_t>(raw));
      }
      value = *v;
    } else if constexpr (std::is_same_v<T, char>) {
      value = static_cast<char>(src.ReadUnsignedInt8());
    } else if constexpr (std::is_same_v<T, int8_t>) {
      value = src.ReadSignedInt8();
    } else if constexpr (std::is_same_v<T, uint8_t>) {
      value = src.ReadUnsignedInt8();
    } else if constexpr (std::is_same_v<T, int16_t>) {
      value = src.ReadSignedInt16();
    } else if constexpr (std::is_same_v<T, uint16_t>) {
      value = src.ReadUnsignedInt16();
    } else if constexpr (std::is_same_v<T, int32_t>) {
      value = src.ReadSignedInt32();
    } else if constexpr (std::is_same_v<T, uint32_t>) {
      value = src.ReadUnsignedInt32();
    } else if constexpr (std::is_same_v<T, int64_t>) {
      value = src.ReadSignedInt64();
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      value = src.ReadUnsignedInt64();
    } else if constexpr (std::is_same_v<T, float>) {
      value = src.ReadFloat();
    } else if constexpr (std::is_same_v<T, double>) {
      value = src.ReadDouble();
    } else if constexpr (kIsString<T>) {
      detail::ReadString(src, value);
    } else if constexpr (kIsOptional<T>) {
      value.reset();
      const bool present = src.OnNullableBegin();
      if (present) {
        auto& v = value.emplace();
        self(v, src);
      }
      src.OnNullableEnd();
    } else if constexpr (kIsUniquePtr<T>) {
      value.reset();
      const bool present = src.OnNullableBegin();
      if (present) {
        value = std::make_unique<typename T::element_type>();
        self(*value, src);
      }
      src.OnNullableEnd();
    } else if constexpr (kIsSharedPtr<T>) {
      value.reset();
      const bool present = src.OnNullableBegin();
      if (present) {
        value = std::make_shared<typename T::element_type>();
        self(*value, src);
      }
      src.OnNullableEnd();
    } else if constexpr (kIsVariant<T>) {
      const size_t index = src.ReadUnsignedInt64();
      const bool matched =
        detail::ReadVariantIndex(value, index, [&](auto& v) { self(v, src); });
      if (!matched) [[unlikely]] {
        SDB_THROW(ERROR_BAD_PARAMETER, "Variant index ", index,
                  " out of range");
      }
    } else if constexpr (!std::is_empty_v<T>) {
      const size_t count = src.OnListBegin();
      auto check_size = [&](size_t expected) {
        if (count != expected) {
          SDB_THROW(ERROR_BAD_PARAMETER, "Failed to read: serialized data has ",
                    count, " element(s), expected ", expected);
        }
      };
      if constexpr (std::is_aggregate_v<T>) {
        check_size(boost::pfr::tuple_size_v<T>);
      } else if constexpr (kIsTuple<T>) {
        check_size(std::tuple_size_v<T>);
      } else if constexpr (kIsArray<T>) {
        check_size(value.size());
      }

      size_t element_idx = 0;
      try {
        if constexpr (std::is_aggregate_v<T>) {
          boost::pfr::for_each_field(value, [&](auto& v) {
            self(v, src);
            ++element_idx;
          });
        } else if constexpr (kIsTuple<T>) {
          std::apply(
            [&](auto&&... args) { ((self(args, src), ++element_idx), ...); },
            value);
        } else if constexpr (kIsArray<T>) {
          for (auto& v : value) {
            self(v, src);
            ++element_idx;
          }
        } else if constexpr (detail::IsSet<T>) {
          value.clear();
          if constexpr (detail::HasReserve<T>) {
            value.reserve(count);
          }
          typename T::value_type v;
          for (; element_idx < count; ++element_idx) {
            self(v, src);
            [[maybe_unused]] bool emplaced = value.emplace(std::move(v)).second;
            SDB_ASSERT(emplaced);
          }
        } else if constexpr (detail::IsMap<T>) {
          value.clear();
          if constexpr (detail::HasReserve<T>) {
            value.reserve(count);
          }
          std::pair<typename T::key_type, typename T::mapped_type> v;
          for (; element_idx < count; ++element_idx) {
            self(v, src);
            [[maybe_unused]] bool emplaced = value.emplace(std::move(v)).second;
            SDB_ASSERT(emplaced);
          }
        } else if constexpr (detail::IsContainer<T> && detail::HasResize<T>) {
          value.resize(count);
          for (auto& v : value) {
            self(v, src);
            ++element_idx;
          }
        } else if constexpr (detail::IsContainer<T>) {
          value.clear();
          if constexpr (detail::HasReserve<T>) {
            value.reserve(count);
          }
          for (; element_idx < count; ++element_idx) {
            self(value.emplace_back(), src);
          }
        }
      } catch (const std::exception& e) {
        src.OnListEnd();
        SDB_THROW(ERROR_BAD_PARAMETER, "Failed to read element ", element_idx,
                  ": ", e.what());
      } catch (...) {
        src.OnListEnd();
        SDB_THROW(ERROR_BAD_PARAMETER, "Failed to read element ", element_idx,
                  ": unspecified error");
      }
      src.OnListEnd();
    } else {
      static_assert(std::is_empty_v<T>, "Other types are not supported");
      [[maybe_unused]] const bool present = src.OnNullableBegin();
      src.OnNullableEnd();
    }
  };

  impl(out, src);
}

template<typename Sink, typename U, typename A = detail::Empty>
void WriteTuple(Sink& b, const U& in, const A& arg = {}) {
  // TODO(gnusi): reserve some space based on layout?
  auto impl = [&]<typename T>(this auto& self, const T& value) -> void {
    static_assert(kIsWritable<T, TupleFormat, A, Sink>, "Type is not writable");

    if constexpr (detail::HasWriteOverload<Sink, TupleFormat, T, A>) {
      SerdeWrite(detail::Context<TupleFormat, Sink*, A, true>{&b, arg}, value);
    } else if constexpr (detail::HasMemberSerialize<Sink, T>) {
      b.OnObjectBegin();
      value.Serialize(b);
      b.OnObjectEnd();
    } else if constexpr (kIsNullable<T>) {
      bool present = false;
      if constexpr (!std::is_empty_v<T>) {
        present = static_cast<bool>(value);
      }
      b.OnNullableBegin(present);
      if (present) {
        if constexpr (!std::is_empty_v<T>) {
          self(*value);
        }
      }
      b.OnNullableEnd();
    } else if constexpr (kIsVariant<T>) {
      // Index discriminator: reordering persisted alternatives breaks old data.
      self(value.index());
      std::visit([&](const auto& v) { self(v); }, value);
    } else if constexpr (detail::IsRange<T>) {
      const size_t count = std::ranges::size(value);
      b.OnListBegin(count);
      for (const auto& v : value) {
        self(v);
      }
      b.OnListEnd();
    } else if constexpr (kIsTuple<T>) {
      b.OnListBegin(std::tuple_size_v<T>);
      std::apply([&](const auto&... args) { (self(args), ...); }, value);
      b.OnListEnd();
    } else if constexpr (std::is_enum_v<T>) {
      const auto raw = std::to_underlying(value);
      if (!magic_enum::enum_cast<T>(raw)) [[unlikely]] {
        detail::ThrowInvalidEnum<T>(static_cast<int64_t>(raw));
      }
      self(raw);
    } else if constexpr (std::is_aggregate_v<T>) {
      // Positional, count-checked only: reordering same-typed fields is silent.
      // Append new fields at the end.
      constexpr size_t kCount = boost::pfr::tuple_size_v<T>;
      b.OnListBegin(kCount);
      boost::pfr::for_each_field(value,
                                 [&](const auto& field_v) { self(field_v); });
      b.OnListEnd();
    } else if constexpr (std::is_same_v<T, bool>) {
      b.WriteValue(value);
    } else if constexpr (detail::kIsSerializableArithmetic<T>) {
      b.WriteValue(value);
    } else if constexpr (kIsString<T>) {
      detail::WriteString(
        b, std::string_view{reinterpret_cast<const char*>(value.data()),
                            value.size()});
    } else {
      static_assert(false, "Other types are not supported");
    }
  };

  impl(in);
}

template<typename Source, typename U, typename A = detail::Empty>
void ReadObject(Source& src, U& out, const A& arg = {}) {
  // Member Serialize/Deserialize is TupleFormat (duckdb)-only, not dispatched
  // here.
  auto impl = [&]<typename T>(this auto& self, T& value) -> void {
    static_assert(!std::is_const_v<T>);

    if constexpr (detail::HasSourceReadOverload<Source, ObjectFormat, T, A>) {
      SerdeRead(detail::Context<ObjectFormat, Source*, A, false>{&src, arg},
                value);
    } else if constexpr (std::is_same_v<T, bool>) {
      value = src.ReadBool();
    } else if constexpr (std::is_enum_v<T>) {
      const std::string name = src.ReadString();
      auto cast = magic_enum::enum_cast<T>(name, magic_enum::case_insensitive);
      if (!cast) [[unlikely]] {
        SDB_THROW(ERROR_BAD_PARAMETER, "Invalid enum value '", name, "' for ",
                  magic_enum::enum_type_name<T>());
      }
      value = *cast;
    } else if constexpr (detail::kIsSerializableArithmetic<T>) {
      if constexpr (std::is_floating_point_v<T>) {
        value = static_cast<T>(src.ReadDouble());
      } else if constexpr (std::is_signed_v<T>) {
        value = static_cast<T>(src.ReadSignedInt64());
      } else {
        value = static_cast<T>(src.ReadUnsignedInt64());
      }
    } else if constexpr (kIsString<T>) {
      detail::ReadString(src, value);
    } else if constexpr (kIsOptional<T>) {
      value.reset();
      if (src.OnNullableBegin()) {
        self(value.emplace());
      }
    } else if constexpr (kIsUniquePtr<T>) {
      value.reset();
      if (src.OnNullableBegin()) {
        value = std::make_unique<typename T::element_type>();
        self(*value);
      }
    } else if constexpr (kIsSharedPtr<T>) {
      value.reset();
      if (src.OnNullableBegin()) {
        value = std::make_shared<typename T::element_type>();
        self(*value);
      }
    } else if constexpr (kIsVariant<T>) {
      if (!src.OnListBegin()) [[unlikely]] {
        SDB_THROW(ERROR_BAD_PARAMETER, "JSON: expected variant [index, value]");
      }
      const size_t index = src.ReadUnsignedInt64();
      if (!src.NextListElement()) [[unlikely]] {
        SDB_THROW(ERROR_BAD_PARAMETER, "JSON: variant missing its value");
      }
      const bool matched =
        detail::ReadVariantIndex(value, index, [&](auto& v) { self(v); });
      if (!matched) [[unlikely]] {
        SDB_THROW(ERROR_BAD_PARAMETER, "Variant index ", index,
                  " out of range");
      }
      src.OnScopeEnd();
    } else if constexpr (detail::IsMap<T>) {
      using Key = typename T::key_type;
      value.clear();
      src.ForEachObjectField([&](std::string_view name) {
        Key key{};
        if constexpr (kIsString<Key>) {
          if constexpr (detail::HasAssign<Key>) {
            key.assign(name);
          } else {
            key = Key{name};
          }
        } else if constexpr (std::is_enum_v<Key>) {
          auto cast =
            magic_enum::enum_cast<Key>(name, magic_enum::case_insensitive);
          if (!cast) [[unlikely]] {
            SDB_THROW(ERROR_BAD_PARAMETER, "Invalid enum map key '", name,
                      "' for ", magic_enum::enum_type_name<Key>());
          }
          key = *cast;
        } else {
          static_assert(std::is_integral_v<Key>,
                        "ReadObject: only string/enum/integral map keys are "
                        "supported");
          if (!absl::SimpleAtoi(name, &key)) [[unlikely]] {
            SDB_THROW(ERROR_BAD_PARAMETER, "Failed to parse integral map key '",
                      name, "'");
          }
        }
        typename T::mapped_type val{};
        self(val);
        [[maybe_unused]] const bool emplaced =
          value.emplace(std::move(key), std::move(val)).second;
        SDB_ASSERT(emplaced);
      });
    } else if constexpr (kIsTuple<T>) {
      constexpr size_t kSize = std::tuple_size_v<T>;
      const bool nonempty = src.OnListBegin();
      bool more = nonempty;
      std::apply(
        [&](auto&... elems) {
          (
            [&](auto& elem) {
              if (more) {
                self(elem);
                more = src.NextListElement();
              }
            }(elems),
            ...);
        },
        value);
      if (nonempty) {
        src.OnScopeEnd();
      }
      if (more) [[unlikely]] {
        SDB_THROW(ERROR_BAD_PARAMETER,
                  "JSON: tuple has more elements than the fixed size ", kSize);
      }
    } else if constexpr (kIsArray<T>) {
      auto it = value.begin();
      const auto last = value.end();
      src.ForEachListElement([&] {
        if (it == last) [[unlikely]] {
          SDB_THROW(ERROR_BAD_PARAMETER,
                    "JSON: array has more elements than the fixed size ",
                    value.size());
        }
        self(*it++);
      });
    } else if constexpr (std::is_aggregate_v<T>) {
      // Single pass: match each field to a member by name (constexpr compare,
      // not a per-member JSON lookup). Missing default, unknown skipped.
      src.ForEachObjectField([&](std::string_view name) {
        bool matched = false;
        boost::pfr::for_each_field_with_name(
          value, [&](std::string_view field_name, auto& fv) {
            if (!matched && field_name == name) {
              self(fv);
              matched = true;
            }
          });
      });
    } else if constexpr (detail::IsSet<T>) {
      value.clear();
      src.ForEachListElement([&] {
        typename T::value_type v;
        self(v);
        [[maybe_unused]] const bool emplaced =
          value.emplace(std::move(v)).second;
        SDB_ASSERT(emplaced);
      });
    } else if constexpr (detail::IsContainer<T>) {
      value.clear();
      src.ForEachListElement([&] { self(value.emplace_back()); });
    } else {
      static_assert(sizeof(T) == 0, "ReadObject: unsupported field type");
    }
  };

  impl(out);
}

template<typename Sink, typename U, typename A = detail::Empty>
void WriteObject(Sink& b, const U& in, const A& arg = {}) {
  // An object member is omitted when it has nothing to emit: an empty struct
  // or a null optional/pointer chain.
  auto is_empty = [](this auto&& is_empty, const auto& v) -> bool {
    using V = std::remove_cvref_t<decltype(v)>;
    if constexpr (std::is_empty_v<V>) {
      return true;
    } else if constexpr (kIsOptional<V> || kIsPointer<V>) {
      return !v || is_empty(*v);
    } else {
      return false;
    }
  };

  auto impl = [&]<typename T>(this auto& self, const T& value) -> void {
    static_assert(kIsWritable<T, ObjectFormat, A, Sink>,
                  "Type is not writable");

    if constexpr (detail::HasWriteOverload<Sink, ObjectFormat, T, A>) {
      SerdeWrite(detail::Context<ObjectFormat, Sink*, A, true>{&b, arg}, value);
    } else if constexpr (kIsNullable<T>) {
      if constexpr (!std::is_empty_v<T>) {
        if (value) {
          self(*value);
          return;
        }
      }
      b.WriteNull();
    } else if constexpr (kIsVariant<T>) {
      // [index, value]; see WriteTuple re: index-based discriminator fragility.
      b.OnListBegin();
      self(value.index());
      b.OnSeparator();
      std::visit([&](const auto& v) { self(v); }, value);
      b.OnListEnd();
    } else if constexpr (detail::IsMap<T>) {
      b.OnObjectBegin();
      bool first = true;
      for (auto& [k, v] : value) {
        if (!std::exchange(first, false)) {
          b.OnSeparator();
        }
        using Key = std::remove_cvref_t<decltype(k)>;
        if constexpr (kIsString<Key>) {
          b.OnPropertyBegin(std::string_view{k});
        } else if constexpr (std::is_enum_v<Key>) {
          b.OnPropertyBegin(magic_enum::enum_name(k));
        } else {
          static_assert(std::is_integral_v<Key>,
                        "WriteObject: only string/enum/integral map keys are "
                        "supported");
          b.OnPropertyBegin(absl::AlphaNum(k).Piece());
        }
        self(v);
      }
      b.OnObjectEnd();
    } else if constexpr (detail::IsRange<T>) {
      b.OnListBegin();
      bool first = true;
      for (const auto& v : value) {
        if (!std::exchange(first, false)) {
          b.OnSeparator();
        }
        self(v);
      }
      b.OnListEnd();
    } else if constexpr (kIsTuple<T>) {
      b.OnListBegin();
      bool first = true;
      std::apply(
        [&](const auto&... args) {
          auto emit = [&](const auto& arg_value) {
            if (!std::exchange(first, false)) {
              b.OnSeparator();
            }
            self(arg_value);
          };
          (emit(args), ...);
        },
        value);
      b.OnListEnd();
    } else if constexpr (std::is_enum_v<T>) {
      const auto name = magic_enum::enum_name(value);
      if (name.empty()) [[unlikely]] {
        detail::ThrowInvalidEnum<T>(
          static_cast<int64_t>(std::to_underlying(value)));
      }
      detail::WriteString(b, name);
    } else if constexpr (std::is_aggregate_v<T>) {
      b.OnObjectBegin();
      bool first = true;
      boost::pfr::for_each_field_with_name(
        value, [&]<typename FV>(std::string_view name, const FV& fv) {
          if (is_empty(fv)) {
            return;
          }
          if (!std::exchange(first, false)) {
            b.OnSeparator();
          }
          b.OnPropertyBegin(name);
          self(fv);
        });
      b.OnObjectEnd();
    } else if constexpr (std::is_same_v<T, bool>) {
      b.WriteValue(value);
    } else if constexpr (std::is_arithmetic_v<T>) {
      static_assert(detail::kIsSerializableArithmetic<T>);
      if constexpr (std::is_floating_point_v<T>) {
        b.WriteValue(static_cast<double>(value));
      } else if constexpr (std::is_signed_v<T>) {
        b.WriteValue(static_cast<int64_t>(value));
      } else {
        b.WriteValue(static_cast<uint64_t>(value));
      }
    } else if constexpr (kIsString<T>) {
      detail::WriteString(
        b, std::string_view{reinterpret_cast<const char*>(value.data()),
                            value.size()});
    } else {
      static_assert(false, "Other types are not supported");
    }
  };

  impl(in);
}

}  // namespace sdb::basics
