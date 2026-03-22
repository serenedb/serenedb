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

#include <exception>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

#include "vpack/builder.h"
#include "vpack/iterator.h"
#include "vpack/slice.h"
#include "vpack/string.h"

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

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/type_traits.h"

namespace vpack {
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

template<typename T>
struct Field {
  using Type = T;

  template<typename... Args>
  // clang-format off
    requires(requires { T{std::declval<Args&&>()...}; })
  // clang-format on
  constexpr Field(Args&&... args) : _value{std::forward<Args>(args)...} {}

  constexpr auto& operator*(this auto& self) { return self._value; }
  constexpr auto* operator->(this auto& self) {
    return std::addressof(self._value);
  }

  constexpr bool operator==(const Field& rhs) const = default;
  constexpr auto operator<=>(const Field& rhs) const = default;

 private:
  [[no_unique_address]] T _value;
};

template<typename T>
struct Embedded : Field<T> {
  using IsEmbedded = void;

  using Field<T>::Field;
};

template<typename T>
Embedded(T) -> Embedded<T>;

template<typename T>
struct Skip : Field<T> {
  using IsSkip = void;
  using IsOptional = void;

  using Field<T>::Field;
};

template<detail::FixedString S, typename T>
struct Name : Field<T> {
  static constexpr std::string_view kName = S;

  using Field<T>::Field;
};

template<typename T>
struct Optional : Field<T> {
  using IsOptional = void;

  using Field<T>::Field;
};

template<detail::FixedString S, typename T>
struct NameOptional : Name<S, T> {
  using IsOptional = void;

  using Name<S, T>::Name;
};

struct ReadOptions {
  // Skip unknown fields
  bool skip_unknown = true;
  // Ensure every mandatory field is specified
  bool strict = true;
};

namespace detail {

template<typename F, typename V, typename A>
class Context {
 public:
  using Format = F;
  using Arg = A;

  explicit Context(V vpack, const Arg& arg) : _vpack{vpack}, _arg{&arg} {}

  decltype(auto) vpack() const {
    if constexpr (std::is_pointer_v<V>) {
      return *_vpack;
    } else {
      return _vpack;
    }
  }

  auto& arg() const noexcept { return *_arg; }

 private:
  static_assert(sizeof(V) == sizeof(void*));
  V _vpack;
  const Arg* _arg;
};

template<typename Lambda, int = (Lambda{}(), 0)>
constexpr bool IsConstexpr(Lambda) {
  return true;
}
constexpr bool IsConstexpr(...) { return false; }

template<typename T>
inline constexpr bool kIsConstexprConstructible =
  IsConstexpr([]() { return T{}; });

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
concept IsField = std::is_base_of_v<Field<typename T::Type>, T>;

template<typename T>
concept IsEmbeddedField = IsField<T> && requires { typename T::IsEmbedded; };

template<typename T>
concept IsNameField = IsField<T> && requires { T::kName; };

template<typename T>
concept IsOptionalField = IsField<T> && requires { typename T::IsOptional; };

template<typename T>
concept IsSkipField = IsField<T> && requires { typename T::IsSkip; };

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

template<typename K, typename V, typename A>
concept HasWriteHook = requires(K&& k, V&& v, vpack::Builder& b, const A& arg) {
  VPackWriteHook(Context<ObjectFormat, vpack::Builder*, A>{&b, arg},
                 std::forward<K>(k), std::forward<V>(v));
};

template<typename F, typename T, typename A>
concept HasWriteOverload = requires(T&& t, vpack::Builder& b, const A& arg) {
  VPackWrite(Context<F, vpack::Builder*, A>{&b, arg}, t);
};

template<typename F, typename T, typename A>
concept HasReadOverload = requires(T& t, vpack::Slice s, const A& arg) {
  VPackRead(Context<F, vpack::Slice, A>{s, arg}, t);
};

struct ILoader {
  virtual ~ILoader() = default;
  virtual bool load(void* value, vpack::Slice slice, ReadOptions options,
                    const void* arg) const = 0;
};

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
inline constexpr bool kIsVPackBuilder = std::is_same_v<T, vpack::Builder>;

template<typename T>
inline constexpr bool kIsVPackString = std::is_same_v<T, vpack::String>;

template<typename T>
inline constexpr bool kIsVPackSlice = std::is_same_v<T, vpack::Slice>;

template<typename T>
inline constexpr bool kIsSupportedBuiltIn =
  std::is_enum_v<T> || detail::IsField<T> || detail::IsContainer<T> ||
  detail::IsRange<T> || detail::IsMap<T> || detail::IsSet<T> || kIsArray<T> ||
  kIsTuple<T> || kIsNullable<T> || kIsPrimitive<T> || kIsVPackBuilder<T> ||
  kIsVPackString<T> || kIsVPackSlice<T> || std::is_aggregate_v<T>;

template<typename T, typename F, typename A = detail::Empty>
inline constexpr bool kIsWritable =
  kIsSupportedBuiltIn<T> || kIsVariant<T> || detail::HasWriteOverload<F, T, A>;

template<typename T, typename F, typename A = detail::Empty>
inline constexpr bool kIsReadable =
  kIsSupportedBuiltIn<T> || detail::HasReadOverload<F, T, A>;

template<typename T>
struct Nullable : Field<T> {
  static_assert(kIsNullable<T>);

  using Field<T>::Field;
};

template<typename T, bool AsString>
auto ReadEnum(vpack::Slice in) {
  const auto value = [&]() {
    if constexpr (AsString) {
      return magic_enum::enum_cast<T>(in.stringView(),
                                      magic_enum::case_insensitive);
    } else {
      return magic_enum::enum_cast<T>(
        in.getNumber<std::underlying_type_t<T>, true>());
    }
  }();

  if (!value) [[unlikely]] {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Invalid enum value '", value, "' for ",
              magic_enum::enum_type_name<T>());
  }

  return *value;
}

template<typename T, typename A>
void WriteKey(const T& v, vpack::Builder& b, const A& arg) {
  if constexpr (detail::HasWriteOverload<ObjectFormat, T, A>) {
    VPackWrite(detail::Context<ObjectFormat, vpack::Builder*, A>{&b, arg}, v);
  } else if constexpr (kIsString<T>) {
    b.add(v);
  } else if constexpr (std::is_enum_v<T>) {
    b.add(magic_enum::enum_name(v));
  } else if constexpr (std::is_integral_v<T>) {
    b.add(absl::AlphaNum(v).Piece());
  } else {
    static_assert(std::is_same_v<T, detail::Empty>,
                  "Key type is not supported");
  }
}

template<typename T, typename A>
void ReadKey(T& value, vpack::Slice slice, const A& arg) {
  if constexpr (detail::HasReadOverload<ObjectFormat, T, A>) {
    VPackRead(detail::Context<ObjectFormat, vpack::Slice, A>{slice, arg},
              value);
  } else if constexpr (kIsString<T>) {
    if constexpr (detail::HasAssign<T>) {
      value.assign(slice.stringView());
    } else {
      value = T{slice.stringView()};
    }
  } else if constexpr (std::is_enum_v<T>) {
    value = readEnum<T, true>(slice);
  } else {
    static_assert(std::is_integral_v<T>,
                  "Non-integral dictionary keys are not supported");

    if (!absl::SimpleAtoi(slice.stringView(), &value)) {
      SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Failed to parse map key");
    }
  }
}

namespace detail {

template<typename U, typename Arg>
struct Loader : ILoader {
  bool load(void* p, vpack::Slice slice, ReadOptions options,
            const void* arg) const final;
};

template<typename T, typename Arg>
inline constexpr Loader<T, Arg> kLoader;

template<typename U>
constexpr bool isOptional() {  // NOLINT
  if constexpr (IsField<U>) {
    if constexpr (IsOptionalField<U>) {
      return true;
    } else {
      return kIsNullable<typename U::Type>;
    }
  } else {
    return kIsNullable<U>;
  }
};

template<typename T, typename Arg, typename F>
constexpr auto MakeField(F&& f) noexcept {
  auto align_up = [](size_t size, size_t alignment) {
    return (size + alignment - 1) & (0 - alignment);
  };

  std::array<std::pair<std::string_view, std::pair<size_t, const ILoader*>>,
             boost::pfr::tuple_size_v<T>>
    offsets;
  auto begin = offsets.begin();

  size_t count = 0;
  size_t offset = 0;
  boost::pfr::for_each_field_with_name(
    T{}, [&]<typename U>(std::string_view name, const U&) {
      count += size_t{!isOptional<U>()};

      if constexpr (detail::IsNameField<U>) {
        name = U::kName;
      }
      begin->first = name;

      if constexpr (std::is_empty_v<U>) {
        // We assume that empty members are always marked with
        // [[no_unique_address]]
        begin->second = {0, &kLoader<U, Arg>};
      } else {
        const auto member_offset = align_up(offset, alignof(U));
        begin->second = {member_offset, &kLoader<U, Arg>};
        offset = member_offset + align_up(sizeof(U), alignof(U));
      }

      ++begin;
    });

  return f(offsets, count);
}

// template<typename T, typename Arg>
// constexpr auto kFieldMap = makeField<T, Arg>([](auto& offsets, auto count) {
//   return std::pair{frozen::make_unordered_map(offsets), count};
// });

template<typename T, typename Arg>
const auto& MakeFieldMap() {
  // I disabled compile time overload for now
  // First of all I don't see a lot of sense in super-duper fast deserialize of
  // ddl from user
  // Secondly and more important I think frozen is works slower than abseil
  // Maybe it can make sense in future if we will use this serialization
  // somewhere there it's matter: replication, metadata storage, etc.
  // Maybe we can use TrivialBiMap?
  // if constexpr (kIsConstexprConstructible<T>) {
  //   return kFieldMap<T, Arg>;
  // } else {
  static const auto kFields = MakeField<T, Arg>([](auto& offsets, auto count) {
    return std::pair{
      sdb::containers::FlatHashMap<std::string_view,
                                   std::pair<size_t, const ILoader*>>{
        offsets.begin(), offsets.end()},
      count};
  });
  return kFields;
  // }
}

template<typename T>
void CheckFieldMap(const auto& fields, const T& value) {
#ifdef SDB_DEV
  const auto* base = reinterpret_cast<const char*>(std::addressof(value));
  boost::pfr::for_each_field_with_name(
    value, [&]<typename U>(std::string_view name, const U& v) {
      if constexpr (detail::IsNameField<U>) {
        name = U::kName;
      }
      const auto it = fields.find(name);
      SDB_ASSERT(it != fields.end());
      SDB_ASSERT(base + it->second.first ==
                 reinterpret_cast<const char*>(std::addressof(v)));
    });
#endif
}

template<typename U, typename Arg>
bool Loader<U, Arg>::load(void* p, vpack::Slice slice, ReadOptions options,
                          const void* arg) const {
  static_assert(kIsReadable<U, ObjectFormat, Arg>, "Type is not readable");

  auto impl = [options, arg]<typename T>(this auto& self, T& value,
                                         vpack::Slice slice) {
    static_assert(!detail::IsEmbeddedField<T>,
                  "Loader for embedded fields is not implemented");

    if constexpr (std::is_empty_v<T>) {
      return;
    } else if constexpr (IsField<T>) {
      if constexpr (!detail::IsSkipField<T>) {
        self(*value, slice);
      }
    } else if constexpr (HasReadOverload<ObjectFormat, T, Arg>) {
      VPackRead(
        Context<ObjectFormat, vpack::Slice, Arg>{slice,
                                                 *static_cast<const Arg*>(arg)},
        value);
    } else if constexpr (std::is_same_v<T, bool>) {
      value = slice.getBool();
    } else if constexpr (std::is_enum_v<T>) {
      auto raw = slice.stringView();
      auto v = magic_enum::enum_cast<T>(raw, magic_enum::case_insensitive);

      if (!v) [[unlikely]] {
        SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Invalid enum value ", raw, " for ",
                  magic_enum::enum_type_name<T>());
      }

      value = *v;
    } else if constexpr (std::is_arithmetic_v<T>) {
      value = slice.getNumber<T, true>();
    } else if constexpr (kIsString<T>) {
      if constexpr (HasAssign<T>) {
        value.assign(slice.stringView());
      } else {
        value = T{slice.stringView()};
      }
    } else if constexpr (kIsOptional<T>) {
      value.reset();
      if (!slice.isNull()) {
        auto& v = value.emplace();
        return self(v, slice);
      }
    } else if constexpr (kIsUniquePtr<T>) {
      value.reset();
      if (!slice.isNull()) {
        value = std::make_unique<typename T::element_type>();
        return self(*value, slice);
      }
    } else if constexpr (kIsSharedPtr<T>) {
      value.reset();
      if (!slice.isNull()) {
        value = std::make_shared<typename T::element_type>();
        return self(*value, slice);
      }
    } else if constexpr (kIsVPackBuilder<T>) {
      value = vpack::Builder{slice};
    } else if constexpr (kIsVPackString<T> || kIsVPackSlice<T>) {
      value = slice;
    } else if constexpr (kIsTuple<T> || kIsArray<T> || IsSet<T> ||
                         IsContainer<T>) {
      vpack::ArrayIterator it{slice};
      auto check_size = [&](size_t expected) {
        if (it.size() != expected) {
          SDB_THROW(sdb::ERROR_BAD_PARAMETER,
                    "Failed to parse array due to incompatible format, "
                    "number of attributes ",
                    it.size(), " != ", expected);
        }
      };
      if constexpr (kIsTuple<T>) {
        check_size(std::tuple_size_v<T>);
      } else if constexpr (kIsArray<T>) {
        check_size(value.size());
      }

      try {
        if constexpr (kIsTuple<T>) {
          std::apply(
            [&](auto&&... args) { ((self(args, *it), it.next()), ...); },
            value);
        } else if constexpr (kIsArray<T>) {
          check_size(value.size());
          for (auto& v : value) {
            self(v, *it);
            it.next();
          }
        } else if constexpr (IsSet<T>) {
          value.clear();

          if constexpr (HasReserve<T>) {
            value.reserve(it.size());
          }

          typename T::value_type v;
          for (; it.valid(); it.next()) {
            self(v, *it);

            [[maybe_unused]] bool emplaced = value.emplace(std::move(v)).second;
            SDB_ASSERT(emplaced);
          }
        } else {
          static_assert(IsContainer<T>, "Other types are not supported");
          value.clear();

          if constexpr (HasReserve<T>) {
            value.reserve(it.size());
          }

          for (; it.valid(); it.next()) {
            self(value.emplace_back(), *it);
          }
        }
      } catch (const std::exception& e) {
        SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Failed to parse value at '",
                  it.index(), "': ", e.what());
      } catch (...) {
        SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Failed to parse value at '",
                  it.index(), "' due to unspecified error");
      }
    } else {
      static_assert(std::is_aggregate_v<T> || IsMap<T>);
      vpack::ObjectIterator it{slice, true};
      auto wrapper = [](auto key, auto&& call) {
        try {
          call();
        } catch (const std::exception& e) {
          SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Failed to parse attribute '",
                    key, "': ", e.what());
        } catch (...) {
          SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Failed to parse attribute '",
                    key, "' due to unspecified error");
        }
      };

      if constexpr (IsMap<T>) {
        value.clear();

        if constexpr (HasReserve<T>) {
          value.reserve(it.size());
        }

        typename T::key_type key;
        typename T::mapped_type val;
        for (auto [k, v] : it) {
          // TODO(mbkkt) node type?
          ReadKey(key, k, arg);
          wrapper(k.stringView(), [&] { self(val, v); });

          [[maybe_unused]] bool emplaced =
            value.emplace(std::move(key), std::move(val)).second;
          SDB_ASSERT(emplaced);
        }
      } else {
        const auto& fields = MakeFieldMap<T, Arg>();
        CheckFieldMap<T>(fields.first, value);

        size_t count = 0;
        for (auto [k, v] : it) {
          const auto key = k.stringView();
          const auto it = fields.first.find(key);

          if (it == fields.first.end()) {
            if (!options.skip_unknown) {
              SDB_THROW(sdb::ERROR_BAD_PARAMETER,
                        "Found unexpected attribute '", key, "'");
            }
            continue;
          }

          const auto [offset, loader] = it->second;
          auto* val = reinterpret_cast<char*>(std::addressof(value)) + offset;

          wrapper(
            key, [&] { count += size_t{!loader->load(val, v, options, arg)}; });
        }

        if (options.strict && count < fields.second) {
          // TODO(gnusi): report mandatory fields
          SDB_THROW(sdb::ERROR_BAD_PARAMETER,
                    "Incompatible format, object should have at least ",
                    fields.second, " mandatory attributes");
        }
      }
    }
  };
  auto& value = *reinterpret_cast<U*>(p);
  impl(value, slice);
  return isOptional<U>();
}

}  // namespace detail

template<typename U, typename A = detail::Empty>
void ReadTuple(vpack::Slice in, U& out, const A& arg = {}) {
  auto impl = [&arg]<typename T>(this auto& self, T& value,
                                 vpack::Slice slice) {
    static_assert(kIsReadable<T, TupleFormat, A>, "Type is not readable");
    static_assert(!detail::IsSkipField<T>,
                  "Skip not really supported for Tuple");

    if constexpr (detail::IsField<T>) {
      self(*value, slice);
    } else if constexpr (detail::HasReadOverload<TupleFormat, T, A>) {
      VPackRead(detail::Context<TupleFormat, vpack::Slice, A>{slice, arg},
                value);
    } else if constexpr (std::is_same_v<T, bool>) {
      value = slice.getBool();
    } else if constexpr (std::is_enum_v<T>) {
      auto raw = slice.getNumber<std::underlying_type_t<T>, true>();
      auto v = magic_enum::enum_cast<T>(raw);

      if (!v) [[unlikely]] {
        SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Invalid enum value ", raw, " for ",
                  magic_enum::enum_type_name<T>());
      }

      value = *v;
    } else if constexpr (std::is_arithmetic_v<T>) {
      value = slice.getNumber<T, true>();
    } else if constexpr (kIsString<T>) {
      if constexpr (detail::HasAssign<T>) {
        value.assign(slice.stringView());
      } else {
        value = T{slice.stringView()};
      }
    } else if constexpr (kIsOptional<T>) {
      value.reset();
      if (!slice.isNull()) {
        auto& v = value.emplace();
        return self(v, slice);
      }
    } else if constexpr (kIsUniquePtr<T>) {
      value.reset();
      if (!slice.isNull()) {
        value = std::make_unique<typename T::element_type>();
        return self(*value, slice);
      }
    } else if constexpr (kIsVPackBuilder<T>) {
      value = vpack::Builder{slice};
    } else if constexpr (kIsVPackString<T> || kIsVPackSlice<T>) {
      value = slice;
    } else if constexpr (kIsSharedPtr<T>) {
      value.reset();
      if (!slice.isNull()) {
        value = std::make_shared<typename T::element_type>();
        return self(*value, slice);
      }
    } else if constexpr (!std::is_empty_v<T>) {
      vpack::ArrayIterator it{slice};
      auto check_size = [&](size_t expected) {
        if (it.size() != expected) {
          SDB_THROW(sdb::ERROR_BAD_PARAMETER,
                    "Failed to parse array due to incompatible format, "
                    "number of attributes ",
                    it.size(), " != ", expected);
        }
      };
      if constexpr (std::is_aggregate_v<T>) {
        check_size(boost::pfr::tuple_size_v<T>);
      } else if constexpr (kIsTuple<T>) {
        check_size(std::tuple_size_v<T>);
      } else if constexpr (kIsArray<T>) {
        check_size(value.size());
      }

      try {
        if constexpr (std::is_aggregate_v<T>) {
          boost::pfr::for_each_field(value, [&](auto& v) {
            self(v, *it);
            it.next();
          });
        } else if constexpr (kIsTuple<T>) {
          std::apply(
            [&](auto&&... args) { ((self(args, *it), it.next()), ...); },
            value);
        } else if constexpr (kIsArray<T>) {
          for (auto& v : value) {
            self(v, *it);
            it.next();
          }
        } else if constexpr (detail::IsSet<T>) {
          value.clear();

          if constexpr (detail::HasReserve<T>) {
            value.reserve(it.size());
          }

          typename T::value_type v;
          for (; it.valid(); it.next()) {
            self(v, *it);

            [[maybe_unused]] bool emplaced = value.emplace(std::move(v)).second;
            SDB_ASSERT(emplaced);
          }
        } else if constexpr (detail::IsMap<T>) {
          value.clear();

          if constexpr (detail::HasReserve<T>) {
            value.reserve(it.size());
          }

          // TODO(mbkkt) node type?
          std::pair<typename T::key_type, typename T::mapped_type> v;
          for (; it.valid(); it.next()) {
            self(v, *it);

            [[maybe_unused]] bool emplaced = value.emplace(std::move(v)).second;
            SDB_ASSERT(emplaced);
          }
        } else if constexpr (detail::IsContainer<T>) {
          value.clear();

          if constexpr (detail::HasReserve<T>) {
            value.reserve(it.size());
          }

          for (; it.valid(); it.next()) {
            self(value.emplace_back(), *it);
          }
        }
      } catch (const std::exception& e) {
        SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Failed to parse value at '",
                  it.index(), "': ", e.what());
      } catch (...) {
        SDB_THROW(sdb::ERROR_BAD_PARAMETER, "Failed to parse value at '",
                  it.index(), "' due to unspecified error");
      }
    } else {
      static_assert(std::is_empty_v<T>, "Other types are not supported");
    }
  };

  impl(out, in);
}

template<typename U, typename A = detail::Empty>
void WriteTuple(vpack::Builder& b, const U& in, const A& arg = {}) {
  // TODO(gnusi): reserve some space based on layout?
  auto impl = [&]<typename T>(this auto& self, const T& value) {
    static_assert(kIsWritable<T, TupleFormat, A>, "Type is not writable");
    static_assert(!detail::IsSkipField<T>,
                  "Skip not really supported for Tuple");

    if constexpr (detail::HasWriteOverload<TupleFormat, T, A>) {
      VPackWrite(detail::Context<TupleFormat, vpack::Builder*, A>{&b, arg},
                 value);
    } else if constexpr (kIsNullable<T>) {
      // TODO(mbkkt) probably should be is_same_v<T, RemovedField>
      if constexpr (!std::is_empty_v<T>) {
        if (value) {
          return self(*value);
        }
      }
      b.add(vpack::Slice::nullSlice());
    } else if constexpr (kIsVariant<T>) {
      return std::visit([&](const auto& v) { return self(v); }, value);
    } else if constexpr (kIsVPackBuilder<T> || kIsVPackString<T>) {
      b.add(value.slice());
    } else if constexpr (kIsVPackSlice<T>) {
      b.add(value);
    } else if constexpr (detail::IsRange<T>) {
      b.openArray(true);
      absl::c_for_each(value, self);
      b.close();
    } else if constexpr (kIsTuple<T>) {
      b.openArray(true);
      std::apply([&](const auto&... args) { (self(args), ...); }, value);
      b.close();
    } else if constexpr (std::is_enum_v<T>) {
      SDB_ASSERT(magic_enum::enum_cast<T>(std::to_underlying(value)));
      b.add(std::to_underlying(value));
    } else if constexpr (std::is_aggregate_v<T>) {
      b.openArray(true);
      boost::pfr::for_each_field(value, self);
      b.close();
    } else {
      static_assert(kIsPrimitive<T>, "Other types are not supported");
      b.add(value);
    }
  };

  impl(in);
}

template<typename T, typename A = detail::Empty>
void ReadObject(vpack::Slice slice, T& v, ReadOptions options = {},
                const A& arg = {}) {
  static_assert(!std::is_const_v<T>);

  detail::kLoader<T, A>.load(std::addressof(v), slice, options,
                             std::addressof(arg));
}

template<typename T, typename A = detail::Empty>
sdb::Result ReadObjectNothrow(vpack::Slice slice, T& v,
                              ReadOptions options = {}, const A& arg = {}) {
  return sdb::basics::SafeCall(
    [&] { ReadObject(slice, v, options, arg); },
    [](ErrorCode code, std::string_view msg) {
      return sdb::Result{
        code == sdb::ERROR_INTERNAL ? sdb::ERROR_BAD_PARAMETER : code, msg};
    });
}

template<typename U, typename A = detail::Empty>
void WriteObject(vpack::Builder& b, const U& in, const A& arg = {}) {
  // TODO(gnusi): reserve some space based on layout?
  auto impl = [&]<typename K, typename T>(this auto& self, const K& key,
                                          const T& value) {
    static_assert(kIsWritable<T, ObjectFormat, A>, "Type is not writable");

    if constexpr (detail::HasWriteHook<K, T, A>) {
      if (!VPackWriteHook(
            detail::Context<ObjectFormat, vpack::Builder*, A>{&b, arg}, key,
            value)) {
        return false;
      }
    }

    if constexpr (detail::IsEmbeddedField<T>) {
      // TODO(gnusi): think about it
      auto& v = *value;
      if constexpr (kIsPointer<std::remove_cvref_t<decltype(v)>>) {
        boost::pfr::for_each_field_with_name(*v, self);
      } else {
        boost::pfr::for_each_field_with_name(v, self);
      }
    } else if constexpr (detail::HasWriteOverload<ObjectFormat, T, A>) {
      WriteKey(key, b, arg);
      VPackWrite(detail::Context<ObjectFormat, vpack::Builder*, A>{&b, arg},
                 value);
    } else if constexpr (kIsNullable<T>) {
      if constexpr (!std::is_empty_v<T>) {
        if (value) {
          return self(key, *value);
        }
      }

      if constexpr (std::is_same_v<K, detail::Empty>) {
        // We force null value for empty keys, for example for
        // kIsVariant, IsRange, kIsTuple, IsMap cases below
        b.add(vpack::Slice::nullSlice());
      }

      return false;
    } else {
      WriteKey(key, b, arg);
      if constexpr (kIsVariant<T>) {
        return std::visit(
          [&](const auto& v) { return self(detail::Empty{}, v); }, value);
      } else if constexpr (kIsVPackBuilder<T> || kIsVPackString<T>) {
        b.add(value.slice());
      } else if constexpr (kIsVPackSlice<T>) {
        b.add(value);
      } else if constexpr (detail::IsMap<T>) {
        b.openObject(true);
        for (auto& [k, v] : value) {
          WriteKey(k, b, arg);
          self(detail::Empty{}, v);
        }
        b.close();
      } else if constexpr (detail::IsRange<T>) {
        b.openArray(true);
        for (auto& v : value) {
          self(detail::Empty{}, v);
        }
        b.close();
      } else if constexpr (kIsTuple<T>) {
        b.openArray(true);
        std::apply(
          [&](const auto&... args) { (self(detail::Empty{}, args), ...); },
          value);
        b.close();
      } else if constexpr (std::is_enum_v<T>) {
        const auto v = magic_enum::enum_name(value);
        SDB_ASSERT(!v.empty());
        b.add(v);
      } else if constexpr (std::is_aggregate_v<T>) {
        b.openObject(true);
        boost::pfr::for_each_field_with_name(value, self);
        b.close();
      } else {
        static_assert(kIsPrimitive<T>, "Other types are not supported");
        b.add(value);
      }
    }

    return true;
  };

  impl(detail::Empty{}, in);
}

template<typename T, typename A = detail::Empty>
sdb::Result ReadTupleNothrow(vpack::Slice slice, T& v, const A& arg = {}) {
  return sdb::basics::SafeCall(
    [&] { ReadTuple(slice, v, arg); },
    [](ErrorCode code, std::string_view msg) {
      return sdb::Result{
        code == sdb::ERROR_INTERNAL ? sdb::ERROR_BAD_PARAMETER : code, msg};
    });
}

template<typename T, typename A = detail::Empty>
sdb::Result ReadNothrow(bool object, vpack::Slice slice, T& v,
                        ReadOptions options = {}, const A& arg = {}) {
  if (object) {
    return ReadObjectNothrow(slice, v, options, arg);
  } else {
    return ReadTupleNothrow(slice, v, arg);
  }
}

template<typename Context, typename T>
bool VPackWriteHook(Context ctx, auto&&, const Skip<T>&) {
  static_assert(std::is_same_v<typename Context::Format, ObjectFormat>);
  return false;
}

template<typename Context, typename T, detail::FixedString S>
bool VPackWriteHook(Context ctx, auto&&, const Name<S, T>& v) {
  static_assert(std::is_same_v<typename Context::Format, ObjectFormat>);

  ctx.vpack().add(Name<S, T>::kName);
  WriteObject(ctx.vpack(), *v, ctx.arg());
  return false;
}

template<typename Context, typename T>
void VPackWrite(Context ctx, const Field<T>& v) {
  if constexpr (std::is_same_v<typename Context::Format, ObjectFormat>) {
    WriteObject(ctx.vpack(), *v, ctx.arg());
  } else {
    WriteTuple(ctx.vpack(), *v, ctx.arg());
  }
}

template<typename Context, typename T, typename K>
bool VPackWriteHook(Context ctx, const K& key, const Nullable<T>& value) {
  static_assert(std::is_same_v<typename Context::Format, ObjectFormat>);

  WriteKey(key, ctx.vpack(), ctx.arg());

  if constexpr (!std::is_empty_v<T>) {
    if (auto& v = *value; v) {
      WriteObject(ctx.vpack(), *v, ctx.arg());
      return false;
    }
  }

  ctx.vpack().add(vpack::Slice::nullSlice());
  return false;
}

}  // namespace vpack
