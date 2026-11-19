////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <array>
#include <cstddef>
#include <magic_enum/magic_enum.hpp>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

template<auto First, auto... Rest>
struct Axis {
  using Key = decltype(First);
  static constexpr std::array<Key, 1 + sizeof...(Rest)> kKeys{First, Rest...};

  template<Key K, typename F>
  IRS_FORCE_INLINE static decltype(auto) Call(F&& f) {
    return std::forward<F>(f).template operator()<K>();
  }
};

struct BoolAxis : Axis<true, false> {
  static constexpr bool Of(bool value) noexcept { return value; }
};

template<auto Values, typename = std::make_index_sequence<Values.size()>>
struct ArrayAxis;

template<auto Values, size_t... I>
struct ArrayAxis<Values, std::index_sequence<I...>> : Axis<Values[I]...> {
  using Key = typename Axis<Values[I]...>::Key;

  static constexpr Key Of(Key value) noexcept { return value; }
};

template<typename Enum>
using EnumAxis = ArrayAxis<magic_enum::enum_values<Enum>()>;

template<typename T>
struct AxisFor;

template<>
struct AxisFor<bool> {
  using Type = BoolAxis;
};

template<typename T>
struct AxisFor<T*> {
  using Type = BoolAxis;
};

template<typename E>
  requires std::is_enum_v<E>
struct AxisFor<E> {
  using Type = EnumAxis<E>;
};

template<typename T>
using AxisOf = typename AxisFor<std::remove_cvref_t<T>>::Type;

template<typename A>
concept DispatchAxis = requires { A::kKeys; };

template<typename A, auto K>
struct Pin {
  using Axis = A;
  static constexpr typename A::Key kKey = K;
};

template<typename... Ps>
struct Pins {
  template<typename A>
  static constexpr bool kHas = (std::is_same_v<A, typename Ps::Axis> || ...);

  template<typename A>
  static consteval typename A::Key KeyOf() {
    static_assert(kHas<A>);
    return Find<A, Ps...>();
  }

  template<typename A>
  static consteval bool Pinned(typename A::Key key) {
    if constexpr (kHas<A>) {
      return KeyOf<A>() == key;
    } else {
      return false;
    }
  }

 private:
  template<typename A, typename P, typename... Rest>
  static consteval typename A::Key Find() {
    if constexpr (std::is_same_v<A, typename P::Axis>) {
      return P::kKey;
    } else {
      return Find<A, Rest...>();
    }
  }
};

using NoPins = Pins<>;

template<typename T>
inline constexpr bool kIsPins = false;

template<typename... Ps>
inline constexpr bool kIsPins<Pins<Ps...>> = true;

template<typename P, typename... As>
using Restrict = Pins<Pin<As, P::template KeyOf<As>()>...>;

namespace dispatch {

template<typename A, typename Value>
constexpr bool Matches(Value value, typename A::Key key) noexcept {
  if constexpr (requires { A::Matches(value, key); }) {
    return A::Matches(value, key);
  } else {
    return value == key;
  }
}

template<typename A, size_t I, typename Value, typename F>
IRS_FORCE_INLINE decltype(auto) KeyAt(Value key, F& f) {
  constexpr auto kKey = A::kKeys[I];
  if constexpr (I + 1 == A::kKeys.size()) {
    SDB_ASSERT(Matches<A>(key, kKey));
    return f.template operator()<kKey>();
  } else {
    if (key == kKey) {
      return f.template operator()<kKey>();
    }
    return KeyAt<A, I + 1>(key, f);
  }
}

template<typename Acc, typename... As>
struct PinsOf;

template<typename... Ps>
struct PinsOf<Pins<Ps...>> {
  template<size_t I, typename Tuple, typename F>
  IRS_FORCE_INLINE static decltype(auto) Run(const Tuple&, F& f) {
    return f.template operator()<Pins<Ps...>>();
  }
};

}  // namespace dispatch

template<typename A, typename P = NoPins, typename Selector, typename F>
IRS_FORCE_INLINE decltype(auto) ResolveKey(
  [[maybe_unused]] const Selector& selector, F&& f) {
  if constexpr (P::template kHas<A>) {
    constexpr auto kKey = P::template KeyOf<A>();
    SDB_ASSERT(dispatch::Matches<A>(A::Of(selector), kKey));
    return std::forward<F>(f).template operator()<kKey>();
  } else if constexpr (requires { A::Dispatch(A::Of(selector), f); }) {
    return A::Dispatch(A::Of(selector), f);
  } else {
    return dispatch::KeyAt<A, 0>(A::Of(selector), f);
  }
}

template<DispatchAxis A, typename P = NoPins, typename Selector, typename F>
IRS_FORCE_INLINE decltype(auto) Resolve(const Selector& selector, F&& f) {
  return ResolveKey<A, P>(
    selector, [&]<typename A::Key K> IRS_FORCE_INLINE -> decltype(auto) {
      return A::template Call<K>(f);
    });
}

template<typename P = NoPins, typename Selector, typename F>
  requires kIsPins<P>
IRS_FORCE_INLINE decltype(auto) Resolve(const Selector& selector, F&& f) {
  return Resolve<AxisOf<Selector>, P>(selector, std::forward<F>(f));
}

namespace dispatch {

template<typename... Ps, typename A, typename... As>
struct PinsOf<Pins<Ps...>, A, As...> {
  template<size_t I, typename Tuple, typename F>
  IRS_FORCE_INLINE static decltype(auto) Run(const Tuple& selectors, F& f) {
    return ResolveKey<A>(
      std::get<I>(selectors),
      [&]<typename A::Key K> IRS_FORCE_INLINE -> decltype(auto) {
        return PinsOf<Pins<Ps..., Pin<A, K>>, As...>::template Run<I + 1>(
          selectors, f);
      });
  }
};

}  // namespace dispatch

template<typename Base, typename... As, typename... Selectors, typename F>
IRS_FORCE_INLINE decltype(auto) ResolvePinsFrom(
  const std::tuple<Selectors...>& selectors, F&& f) {
  static_assert(sizeof...(As) == sizeof...(Selectors));
  return dispatch::PinsOf<Base, As...>::template Run<0>(selectors, f);
}

template<typename... As, typename... Selectors, typename F>
IRS_FORCE_INLINE decltype(auto) ResolvePins(
  const std::tuple<Selectors...>& selectors, F&& f) {
  return ResolvePinsFrom<NoPins, As...>(selectors, std::forward<F>(f));
}

}  // namespace irs
