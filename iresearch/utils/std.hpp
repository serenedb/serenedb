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

#include <algorithm>
#include <iterator>

#include "iresearch/utils/assert.h"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace irstd {

template<typename In, typename Out>
struct AdjustConst {
  typedef Out value_type;
  typedef Out& reference;
  typedef Out* pointer;
};

template<typename In, typename Out>
struct AdjustConst<const In, Out> {
  typedef const Out value_type;
  typedef const Out& reference;
  typedef const Out* pointer;
};

// converts reverse iterator to corresponding forward iterator
// the function does not accept containers "end"
template<typename ReverseIterator>
constexpr typename ReverseIterator::iterator_type ToForward(
  ReverseIterator it) {
  return --(it.base());
}

template<typename Iterator>
constexpr std::reverse_iterator<Iterator> MakeReverseIterator(Iterator it) {
  return std::reverse_iterator<Iterator>(it);
}

template<typename Container, typename Iterator>
void SwapRemove(Container& cont, Iterator it) {
  SDB_ASSERT(!cont.empty());
  std::swap(*it, cont.back());
  cont.pop_back();
}

namespace heap {
namespace detail {

template<typename RandomAccessIterator, typename Diff, typename Func>
inline void ForEachTop(RandomAccessIterator begin, Diff idx, Diff bottom,
                       Func func) {
  if (idx < bottom) {
    const RandomAccessIterator target = begin + idx;
    if (*begin == *target) {
      func(*target);
      ForEachTop(begin, 2 * idx + 1, bottom, func);
      ForEachTop(begin, 2 * idx + 2, bottom, func);
    }
  }
}

template<typename RandomAccessIterator, typename Diff, typename Pred,
         typename Func>
inline void ForEachIf(RandomAccessIterator begin, Diff idx, Diff bottom,
                      Pred pred, Func func) {
  if (idx < bottom) {
    const RandomAccessIterator target = begin + idx;
    if (pred(*target)) {
      func(*target);
      ForEachIf(begin, 2 * idx + 1, bottom, pred, func);
      ForEachIf(begin, 2 * idx + 2, bottom, pred, func);
    }
  }
}

}  // namespace detail

/////////////////////////////////////////////////////////////////////////////
/// @brief calls func for each element in a heap equals to top
////////////////////////////////////////////////////////////////////////////
template<typename RandomAccessIterator, typename Pred, typename Func>
inline void ForEachIf(RandomAccessIterator begin, RandomAccessIterator end,
                      Pred pred, Func func) {
  typedef typename std::iterator_traits<RandomAccessIterator>::difference_type
    difference_type;

  detail::ForEachIf(begin, difference_type(0), difference_type(end - begin),
                    pred, func);
}

/////////////////////////////////////////////////////////////////////////////
/// @brief calls func for each element in a heap equals to top
////////////////////////////////////////////////////////////////////////////
template<typename RandomAccessIterator, typename Func>
inline void ForEachTop(RandomAccessIterator begin, RandomAccessIterator end,
                       Func func) {
  typedef typename std::iterator_traits<RandomAccessIterator>::difference_type
    difference_type;

  detail::ForEachTop(begin, difference_type(0), difference_type(end - begin),
                     func);
}

}  // namespace heap

/////////////////////////////////////////////////////////////////////////////
/// @brief checks that all values in the specified range are equals
////////////////////////////////////////////////////////////////////////////
template<typename ForwardIterator>
inline bool AllEqual(ForwardIterator begin, ForwardIterator end) {
  typedef typename std::iterator_traits<ForwardIterator>::value_type value_type;
  return end == std::adjacent_find(begin, end, std::not_equal_to<value_type>());
}

//////////////////////////////////////////////////////////////////////////////
/// @class back_emplace_iterator
/// @brief provide in place construction capabilities for stl algorithms
////////////////////////////////////////////////////////////////////////////
template<typename Container>
class BackEmplaceIterator {
 public:
  using container_type = Container;
  using iterator_category = std::output_iterator_tag;
  using value_type = void;
  using pointer = void;
  using reference = void;
  using difference_type = ptrdiff_t;

  explicit BackEmplaceIterator(Container& cont) noexcept
    : _cont{std::addressof(cont)} {}

  template<class T>
  auto& operator=(T&& t) {
    _cont->emplace_back(std::forward<T>(t));
    return *this;
  }

  auto& operator*() { return *this; }
  auto& operator++() { return *this; }
  auto& operator++(int) { return *this; }

 private:
  Container* _cont;
};

template<typename Container>
inline auto BackEmplacer(Container& cont) noexcept {
  return BackEmplaceIterator<Container>{cont};
}

namespace detail {

template<typename Builder, size_t Size>
struct Initializer {
  using type = typename Builder::Type;

  static constexpr auto kIdx = Size - 1;

  template<typename Array>
  constexpr explicit Initializer(Array& cache) : init(cache) {
    cache[kIdx] = []() -> const type& {
      static const typename Builder::Type kInstance = Builder::Make(kIdx);
      return kInstance;
    };
  }

  Initializer<Builder, Size - 1> init;
};

template<typename Builder>
struct Initializer<Builder, 1> {
  using type = typename Builder::Type;

  static constexpr auto kIdx = 0;

  template<typename Array>
  constexpr explicit Initializer(Array& cache) {
    cache[kIdx] = []() -> const type& {
      static const typename Builder::Type kInstance = Builder::Make(kIdx);
      return kInstance;
    };
  }
};

template<typename Builder>
struct Initializer<Builder, 0>;

}  // namespace detail

template<typename Builder, size_t Size>
class StaticLazyArray {
 public:
  using type = typename Builder::Type;

  static const type& at(size_t i) {
    static const StaticLazyArray kInstance;
    return kInstance._cache[std::min(i, Size)]();
  }

 private:
  constexpr StaticLazyArray() : _init{_cache} {
    _cache[Size] = []() -> const type& {
      static const type kInstance;
      return kInstance;
    };
  }

  using func = const type& (*)();

  func _cache[Size + 1];
  detail::Initializer<Builder, Size> _init;
};

}  // namespace irstd
}  // namespace irs
