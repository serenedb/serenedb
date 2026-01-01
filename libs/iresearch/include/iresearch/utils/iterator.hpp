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

#include <memory>

#include "basics/memory.hpp"
#include "basics/misc.hpp"
#include "basics/noncopyable.hpp"
#include "basics/std.hpp"

namespace irs {

template<typename T, typename Base = memory::Managed>
struct Iterator : Base {
  virtual T value() const = 0;
  // TODO(mbkkt) return T? In such case some algorithms probably will be faster
  virtual bool next() = 0;
};

template<typename Key, typename Value, typename Iterator, typename Base,
         typename Less = std::less<Key>>
class IteratorAdaptor : public Base {
 public:
  typedef Iterator iterator_type;
  typedef Key key_type;
  typedef Value value_type;
  typedef const value_type& const_reference;

  IteratorAdaptor(iterator_type begin, iterator_type end,
                  const Less& less = Less())
    : _begin{begin}, _cur{begin}, _end{end}, _less{less} {}

  const_reference value() const noexcept final { return *_cur; }

  bool seek(key_type key) noexcept final {
    _begin = std::lower_bound(_cur, _end, key, _less);
    return next();
  }

  bool next() noexcept final {
    if (_begin == _end) {
      _cur = _begin;  // seal iterator
      return false;
    }

    _cur = _begin++;
    return true;
  }

 private:
  iterator_type _begin;
  iterator_type _cur;
  iterator_type _end;
  [[no_unique_address]] Less _less;
};

}  // namespace irs
