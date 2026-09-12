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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/hash/hash.h>

#include "string.hpp"

namespace irs {

template<typename Elem>
class HashedBasicStringView : public basic_string_view<Elem> {
 public:
  using Base = basic_string_view<Elem>;

  template<typename Hash = absl::Hash<Base>>
  explicit HashedBasicStringView(Base base, const Hash& hash = {})
    : HashedBasicStringView{base, hash(base)} {}

  HashedBasicStringView(Base base, size_t hash) noexcept
    : Base{base}, _hash{hash} {}

  size_t Hash() const noexcept { return _hash; }

 private:
  size_t _hash;
};

using hashed_string_view = HashedBasicStringView<char>;      // NOLINT
using hashed_bytes_view = HashedBasicStringView<byte_type>;  // NOLINT

struct HashedStrHash {
  using is_transparent = void;

  template<typename T>
  size_t operator()(const basic_string_view<T>& key) const noexcept {
    return absl::HashOf(key);
  }

  template<typename T>
  size_t operator()(const HashedBasicStringView<T>& key) const noexcept {
    return key.Hash();
  }
};

}  // namespace irs
