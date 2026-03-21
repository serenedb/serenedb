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

#include <absl/container/flat_hash_map.h>

namespace sdb::containers {
namespace detail {

template<typename T>
char IsCompleteHelper(char (*)[sizeof(T)]);

template<typename T>
int IsCompleteHelper(...);

template<size_t SizeofK, size_t SizeofT, typename K, typename V>
constexpr bool MapSizeofChecker() noexcept {
  if constexpr (sizeof(IsCompleteHelper<K>(nullptr)) == sizeof(char) &&
                sizeof(IsCompleteHelper<V>(nullptr)) == sizeof(char)) {
    static_assert(sizeof(K) <= SizeofK && sizeof(K) + sizeof(V) <= SizeofT,
                  "For large K better to use NodeHashMap. "
                  "For large V better to use FlatHashMap<K, some_ptr<V>>. "
                  "If you really sure what do you do,"
                  " please use absl::flat_hash_map directly.");
  }
  return true;
}

}  // namespace detail

template<typename K, typename V,
         typename Hash = typename absl::flat_hash_map<K, V>::hasher,
         typename Eq = typename absl::flat_hash_map<K, V, Hash>::key_equal,
         typename Allocator =
           typename absl::flat_hash_map<K, V, Hash, Eq>::allocator_type
#if !defined(ABSL_HAVE_ADDRESS_SANITIZER) && \
  !defined(ABSL_HAVE_MEMORY_SANITIZER)
         ,  // TODO(mbkkt) After additional benchmarks change Sizeof
         typename = std::enable_if_t<detail::MapSizeofChecker<32, 80, K, V>()>
#endif
         >
using FlatHashMap = absl::flat_hash_map<K, V, Hash, Eq, Allocator>;

}  // namespace sdb::containers
