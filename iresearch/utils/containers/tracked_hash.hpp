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

#include <utility>

#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs::containers {

template<typename K, typename V,
         typename Hash = typename absl::flat_hash_map<K, V>::hasher,
         typename Eq = typename absl::flat_hash_map<K, V, Hash>::key_equal>
using TrackedFlatHashMap =
  FlatHashMap<K, V, Hash, Eq, TrackedAllocator<std::pair<const K, V>>>;

template<typename T, typename Hash = typename absl::flat_hash_set<T>::hasher,
         typename Eq = typename absl::flat_hash_set<T, Hash>::key_equal>
using TrackedFlatHashSet = FlatHashSet<T, Hash, Eq, TrackedAllocator<T>>;

}  // namespace irs::containers
