////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/flat_hash_map.h>

#include "filter.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/type_id.hpp"

namespace irs {

struct ProxyQueryCache;

// Proxy filter designed to cache results of underlying real filter and
// provide fast replaying same filter on consequent calls.
// It is up to caller to control validity of the supplied cache. If the index
// was changed caller must discard cache and request a new one via make_cache.
// Scoring cache is not supported yet.
class ProxyFilter final : public Filter {
 public:
  using cache_ptr = std::shared_ptr<ProxyQueryCache>;

  Query::ptr prepare(const PrepareContext& ctx) const final;

  template<typename Impl, typename Base = Impl, typename... Args>
  std::pair<Base&, cache_ptr> set_filter(IResourceManager& memory,
                                         Args&&... args) {
    static_assert(std::is_base_of_v<Filter, Base>);
    static_assert(std::is_base_of_v<Base, Impl>);
    auto& real =
      cache_filter(memory, std::make_unique<Impl>(std::forward<Args>(args)...));
    return {sdb::basics::downCast<Base>(real), _cache};
  }

  void set_cache(cache_ptr cache) noexcept { _cache = std::move(cache); }

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<ProxyFilter>::id();
  }

 private:
  Filter& cache_filter(IResourceManager& memory, Filter::ptr&& ptr);

  mutable cache_ptr _cache;
};

}  // namespace irs
