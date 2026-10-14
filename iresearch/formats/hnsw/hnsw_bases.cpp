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

#include "iresearch/formats/hnsw/hnsw_bases.hpp"

#include <faiss/utils/distances.h>

#include <algorithm>
#include <utility>

#include "iresearch/formats/hnsw/hnsw_graph.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

HnswBases::Basis HnswBases::Find(field_id field) const {
  absl::MutexLock lock{&_mutex};
  const auto it = _published.find(field);
  return it == _published.end() ? nullptr : it->second;
}

HnswBases::Basis HnswBases::Publish(field_id field, Basis basis) {
  absl::MutexLock lock{&_mutex};
  auto& slot = _published[field];
  if (!slot) {
    slot = std::move(basis);
  }
  return slot;
}

HnswBases::Basis HnswBases::Share(field_id field, std::vector<float> basis) {
  absl::MutexLock lock{&_mutex};
  auto& published = _published[field];
  if (published && *published == basis) {
    return published;
  }
  for (const auto& loaded : _loaded) {
    if (auto shared = loaded.lock(); shared && *shared == basis) {
      return shared;
    }
  }
  std::erase_if(_loaded, [](const auto& loaded) { return loaded.expired(); });
  auto shared = std::make_shared<const std::vector<float>>(std::move(basis));
  if (published) {
    _loaded.push_back(shared);
  } else {
    published = shared;
  }
  return shared;
}

std::shared_ptr<const PanoramaQuery> AnnQueryCache::Get(
  const Basis& basis, std::span<const float> query) {
  const auto d = static_cast<uint32_t>(query.size());
  SDB_ASSERT(basis->size() == size_t{d} * d);
  Entry* entry = nullptr;
  {
    absl::MutexLock lock{&_mutex};
    for (const auto& cached : _entries) {
      if (cached->basis == basis) {
        entry = cached.get();
        break;
      }
    }
    if (entry == nullptr) {
      auto made = std::make_unique<Entry>();
      made->basis = basis;
      made->query = std::make_shared<PanoramaQuery>();
      made->query->rotated.resize(d);
      made->query->tails.resize(HnswPanoramaLevels(d) + 1);
      entry = _entries.emplace_back(std::move(made)).get();
    }
  }
  auto& out = *entry->query;
  const uint32_t slices = HnswPanoramaLevels(d);
  for (auto slice = entry->next.fetch_add(1, std::memory_order_relaxed);
       slice < slices;
       slice = entry->next.fetch_add(1, std::memory_order_relaxed)) {
    const uint32_t first = slice * kHnswPanoramaLevel;
    const uint32_t last = std::min(d, first + kHnswPanoramaLevel);
    for (uint32_t row = first; row < last; ++row) {
      out.rotated[row] = faiss::fvec_inner_product(
        basis->data() + size_t{row} * d, query.data(), d);
    }
    if (entry->done.fetch_add(1, std::memory_order_acq_rel) + 1 == slices) {
      HnswPanoramaTails(out.rotated.data(), d, out.tails.data());
      entry->ready.store(true, std::memory_order_release);
      entry->ready.notify_all();
    }
  }
  entry->ready.wait(false, std::memory_order_acquire);
  return entry->query;
}

}  // namespace irs
