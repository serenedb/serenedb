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

#include <utility>

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

}  // namespace irs
