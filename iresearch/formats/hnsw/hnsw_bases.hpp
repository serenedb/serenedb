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

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include <memory>
#include <vector>

#include "iresearch/types.hpp"

namespace irs {

class HnswBases {
 public:
  using Basis = std::shared_ptr<const std::vector<float>>;

  Basis Find(field_id field) const;

  Basis Publish(field_id field, Basis basis);

  Basis Share(field_id field, std::vector<float> basis);

 private:
  mutable absl::Mutex _mutex;
  absl::flat_hash_map<field_id, Basis> _published;
  std::vector<std::weak_ptr<const std::vector<float>>> _loaded;
};

}  // namespace irs
