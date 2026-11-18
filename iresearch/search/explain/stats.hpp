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

#include <cstdint>
#include <vector>

#include "iresearch/types.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::explain {

struct TermStats {
  bstring term;
  uint64_t docs_with_term = 0;
  uint64_t total_term_freq = 0;
};

struct SlotStats {
  field_id field = field_limits::invalid();
  uint64_t docs_with_field = 0;
  uint64_t total_term_freq = 0;
  std::vector<TermStats> terms;
  bool phrase = false;
  bool blended = false;
};

class StatsIndex {
 public:
  void Add(const byte_type* slot, SlotStats stats) {
    if (slot != nullptr) {
      _slots.insert_or_assign(slot, std::move(stats));
    }
  }

  const SlotStats* Find(const byte_type* slot) const noexcept {
    const auto it = _slots.find(slot);
    return it == _slots.end() ? nullptr : &it->second;
  }

 private:
  containers::FlatHashMap<const byte_type*, SlotStats> _slots;
};

}  // namespace irs::explain
