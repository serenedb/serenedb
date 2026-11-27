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

#include <cstddef>
#include <cstdint>

#include "iresearch/utils/tracked.hpp"

namespace irs::codecs::text {

class DedupScratch {
 public:
  struct LocalCode {
    uint32_t local;
    bool fresh;
  };

  explicit DedupScratch(TrackedAllocator<> alloc) : _slots{alloc} {}

  void Begin(size_t entries) {
    if (_slots.size() != entries) {
      _slots.assign(entries, Slot{});
    }
    ++_epoch;
  }

  LocalCode Local(uint32_t code, uint32_t next) noexcept {
    auto& slot = _slots[code - 1];
    if (slot.epoch == _epoch) {
      return {slot.local, false};
    }
    slot = {_epoch, next};
    return {next, true};
  }

 private:
  struct Slot {
    uint32_t epoch = 0;
    uint32_t local = 0;
  };

  TrackedVector<Slot> _slots;
  uint32_t _epoch = 0;
};

}  // namespace irs::codecs::text
