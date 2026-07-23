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

#include <cstring>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <vector>

#include "iresearch/analysis/batch/token_batch.hpp"

namespace irs {

// Accumulates one value's tokens into caller-owned vectors: term handles
// (bytes copied into `arena`, so they outlive the consume cycle until the
// owner resets that arena), absolute batch-convention positions, and offsets
// when requested. Reused per value via Rebind of the target vectors.
class TokenAccumulator final : public TokenConsumer {
 public:
  explicit TokenAccumulator(duckdb::ArenaAllocator& arena) : _arena(&arena) {}

  void Bind(std::vector<duckdb::string_t>& terms, std::vector<uint32_t>& pos,
            bool dense, std::vector<uint32_t>* offs_start = nullptr,
            std::vector<uint32_t>* offs_end = nullptr) noexcept {
    _terms = &terms;
    _pos = &pos;
    _dense = dense;
    _offs_start = offs_start;
    _offs_end = offs_end;
    _dense_pos = 0;
  }

  void Consume(TokenBatch& batch, std::span<const DocRun> /*runs*/) final {
    const auto count = batch.count;
    for (uint32_t i = 0; i < count; ++i) {
      const auto& t = batch.terms[i];
      duckdb::string_t stable = t;
      if (t.GetSize() > duckdb::string_t::INLINE_LENGTH) {
        auto* mem = _arena->Allocate(t.GetSize());
        std::memcpy(mem, t.GetData(), t.GetSize());
        stable =
          duckdb::string_t{reinterpret_cast<const char*>(mem), t.GetSize()};
      }
      _terms->push_back(stable);
      _pos->push_back(_dense ? ++_dense_pos : batch.pos[i]);
    }
    if (_offs_start != nullptr) {
      _offs_start->insert(_offs_start->end(), batch.offs_start,
                          batch.offs_start + count);
      _offs_end->insert(_offs_end->end(), batch.offs_end,
                        batch.offs_end + count);
    }
  }

 private:
  duckdb::ArenaAllocator* _arena;
  std::vector<duckdb::string_t>* _terms = nullptr;
  std::vector<uint32_t>* _pos = nullptr;
  std::vector<uint32_t>* _offs_start = nullptr;
  std::vector<uint32_t>* _offs_end = nullptr;
  uint32_t _dense_pos = 0;
  bool _dense = false;
};

}  // namespace irs
