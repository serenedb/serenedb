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
#include <string_view>
#include <vector>

#include "iresearch/analysis/token_batch.hpp"

namespace irs {

// Accumulates one value's tokens into caller-owned vectors: term handles,
// absolute batch-convention positions, and offsets when requested. Reused per
// value via Rebind of the target vectors.
//
// Term retention (which bytes survive the consume cycle):
//   - inline terms (<= INLINE_LENGTH) ride in the 16-byte string_t, always kept
//     as-is;
//   - non-inline terms that point INTO `value` (a producer emitting views into
//     the caller's input) are kept as views -- zero copy -- valid as long as
//     the bound `value` is alive (the caller guarantees it outlives use);
//   - all other non-inline terms (bytes the kernel generated into its own,
//     recycled arena) are copied into `arena`.
// A caller that wants to know which terms were copied (e.g. to intern only
// those when re-emitting past the source's lifetime) binds a `needs_intern`
// flag vector. `value` defaults to empty -> every non-inline term is copied
// (the original behavior).
class TokenAccumulator final : public TokenConsumer {
 public:
  explicit TokenAccumulator(duckdb::ArenaAllocator& arena) : _arena(&arena) {}

  void Bind(std::vector<duckdb::string_t>& terms, std::vector<uint32_t>& pos,
            bool dense, std::vector<uint32_t>* offs_start = nullptr,
            std::vector<uint32_t>* offs_end = nullptr,
            duckdb::string_t value = {},
            std::vector<uint8_t>* needs_intern = nullptr) noexcept {
    _terms = &terms;
    _pos = &pos;
    _dense = dense;
    _offs_start = offs_start;
    _offs_end = offs_end;
    const uint32_t size = value.GetSize();
    _val_begin =
      size > duckdb::string_t::INLINE_LENGTH ? value.GetData() : nullptr;
    _val_end = _val_begin + size;
    _needs_intern = needs_intern;
    _dense_pos = 0;
  }

  void Consume(TokenBatch& batch, DocRuns /*runs*/) final {
    const auto count = batch.count;
    for (uint32_t i = 0; i < count; ++i) {
      const auto& t = batch.terms[i];
      const auto size = t.GetSize();
      const auto* data = t.GetData();
      const bool view = size <= duckdb::string_t::INLINE_LENGTH ||
                        (_val_begin != nullptr && data >= _val_begin &&
                         data < _val_end);
      if (view) {
        _terms->push_back(t);
      } else {
        auto* mem = _arena->Allocate(size);
        std::memcpy(mem, data, size);
        _terms->push_back(duckdb::string_t{
          reinterpret_cast<const char*>(mem), static_cast<uint32_t>(size)});
      }
      if (_needs_intern != nullptr) {
        _needs_intern->push_back(view ? 0 : 1);
      }
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
  std::vector<uint8_t>* _needs_intern = nullptr;
  const char* _val_begin = nullptr;
  const char* _val_end = nullptr;
  uint32_t _dense_pos = 0;
  bool _dense = false;
};

}  // namespace irs
