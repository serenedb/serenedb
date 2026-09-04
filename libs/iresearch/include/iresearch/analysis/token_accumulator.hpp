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

#include "iresearch/analysis/token_sink.hpp"

namespace irs {

class TokenAccumulator final : public TokenConsumer {
 public:
  explicit TokenAccumulator(duckdb::ArenaAllocator& arena) : _arena(&arena) {}

  void Bind(std::vector<duckdb::string_t>& terms, std::vector<uint32_t>& pos,
            bool dense, duckdb::string_t value) noexcept {
    _terms = &terms;
    _pos = &pos;
    _dense = dense;
    const uint32_t size = value.GetSize();
    _val_begin =
      size > duckdb::string_t::INLINE_LENGTH ? value.GetData() : nullptr;
    _val_end = _val_begin ? _val_begin + size : nullptr;
    _dense_pos = 0;
  }

  void Consume(TokenBatch& batch, DocRuns /*runs*/) final {
    const auto count = batch.count;
    for (uint32_t i = 0; i < count; ++i) {
      const auto& t = batch.terms[i];
      const auto size = t.GetSize();
      const auto* data = t.GetData();
      const bool view = size <= duckdb::string_t::INLINE_LENGTH ||
                        (_val_begin && data >= _val_begin && data < _val_end);
      if (view) {
        _terms->push_back(t);
      } else {
        auto* mem = _arena->Allocate(size);
        std::memcpy(mem, data, size);
        _terms->push_back(duckdb::string_t{reinterpret_cast<const char*>(mem),
                                           static_cast<uint32_t>(size)});
      }
      _pos->push_back(_dense ? ++_dense_pos : batch.pos[i]);
    }
  }

 private:
  duckdb::ArenaAllocator* _arena;
  std::vector<duckdb::string_t>* _terms = nullptr;
  std::vector<uint32_t>* _pos = nullptr;
  const char* _val_begin = nullptr;
  const char* _val_end = nullptr;
  uint32_t _dense_pos = 0;
  bool _dense = false;
};

struct AccumulatorSink {
  AccumulatorSink() : accumulator{arena} { writer.Bind(accumulator, nullptr); }

  duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
  TokenAccumulator accumulator;
  TokenSink writer;
};

}  // namespace irs
