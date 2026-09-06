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
#include <ranges>
#include <span>
#include <type_traits>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/analysis/token_sink.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace irs {

template<TokenLayout L = TokenLayout::Terms>
class ValueTokens final : public TokenConsumer, public StoreSink {
 public:
  static constexpr TokenLayout kLayout = L;

  explicit ValueTokens(
    duckdb::Allocator& alloc = duckdb::Allocator::DefaultAllocator())
    requires(L == TokenLayout::Terms)
    : _arena{alloc} {}

  ValueTokens(TokenTraits producer,
              duckdb::Allocator& alloc = duckdb::Allocator::DefaultAllocator())
    requires(L != TokenLayout::Terms)
    : _arena{alloc},
      _dense{!producer.explicit_pos},
      _offsets{producer.offsets} {}

  std::span<const duckdb::string_t> terms() const noexcept { return _terms; }
  bytes_view store() const noexcept { return _store; }
  bool interned() const noexcept { return _interned; }

  std::span<const uint32_t> pos() const noexcept
    requires(L != TokenLayout::Terms)
  {
    return _pos;
  }

  std::span<const uint32_t> offs_start() const noexcept
    requires(L == TokenLayout::TermsPosOffs)
  {
    return _offs_start;
  }

  std::span<const uint32_t> offs_end() const noexcept
    requires(L == TokenLayout::TermsPosOffs)
  {
    return _offs_end;
  }

  size_t MemoryUsage() const noexcept {
    size_t size = _terms.capacity() * sizeof(duckdb::string_t) +
                  _store.capacity() + _arena.SizeInBytes();
    if constexpr (L != TokenLayout::Terms) {
      size += _pos.capacity() * sizeof(uint32_t);
    }
    if constexpr (L == TokenLayout::TermsPosOffs) {
      size +=
        (_offs_start.capacity() + _offs_end.capacity()) * sizeof(uint32_t);
    }
    return size;
  }

  void Prepare(duckdb::string_t value) {
    Discard();
    if (_interned) {
      _arena.Reset();
      if (const auto* head = _arena.GetHead();
          head && head->maximum_size > kMaxRetainedArenaBytes) [[unlikely]] {
        _arena.Destroy();
      }
      _interned = false;
    }
    _dense_pos = 0;
    const auto size = value.GetSize();
    _val_begin =
      size > duckdb::string_t::INLINE_LENGTH ? value.GetData() : nullptr;
    _val_end = _val_begin ? _val_begin + size : nullptr;
  }

  void Discard() noexcept {
    _terms.clear();
    _store.clear();
    if constexpr (L != TokenLayout::Terms) {
      _pos.clear();
    }
    if constexpr (L == TokenLayout::TermsPosOffs) {
      _offs_start.clear();
      _offs_end.clear();
    }
  }

  void Consume(TokenBatch& batch, DocRuns) final {
    const auto count = batch.count;
    const auto first = _terms.size();
    _terms.insert(_terms.end(), batch.terms, batch.terms + count);
    auto* const terms = _terms.data() + first;
    for (uint32_t i = 0; i < count; ++i) {
      const auto size = terms[i].GetSize();
      if (size <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
        continue;
      }
      const auto* data = terms[i].GetData();
      if (_val_begin && data >= _val_begin && data + size <= _val_end) {
        continue;
      }
      terms[i] = Intern(data, static_cast<uint32_t>(size));
    }
    if constexpr (L != TokenLayout::Terms) {
      if (_dense) {
        const auto ordinals =
          std::views::iota(_dense_pos + 1, _dense_pos + 1 + count);
        _pos.insert(_pos.end(), ordinals.begin(), ordinals.end());
        _dense_pos += count;
      } else {
        _pos.insert(_pos.end(), batch.pos, batch.pos + count);
      }
    }
    if constexpr (L == TokenLayout::TermsPosOffs) {
      if (_offsets) {
        _offs_start.insert(_offs_start.end(), batch.offs_start,
                           batch.offs_start + count);
        _offs_end.insert(_offs_end.end(), batch.offs_end,
                         batch.offs_end + count);
      }
    }
  }

  void OnStore(doc_id_t, bytes_view blob) final {
    _store.assign(blob.data(), blob.size());
  }

 private:
  static constexpr size_t kMaxRetainedArenaBytes = 64 * 1024;

  IRS_NO_INLINE duckdb::string_t Intern(const char* data, uint32_t size) {
    _interned = true;
    auto* mem = _arena.Allocate(size);
    std::memcpy(mem, data, size);
    return {reinterpret_cast<const char*>(mem), size};
  }

  duckdb::ArenaAllocator _arena;
  std::vector<duckdb::string_t> _terms;
  [[no_unique_address]] utils::Need<L != TokenLayout::Terms,
                                    std::vector<uint32_t>> _pos;
  [[no_unique_address]] utils::Need<L == TokenLayout::TermsPosOffs,
                                    std::vector<uint32_t>> _offs_start;
  [[no_unique_address]] utils::Need<L == TokenLayout::TermsPosOffs,
                                    std::vector<uint32_t>> _offs_end;
  bstring _store;
  const char* _val_begin = nullptr;
  const char* _val_end = nullptr;
  uint32_t _dense_pos = 0;
  bool _dense = true;
  bool _offsets = false;
  bool _interned = false;
};

class ValueAnalyzer {
 public:
  explicit ValueAnalyzer(
    duckdb::Allocator& alloc = duckdb::Allocator::DefaultAllocator())
    : _writer{alloc} {}

  template<typename Consumer>
  IRS_FORCE_INLINE bool Analyze(analysis::Tokenizer& tokenizer,
                                duckdb::string_t value, Consumer& out,
                                BlockTraits traits = {}) {
    StoreSink* store = nullptr;
    if constexpr (std::is_base_of_v<StoreSink, Consumer>) {
      store = &out;
    }
    _writer.Bind(out, store);
    out.Prepare(value);
    const FillCtx ctx{Consumer::kLayout, traits};
    bool ok;
    if constexpr (Consumer::kLayout == TokenLayout::TermsPosOffs) {
      ok = tokenizer.Fill(value, doc_limits::min(), _writer, ctx);
    } else {
      ok = tokenizer.Fill(value, _writer, ctx);
    }
    if (!ok) [[unlikely]] {
      _writer.Discard();
      out.Discard();
      return false;
    }
    _writer.Finish();
    return true;
  }

  size_t MemoryUsage() const noexcept { return sizeof(TokenSink); }

 private:
  TokenSink _writer;
};

}  // namespace irs
