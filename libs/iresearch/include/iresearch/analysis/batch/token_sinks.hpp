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

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace irs {

struct CollectedToken {
  bstring term;
  uint32_t pos;
  uint32_t offs_start;
  uint32_t offs_end;
};

// Self-contained collector: a consumer that materializes every token plus the
// writer that feeds it. Feed with `Analyze()` below or drive `writer`
// directly and call `writer.Finish()`.
class TokenCollector final : public TokenConsumer {
 public:
  explicit TokenCollector(TokenLayout layout = TokenLayout::TermsPosOffs)
    : writer{*this}, layout{layout} {}

  void Consume(TokenBatch& batch, std::span<const DocRun> /*runs*/) final {
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      const uint32_t pos = writer.dense_pos ? ++_dense_pos : batch.pos[i];
      tokens.push_back(
        {bstring{reinterpret_cast<const byte_type*>(t.GetData()), t.GetSize()},
         pos, has_offs ? batch.offs_start[i] : 0,
         has_offs ? batch.offs_end[i] : 0});
    }
  }

  void OnStore(doc_id_t /*doc*/, bytes_view blob) final {
    store.assign(blob.data(), blob.size());
  }

  void clear() noexcept {
    tokens.clear();
    store.clear();
    _dense_pos = 0;
  }

  std::vector<CollectedToken> tokens;
  bstring store;
  TokenWriter writer;
  TokenLayout layout;
  bool has_offs = false;

 private:
  uint32_t _dense_pos = 0;
};

inline bool AnalyzeValue(analysis::Tokenizer& tokenizer, std::string_view value,
                         TokenCollector& out) {
  out.clear();
  out.has_offs =
    tokenizer.Traits().offsets && out.layout == TokenLayout::TermsPosOffs;
  if (!tokenizer.Fill(value, out.writer, out.layout)) {
    return false;
  }
  out.writer.Finish();
  return true;
}

// Terms-only drain into a vector, one bstring per token.
class TermVectorSink final : public TokenConsumer {
 public:
  explicit TermVectorSink(std::vector<bstring>& out)
    : writer{*this}, _out(&out) {}

  void Consume(TokenBatch& batch, std::span<const DocRun> /*runs*/) final {
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      _out->emplace_back(reinterpret_cast<const byte_type*>(t.GetData()),
                         t.GetSize());
    }
  }

  TokenWriter writer;

 private:
  std::vector<bstring>* _out;
};

// Terms-only drain appending every token to the LIST(VARCHAR) vector's
// child starting at the construction offset (growing the list as needed);
// offset() after writer.Finish() is the new child offset and keeps
// accumulating across values, so one sink serves a whole result vector.
// The caller owns the final ListVector::SetListSize.
class ListVectorSink final : public TokenConsumer {
 public:
  ListVectorSink(duckdb::Vector& list, uint64_t offset)
    : writer{*this}, _list(&list), _offset(offset) {}

  void Consume(TokenBatch& batch, std::span<const DocRun> /*runs*/) final {
    auto& child = duckdb::ListVector::GetEntry(*_list);
    for (uint32_t i = 0; i < batch.count; ++i) {
      if (_offset >= duckdb::ListVector::GetListCapacity(*_list)) {
        duckdb::ListVector::SetListSize(*_list, _offset);
        duckdb::ListVector::Reserve(
          *_list, duckdb::ListVector::GetListCapacity(*_list) * 2);
      }
      auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child);
      const auto& t = batch.terms[i];
      data[_offset] =
        duckdb::StringVector::AddStringOrBlob(child, t.GetData(), t.GetSize());
      ++_offset;
    }
  }

  uint64_t offset() const noexcept { return _offset; }

  TokenWriter writer;

 private:
  duckdb::Vector* _list;
  uint64_t _offset;
};

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
