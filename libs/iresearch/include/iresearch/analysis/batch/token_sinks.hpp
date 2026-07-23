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

#include <string_view>
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
// writer that feeds it. Feed with `AnalyzeValue()` below or drive `writer`
// directly and call `writer.Finish()`.
class TokenCollector final : public TokenConsumer {
 public:
  explicit TokenCollector(TokenLayout layout = TokenLayout::TermsPosOffs)
    : writer{*this}, layout{layout} {}

  void Consume(TokenBatch& batch, std::span<const DocRun> /*runs*/) final {
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      const uint32_t pos = writer.DensePos() ? ++_dense_pos : batch.pos[i];
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

}  // namespace irs
