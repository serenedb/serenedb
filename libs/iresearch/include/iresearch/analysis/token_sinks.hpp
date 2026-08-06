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

#include "iresearch/analysis/token_batch.hpp"
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
    : layout{layout} {
    writer.Bind(*this, this);
  }

  void Consume(TokenBatch& batch, DocRuns /*runs*/) final {
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      const uint32_t pos = dense ? ++_dense_pos : batch.pos[i];
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
  TokenSink writer;
  TokenLayout layout;
  bool has_offs = false;
  // How to read batch.pos[]: dense = implicit ordinals (the producing
  // kernel's !Traits().explicit_pos); AnalyzeValue sets it per analyzer.
  bool dense = true;

 private:
  uint32_t _dense_pos = 0;
};

inline bool AnalyzeValue(analysis::Tokenizer& tokenizer,
                         duckdb::string_t value, TokenCollector& out) {
  out.clear();
  const auto traits = tokenizer.Traits();
  out.has_offs = traits.offsets && out.layout == TokenLayout::TermsPosOffs;
  out.dense = !traits.explicit_pos;
  if (!tokenizer.Fill(value, doc_limits::min(), out.writer, out.layout)) {
    return false;
  }
  out.writer.Finish();
  return true;
}

// Terms-only drain into a vector, one bstring per token.
class TermVectorSink final : public TokenConsumer {
 public:
  explicit TermVectorSink(std::vector<bstring>& out) : _out(&out) {
    writer.Bind(*this, nullptr);
  }

  void Consume(TokenBatch& batch, DocRuns /*runs*/) final {
    for (const auto& t : batch.Terms()) {
      _out->emplace_back(reinterpret_cast<const byte_type*>(t.GetData()),
                         t.GetSize());
    }
  }

  TokenSink writer;

 private:
  std::vector<bstring>* _out;
};

}  // namespace irs
