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

#include <vector>

#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs::analysis {

////////////////////////////////////////////////////////////////////////////////
/// @class SparseNGramTokenizer
/// @brief produces a sparse subset of variable-length ngrams (>= 3 bytes)
///        selected by a monotonic stack over bigram hashes, after
///        https://github.com/danlark1/sparse_ngrams (used by GitHub code
///        search). With covering=false emits at most 2n-2 ngrams covering
///        every substring (index side); with covering=true emits a minimal
///        covering chain of at most n-2 ngrams whose conjunction selects
///        documents containing the input as a substring (query side).
////////////////////////////////////////////////////////////////////////////////
class SparseNGramTokenizer final : public TypedTokenizer<SparseNGramTokenizer>,
                                   private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "sparse_ngram";
  }

  struct Options {
    using Owner = SparseNGramTokenizer;
    size_t max_ngram_length{16};
    bool covering{false};
  };
  static ptr Make(Options opts);

  explicit SparseNGramTokenizer(Options options);

  TokenTraits Traits() const noexcept final { return {.offsets = true}; }
  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  struct HashAndPos {
    uint32_t hash;
    size_t pos;
  };

  struct FillCtx {
    bytes_view data;
    uint64_t* pending_out{nullptr};
    size_t hash_base{0};
    size_t hash_end{0};
    size_t head{0};
    size_t pending_size{0};
    size_t pos{0};
  };

  bool Advance(FillCtx& ctx);
  void FillHashes(FillCtx& ctx);
  void StepAll(FillCtx& ctx, size_t i, uint32_t hash);
  void StepCovering(FillCtx& ctx, size_t i, uint32_t hash);

  static void Emit(FillCtx& ctx, size_t begin, size_t end) noexcept {
    *ctx.pending_out++ = begin | (end << 32);
  }

  size_t StackSize(const FillCtx& ctx) const noexcept {
    return _stack.size() - ctx.head;
  }

  Options _options;
  std::vector<HashAndPos> _stack;
  std::vector<uint64_t> _pending;
  std::vector<uint32_t> _hashes;
};

extern template class TypedTokenizer<SparseNGramTokenizer>;

}  // namespace irs::analysis
