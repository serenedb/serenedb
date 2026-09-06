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

namespace irs::analysis {

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

  size_t MemoryUsage() const noexcept final {
    return _stack.capacity() * sizeof(HashAndPos) +
           _pending.capacity() * sizeof(uint64_t) +
           _hashes.capacity() * sizeof(uint32_t);
  }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  struct HashAndPos {
    uint32_t hash;
    uint32_t pos;
  };

  struct Cursor {
    bytes_view data;
    size_t hash_base{0};
    size_t hash_end{0};
    size_t pos{0};
    size_t top{0};
    size_t head{0};
    size_t pending_size{0};
  };

  void EnsureScratch();
  bool Advance(Cursor& ctx);
  void FillHashes(Cursor& ctx);
  IRS_FORCE_INLINE void StepAll(HashAndPos* base, HashAndPos* limit,
                                HashAndPos*& top, uint64_t*& out, size_t i,
                                uint32_t hash) const;
  IRS_FORCE_INLINE void StepCovering(HashAndPos* base, HashAndPos*& top,
                                     size_t& head, uint64_t*& out, size_t i,
                                     uint32_t hash) const;

  static void Emit(uint64_t*& out, size_t begin, size_t end) noexcept {
    *out++ = begin | (end << 32);
  }

  Options _options;
  std::vector<HashAndPos> _stack;
  std::vector<uint64_t> _pending;
  std::vector<uint32_t> _hashes;
};

extern template class TypedTokenizer<SparseNGramTokenizer>;

}  // namespace irs::analysis
