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

#include <tuple>
#include <vector>

#include "iresearch/analysis/tokenizer.hpp"

namespace irs::analysis {

class SparseNGramTokenizer final : public TypedTokenizer<SparseNGramTokenizer>,
                                   private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "generate_sparse_ngrams";
  }

  struct Options {
    using Owner = SparseNGramTokenizer;
    size_t max_ngram_length{16};
    bool covering{false};
    size_t min_ngram_length{3};
    size_t min_cutoff_length{0};
  };
  static ptr Make(Options opts);

  explicit SparseNGramTokenizer(Options options);

  TokenTraits Traits() const noexcept final { return {}; }

  std::tuple<bool, bool> PrepareBatch(BlockTraits traits) const noexcept {
    return {traits.ascii, _generic};
  }

  size_t MemoryUsage() const noexcept final {
    return _stack.capacity() * sizeof(HashAndPos) +
           _pending.capacity() * sizeof(EmitKSlot) +
           _hashes.capacity() * sizeof(uint32_t) +
           _bounds.capacity() * sizeof(uint32_t) +
           _units.capacity() * sizeof(uint32_t);
  }

  template<TokenLayout Layout, bool KnownAscii, bool Generic>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  struct HashAndPos {
    uint32_t hash;
    uint32_t pos;
  };

  struct Cursor {
    bytes_view data;
    size_t units{0};
    size_t hash_base{0};
    size_t hash_end{0};
    size_t pos{0};
    size_t top{0};
    size_t head{0};
    size_t pending_size{0};
  };

  void EnsureScratch();
  template<bool Symbols, bool Generic>
  IRS_ALIGN_HOT bool Next(Cursor& ctx);
  template<bool Symbols, bool Generic>
  uint64_t FillHashes(Cursor& ctx);
  template<TokenLayout Layout, bool Detect, bool Generic>
  IRS_ALIGN_HOT bool FillBytes(duckdb::string_t value, TokenSink& sink);
  template<TokenLayout Layout, bool Generic>
  bool FillSymbols(duckdb::string_t value, TokenSink& sink);
  template<bool Generic>
  IRS_FORCE_INLINE void StepAll(HashAndPos* base, HashAndPos* limit,
                                HashAndPos*& top, EmitKSlot*& out, size_t i,
                                uint32_t hash) const;
  template<bool Generic>
  IRS_FORCE_INLINE void StepCovering(HashAndPos* base, HashAndPos*& top,
                                     size_t& head, EmitKSlot*& out, size_t i,
                                     uint32_t hash) const;

  template<bool Generic>
  IRS_FORCE_INLINE size_t Window() const noexcept {
    if constexpr (Generic) {
      return _window;
    } else {
      return 2;
    }
  }

  template<bool Generic>
  IRS_FORCE_INLINE void Emit(EmitKSlot*& out, size_t begin,
                             size_t end) const noexcept {
    if constexpr (Generic) {
      if (end - begin < _options.min_cutoff_length) {
        return;
      }
    }
    *out++ = {static_cast<uint32_t>(begin), static_cast<uint32_t>(end)};
  }

  Options _options;
  size_t _window{2};
  bool _generic{false};
  std::vector<HashAndPos> _stack;
  std::vector<EmitKSlot> _pending;
  std::vector<uint32_t> _hashes;
  std::vector<uint32_t> _bounds;
  std::vector<uint32_t> _units;
};

}  // namespace irs::analysis
