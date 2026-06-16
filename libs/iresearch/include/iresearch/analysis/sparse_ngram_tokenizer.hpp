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

#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
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
class SparseNGramTokenizer final : public TypedAnalyzer<SparseNGramTokenizer>,
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

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final {
    if (_pending_idx >= _pending_size && !Advance()) {
      return false;
    }
    const uint64_t entry = _pending[_pending_idx++];
    SetTerm(static_cast<uint32_t>(entry), static_cast<uint32_t>(entry >> 32));
    return true;
  }
  bool reset(std::string_view data) final;

 private:
  // Each gram carries its source byte range as an OffsAttr. The grams are
  // emitted in monotonic-stack order, not sorted by start offset, so they
  // cannot feed the persistent indexer (which requires non-decreasing
  // offsets) -- the highlight path sorts them in memory first.
  using attributes = std::tuple<IncAttr, TermAttr, OffsAttr>;

  struct HashAndPos {
    uint32_t hash;
    size_t pos;
  };

  bool Advance();
  void FillHashes();
  void StepAll(size_t i, uint32_t hash);
  void StepCovering(size_t i, uint32_t hash);

  void Emit(size_t begin, size_t end) noexcept {
    *_pending_out++ = begin | (end << 32);
  }

  void SetTerm(uint32_t begin, uint32_t end) noexcept {
    std::get<TermAttr>(_attrs).value =
      bytes_view{_data.data() + begin, end - begin};
    auto& offs = std::get<OffsAttr>(_attrs);
    offs.start = begin;
    offs.end = end;
  }

  size_t StackSize() const noexcept { return _stack.size() - _head; }

  Options _options;
  bytes_view _data;
  attributes _attrs;
  std::vector<HashAndPos> _stack;
  std::vector<uint64_t> _pending;
  uint64_t* _pending_out{nullptr};
  std::vector<uint32_t> _hashes;
  size_t _hash_base{0};
  size_t _hash_end{0};
  size_t _head{0};
  size_t _pending_size{0};
  size_t _pending_idx{0};
  size_t _pos{0};
  bool _finalized{true};
};

}  // namespace irs::analysis
