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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "iresearch/index/index_features.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class CompoundPostings : public TermPostings {
 public:
  struct PostingsT {
    TermPostings::ptr it;
    const DocRemap* remap;
    size_t source;
  };
  using IteratorsT = std::vector<PostingsT>;

  static constexpr auto kProgressStepDocs = size_t{1} << size_t{14};

  explicit CompoundPostings(const MergeProgress& progress) noexcept
    : _progress(progress, kProgressStepDocs) {}

  template<typename Func>
  void Reset(Func&& func, IndexFeatures features) {
    func(_iterators);
    _current_itr = 0;
    _at = 0;
    _size = 0;
    _has_freq = IndexFeatures::None != (features & IndexFeatures::Freq);
    _has_pos = IndexFeatures::None != (features & IndexFeatures::Pos);
  }

  void Clear() noexcept { _iterators.clear(); }

  bool Aborted() const noexcept { return !static_cast<bool>(_progress); }

  uint32_t NextDocs(doc_id_t* docs, uint32_t* freqs) final;

  void NextPositions(uint32_t* pos, uint32_t* offs_start, uint32_t* offs_len,
                     uint32_t n) final {
    SDB_ASSERT(_current_itr < _iterators.size());
    _iterators[_current_itr].it->NextPositions(pos, offs_start, offs_len, n);
  }

 private:
  uint32_t NextLive(TermPostings& it, const DocRemap& remap, doc_id_t* docs,
                    uint32_t* freqs);

  std::vector<PostingsT> _iterators;
  size_t _current_itr{0};
  ProgressTracker _progress;
  doc_id_t _docs[doc_limits::kBlockSize];
  uint32_t _freqs[doc_limits::kBlockSize];
  uint32_t _at{0};
  uint32_t _size{0};
  bool _has_freq{false};
  bool _has_pos{false};
};

class SortedPostings : public TermPostings {
 public:
  using PostingsT = CompoundPostings::PostingsT;
  using IteratorsT = CompoundPostings::IteratorsT;

  static constexpr auto kProgressStepDocs = CompoundPostings::kProgressStepDocs;

  explicit SortedPostings(const MergeProgress& progress) noexcept
    : _progress(progress, kProgressStepDocs) {}

  template<typename Func>
  void Reset(Func&& func, IndexFeatures features) {
    func(_iterators);
    _has_freq = IndexFeatures::None != (features & IndexFeatures::Freq);
    _has_pos = IndexFeatures::None != (features & IndexFeatures::Pos);
    _cursors.resize(_iterators.size());
    for (size_t i = 0; i != _cursors.size(); ++i) {
      auto& c = _cursors[i];
      c.at = 0;
      c.size = 0;
      c.skip = 0;
      Settle(i);
    }
    _recorded = 0;
    _walked = 0;
    _left = 0;
  }

  void Clear() noexcept {
    _iterators.clear();
    _cursors.clear();
  }

  bool Aborted() const noexcept { return !static_cast<bool>(_progress); }

  uint32_t NextDocs(doc_id_t* docs, uint32_t* freqs) final;

  void NextPositions(uint32_t* pos, uint32_t* offs_start, uint32_t* offs_len,
                     uint32_t n) final;

 private:
  struct Cursor {
    doc_id_t docs[doc_limits::kBlockSize];
    uint32_t freqs[doc_limits::kBlockSize];
    uint32_t at = 0;
    uint32_t size = 0;
    uint64_t skip = 0;
    doc_id_t head = doc_limits::eof();
  };

  void Settle(size_t i);

  std::vector<PostingsT> _iterators;
  std::vector<Cursor> _cursors;
  ProgressTracker _progress;
  size_t _input[doc_limits::kBlockSize];
  uint32_t _freq[doc_limits::kBlockSize];
  uint64_t _skip[doc_limits::kBlockSize];
  uint32_t _recorded = 0;
  uint32_t _walked = 0;
  uint64_t _left = 0;
  size_t _current = 0;
  bool _has_freq = false;
  bool _has_pos = false;
};

}  // namespace irs
