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
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <optional>
#include <vector>

#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/index/merge/merged_postings.hpp"
#include "iresearch/index/merge/term_keys.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct TermReader;

template<typename Postings>
class CompoundTermIterator : public TermOnlyIterator {
 public:
  CompoundTermIterator(const CompoundTermIterator&) = delete;
  CompoundTermIterator& operator=(const CompoundTermIterator&) = delete;

  static constexpr const size_t kProgressStepTerms = size_t{1} << 7;

  explicit CompoundTermIterator(const MergeProgress& progress)
    : _doc_itr{progress},
      _proceed{&progress},
      _progress{progress, kProgressStepTerms} {}

  bool Aborted() const {
    return _failed || !static_cast<bool>(_progress) || _doc_itr.Aborted();
  }

  void Reset(const FieldMeta& meta) noexcept {
    _current_term = {};
    _meta = &meta;
    _doc_itr.Clear();
    _term_iterator_mask.clear();
    _term_iterators.clear();
    _keys.clear();
    _merge.reset();
    _merge_state.reset();
    _runs.clear();
    _run = 0;
    _run_left = 0;
    _pending.reset();
    _pending_at_term = false;
    _min_term.clear();
    _max_term.clear();
    _has_min_term = false;
  }

  const FieldMeta& Meta() const noexcept { return *_meta; }

  void Add(const TermReader& reader, const DocRemap& remap);

  Attribute* GetMutable(TypeInfo::type_id) noexcept final {
    SDB_ASSERT(false);
    return nullptr;
  }

  bool next() final;

  TermPostings::ptr postings(IndexFeatures features) const final;

  bytes_view value() const noexcept final {
    if (!_has_min_term) [[unlikely]] {
      _has_min_term = true;
      _min_term = _current_term;
    }
    SDB_ASSERT(_max_term <= _current_term);
    _max_term = _current_term;
    return _current_term;
  }

  bytes_view MinTerm() const noexcept { return _min_term; }
  bytes_view MaxTerm() const noexcept { return _max_term; }

 private:
  struct TermIteratorImpl {
    SeekTermIterator::ptr it;
    const DocRemap* remap;
    mutable TermPostings::ptr postings;
    uint64_t group = 0;
  };

  void Start();
  std::optional<size_t> NextEntry();
  bool Refill();
  bytes_view Advance(size_t input);
  void Join(size_t input);

  bytes_view _current_term;
  const FieldMeta* _meta{};
  std::vector<size_t> _term_iterator_mask;
  std::vector<TermIteratorImpl> _term_iterators;
  std::vector<duckdb::shared_ptr<TermKeys>> _keys;
  duckdb::shared_ptr<duckdb::PresortedMerge> _merge;
  duckdb::unique_ptr<duckdb::PresortedMergeState> _merge_state;
  duckdb::vector<duckdb::PresortedRun> _runs;
  size_t _run{0};
  uint64_t _run_left{0};
  uint64_t _groups{0};
  std::optional<size_t> _pending;
  bool _pending_at_term{false};
  bool _failed{false};
  mutable bstring _min_term;
  mutable bstring _max_term;
  mutable Postings _doc_itr;
  mutable bool _has_min_term{false};
  const MergeProgress* _proceed;
  ProgressTracker _progress;
};

}  // namespace irs
