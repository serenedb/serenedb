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

#include "iresearch/index/merge/merged_terms.hpp"

#include <utility>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

template<typename Postings>
void CompoundTermIterator<Postings>::Add(const TermReader& reader,
                                         const DocRemap& remap) {
  auto it = reader.iterator();
  SDB_ASSERT(it);
  _term_iterators.emplace_back(std::move(it), &remap);
  _keys.emplace_back(duckdb::make_shared_ptr<TermKeys>(reader));
}

template<typename Postings>
void CompoundTermIterator<Postings>::Start() {
  duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> inputs;
  inputs.reserve(_keys.size());
  for (const auto& keys : _keys) {
    inputs.emplace_back(keys);
  }
  _merge = duckdb::make_shared_ptr<duckdb::PresortedMerge>(std::move(inputs));
  _merge->Partition(1);
  _merge_state = _merge->BeginMerge(0);
}

template<typename Postings>
bool CompoundTermIterator<Postings>::Refill() {
  if (_merge_state->Done()) {
    return false;
  }
  _runs.clear();
  _run = 0;
  duckdb::optional_idx unsorted;
  if (!_merge->Merge(*_merge_state, _runs, STANDARD_VECTOR_SIZE, *_proceed,
                     unsorted)) {
    if (unsorted.IsValid()) {
      SDB_ERROR(IRESEARCH, "Terms of field ", Meta().id, " in merge input ",
                unsorted.GetIndex(), " are not sorted");
    }
    _failed = true;
    return false;
  }
  return !_runs.empty();
}

template<typename Postings>
std::optional<size_t> CompoundTermIterator<Postings>::NextEntry() {
  while (_run_left == 0) {
    if (_run + 1 < _runs.size()) {
      ++_run;
    } else if (!Refill()) {
      return std::nullopt;
    }
    _run_left = _runs[_run].count;
  }
  --_run_left;
  return static_cast<size_t>(_runs[_run].input);
}

template<typename Postings>
bytes_view CompoundTermIterator<Postings>::Advance(size_t input) {
  auto& it = *_term_iterators[input].it;
  const bool next = it.next();
  SDB_ENSURE(next, "CompoundTermIterator: merge input ", input, " of field ",
             Meta().id, " ran out of terms");
  return it.value();
}

template<typename Postings>
void CompoundTermIterator<Postings>::Join(size_t input) {
  _term_iterators[input].group = _groups;
  _term_iterator_mask.emplace_back(input);
}

template<typename Postings>
bool CompoundTermIterator<Postings>::next() {
  _progress();

  if (Aborted()) {
    _term_iterators.clear();
    _term_iterator_mask.clear();
    return false;
  }

  if (!_merge) {
    Start();
  }

  _term_iterator_mask.clear();
  _current_term = {};
  ++_groups;

  auto head = std::exchange(_pending, std::nullopt);
  if (!head) {
    head = NextEntry();
    if (!head) {
      return false;
    }
  }
  _current_term = std::exchange(_pending_at_term, false)
                    ? _term_iterators[*head].it->value()
                    : Advance(*head);
  Join(*head);

  while (const auto entry = NextEntry()) {
    if (_term_iterators[*entry].group == _groups) {
      _pending = entry;
      break;
    }
    const auto term = Advance(*entry);
    if (term != _current_term) {
      SDB_ASSERT(term > _current_term);
      _pending = entry;
      _pending_at_term = true;
      break;
    }
    Join(*entry);
  }

  return !Aborted();
}

template<typename Postings>
TermPostings::ptr CompoundTermIterator<Postings>::postings(
  IndexFeatures /*features*/) const {
  auto add_iterators = [this](typename Postings::IteratorsT& itrs) {
    const auto* const empty = TermPostings::empty().get();
    for (auto& entry : itrs) {
      if (entry.it.get() != empty) {
        _term_iterators[entry.source].postings = std::move(entry.it);
      }
    }
    itrs.clear();
    itrs.reserve(_term_iterator_mask.size());
    for (auto& itr_id : _term_iterator_mask) {
      auto& term_itr = _term_iterators[itr_id];
      SDB_ASSERT(term_itr.it);
      auto it = term_itr.it->ReusePostings(Meta().index_features,
                                           std::move(term_itr.postings));
      SDB_ASSERT(it);
      itrs.emplace_back(std::move(it), term_itr.remap, itr_id);
    }
  };

  _doc_itr.Reset(add_iterators, Meta().index_features);
  return memory::to_managed<TermPostings>(_doc_itr);
}

template class CompoundTermIterator<CompoundPostings>;
template class CompoundTermIterator<SortedPostings>;

}  // namespace irs
