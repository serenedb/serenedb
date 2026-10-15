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
#include "iresearch/utils/memory.hpp"

namespace irs {

template<typename Postings>
void CompoundTermIterator<Postings>::Add(const TermReader& reader,
                                         const DocRemap& remap) {
  auto it = reader.iterator();
  SDB_ASSERT(it);
  _term_iterator_mask.emplace_back(_term_iterators.size());
  _term_iterators.emplace_back(std::move(it), &remap);
}

template<typename Postings>
bool CompoundTermIterator<Postings>::next() {
  _progress();

  if (Aborted()) {
    _term_iterators.clear();
    _term_iterator_mask.clear();
    return false;
  }

  for (const auto itr_id : _term_iterator_mask) {
    auto& it = _term_iterators[itr_id].it;
    SDB_ASSERT(it);
    if (!it->next()) {
      it.reset();
    }
  }

  _term_iterator_mask.clear();
  _current_term = {};

  for (size_t i = 0, count = _term_iterators.size(); i != count; ++i) {
    auto& it = _term_iterators[i].it;
    if (!it) {
      continue;
    }
    const bytes_view value = it->value();
    SDB_ASSERT(!IsNull(value));
    SDB_ASSERT(_term_iterator_mask.empty() == IsNull(_current_term));
    if (!IsNull(_current_term)) {
      const auto cmp = value.compare(_current_term);
      if (cmp > 0) {
        continue;
      }
      if (cmp < 0) {
        _term_iterator_mask.clear();
      }
    }
    _current_term = value;
    _term_iterator_mask.emplace_back(i);
  }

  return !IsNull(_current_term);
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
