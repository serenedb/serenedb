////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <vector>

#include "basics/noncopyable.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/top_k_heap.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {

template<typename T>
struct TopTerm {
  using key_type = T;

  template<typename U = key_type>
  TopTerm(const bytes_view& term, U&& key)
    : term(term.data(), term.size()), key(std::forward<U>(key)) {}

  template<typename SelectorState>
  void emplace(const SelectorState& /*state*/) {
    // NOOP
  }

  template<typename Visitor>
  void Visit(const Visitor& /*visitor*/) const {
    // NOOP
  }

  bstring term;
  key_type key;
};

template<typename T>
struct TopTermComparer {
  bool operator()(const TopTerm<T>& lhs, const TopTerm<T>& rhs) const noexcept {
    return operator()(lhs, rhs.key, rhs.term);
  }

  bool operator()(const TopTerm<T>& lhs, const T& rhs_key,
                  const bytes_view& rhs_term) const noexcept {
    return lhs.key < rhs_key || (!(rhs_key < lhs.key) && lhs.term < rhs_term);
  }
};

template<typename T>
struct TopTermState : TopTerm<T> {
  struct SegmentState {
    SegmentState(const SubReader& segment, const TermReader& field,
                 uint32_t docs_count) noexcept
      : segment(&segment), field(&field), docs_count(docs_count) {}

    const SubReader* segment;
    const TermReader* field;
    size_t terms_count{1};  // number of terms in a segment
    uint32_t docs_count;
  };

  template<typename U = T>
  TopTermState(const bytes_view& term, U&& key)
    : TopTerm<T>(term, std::forward<U>(key)) {}

  template<typename SelectorState>
  void emplace(const SelectorState& state) {
    SDB_ASSERT(state.segment && state.docs_count && state.field);

    const auto* segment = state.segment;
    const auto docs_count = *state.docs_count;

    if (segments.empty() || segments.back().segment != segment) {
      segments.emplace_back(*segment, *state.field, docs_count);
    } else {
      auto& segment = segments.back();
      ++segment.terms_count;
      segment.docs_count += docs_count;
    }
    terms.emplace_back(state.terms->cookie());
  }

  template<typename Visitor>
  void Visit(const Visitor& visitor) {
    auto cookie = terms.begin();
    for (auto& segment : segments) {
      visitor(*segment.segment, *segment.field, segment.docs_count);
      for (size_t i = 0, size = segment.terms_count; i < size; ++i, ++cookie) {
        visitor(*cookie);
      }
    }
  }

  std::vector<SegmentState> segments;
  std::vector<SeekCookie::ptr> terms;
};

template<typename State,
         typename Comparer = TopTermComparer<typename State::key_type>>
class TopTermsSelector : private util::Noncopyable {
 public:
  using state_type = State;
  static_assert(std::is_nothrow_move_assignable_v<state_type>);
  using key_type = typename state_type::key_type;
  using comparer_type = Comparer;

  // We disallow 0 size collectors for consistency since we're not
  // interested in this use case and don't want to burden "collect(...)"
  explicit TopTermsSelector(size_t size, const Comparer& comp = {})
    : _comparer{comp}, _heap{std::max(size_t(1), size), comp} {}

  // Prepare scorer for terms collecting
  // `segment` segment reader for the current term
  // `state` state containing this scored term
  // `terms` segment term-iterator positioned at the current term
  void Prepare(const SubReader& segment, const TermReader& field,
               const SeekTermIterator& terms) noexcept {
    _state.segment = &segment;
    _state.field = &field;
    _state.terms = &terms;

    if (auto* term = irs::get<TermAttr>(terms)) [[likely]] {
      _state.term = &term->value;
    } else {
      SDB_ASSERT(false);
      static constexpr bytes_view kNoTerm;
      _state.term = &kNoTerm;
    }

    if (auto* meta = irs::get<TermMeta>(terms)) [[likely]] {
      _state.docs_count = &meta->docs_count;
    } else {
      static constexpr doc_id_t kNoDocs = 0;
      _state.docs_count = &kNoDocs;
    }
  }

  // Collect current term
  void Visit(const key_type& key) {
    const auto term = *_state.term;

    if (_heap.Full() && !_comparer(_heap.Min(), key, term)) {
      return;
    }

    state_type state{term, key};
    state.emplace(_state);
    _heap.Push(std::move(state));
  }

  // FIXME rename
  template<typename Visitor>
  void Visit(const Visitor& visitor) noexcept {
    for (auto& entry : _heap.Finalize()) {
      visitor(entry);
    }
  }

 private:
  // Collector state
  struct SelectorState {
    const SubReader* segment{};
    const TermReader* field{};
    const SeekTermIterator* terms{};
    const bytes_view* term{};
    const uint32_t* docs_count{};
  };

  [[no_unique_address]] comparer_type _comparer;
  SelectorState _state;
  TopKHeap<state_type, comparer_type> _heap;
};

}  // namespace irs
