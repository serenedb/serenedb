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
#include <functional>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/top_k_heap.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

template<typename T>
struct TopTerm {
  using key_type = T;

  template<typename U = key_type>
  TopTerm(const bytes_view& term, U&& key)
    : term(term.data(), term.size()), key(std::forward<U>(key)) {}

  template<typename SelectorState>
  void emplace(const SelectorState&) {}

  template<typename Visitor>
  void Visit(const Visitor&) const {}

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
    return lhs.key < rhs_key || (!(rhs_key < lhs.key) && lhs.term > rhs_term);
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
    size_t terms_count{1};
    uint32_t docs_count;
  };

  template<typename U = T>
  TopTermState(const bytes_view& term, U&& key)
    : TopTerm<T>(term, std::forward<U>(key)) {}

  template<typename SelectorState>
  void emplace(const SelectorState& state) {
    SDB_ASSERT(state.segment && state.terms && state.field);

    const auto* segment = state.segment;
    const auto& meta = state.terms->cookie();
    const auto docs_count = meta.docs_count;

    if (segments.empty() || segments.back().segment != segment) {
      segments.emplace_back(*segment, *state.field, docs_count);
    } else {
      auto& segment = segments.back();
      ++segment.terms_count;
      segment.docs_count += docs_count;
    }
    terms.emplace_back(meta);
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
  std::vector<PostingMeta> terms;
};

struct TermSelectorState {
  void Bind(const SubReader* owner, const TermReader& term_reader,
            TermIterator& iterator) noexcept {
    segment = owner;
    field = &term_reader;
    terms = &iterator;
    if (auto* attr = irs::get<TermAttr>(iterator)) [[likely]] {
      term = &attr->value;
    } else {
      SDB_ASSERT(false);
      static constexpr bytes_view kNoTerm;
      term = &kNoTerm;
    }
  }

  const SubReader* segment{};
  const TermReader* field{};
  TermIterator* terms{};
  const bytes_view* term{};
};

template<typename State,
         typename Comparer = TopTermComparer<typename State::key_type>>
class TopTermsSelector : private util::Noncopyable {
 public:
  using state_type = State;
  static_assert(std::is_nothrow_move_assignable_v<state_type>);
  using key_type = typename state_type::key_type;
  using comparer_type = Comparer;

  explicit TopTermsSelector(size_t size, const Comparer& comp = {})
    : _comparer{comp}, _heap{std::max(size_t(1), size), comp} {}

  void Prepare(const SubReader& segment, const TermReader& field,
               TermIterator& terms) noexcept {
    _state.Bind(&segment, field, terms);
  }

  void Prepare(const TermReader& field, TermIterator& terms) noexcept {
    _state.Bind(nullptr, field, terms);
  }

  bool Visit(const key_type& key) {
    const auto term = *_state.term;

    if (_heap.Full() && !_comparer(_heap.Min(), key, term)) {
      return true;
    }

    state_type state{term, key};
    state.emplace(_state);
    _heap.Push(std::move(state));
    return true;
  }

  template<typename Visitor>
  void Visit(const Visitor& visitor) noexcept {
    for (auto& entry : _heap.Finalize()) {
      visitor(entry);
    }
  }

 private:
  [[no_unique_address]] comparer_type _comparer;
  TermSelectorState _state;
  TopKHeap<state_type, comparer_type> _heap;
};

template<typename State>
class TiedTermsSelector : private util::Noncopyable {
 public:
  using state_type = State;
  using key_type = typename state_type::key_type;

  explicit TiedTermsSelector(size_t size)
    : _size{std::max(size_t{1}, size)}, _compact_at{2 * _size} {}

  void Prepare(const SubReader& segment, const TermReader& field,
               TermIterator& terms) noexcept {
    _state.Bind(&segment, field, terms);
  }

  void Prepare(const TermReader& field, TermIterator& terms) noexcept {
    _state.Bind(nullptr, field, terms);
  }

  bool Visit(const key_type& key) {
    if (_keys.size() != _size) {
      _keys.push_back(key);
      std::push_heap(_keys.begin(), _keys.end(), std::greater<>{});
    } else if (key < _keys.front()) {
      return true;
    } else if (_keys.front() < key) {
      std::pop_heap(_keys.begin(), _keys.end(), std::greater<>{});
      _keys.back() = key;
      std::push_heap(_keys.begin(), _keys.end(), std::greater<>{});
    }
    state_type state{*_state.term, key};
    state.emplace(_state);
    _states.push_back(std::move(state));
    if (_states.size() == _compact_at) {
      Compact();
    }
    return true;
  }

  template<typename Visitor>
  void Visit(const Visitor& visitor) {
    Compact();
    for (auto& entry : _states) {
      visitor(entry);
    }
  }

 private:
  void Compact() {
    if (_keys.size() == _size) {
      const auto bound = _keys.front();
      std::erase_if(_states,
                    [&](const state_type& state) { return state.key < bound; });
    }
    _compact_at = std::max(2 * _states.size(), 2 * _size);
  }

  TermSelectorState _state;
  std::vector<key_type> _keys;
  std::vector<state_type> _states;
  size_t _size;
  size_t _compact_at;
};

}  // namespace irs
