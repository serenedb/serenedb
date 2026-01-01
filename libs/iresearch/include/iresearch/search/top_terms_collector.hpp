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

#include <unordered_map>
#include <vector>

#include "basics/noncopyable.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/hash_utils.hpp"

namespace irs {

template<typename T>
struct TopTerm {
  using key_type = T;

  template<typename U = key_type>
  TopTerm(const bytes_view& term, U&& key)
    : term(term.data(), term.size()), key(std::forward<U>(key)) {}

  template<typename CollectorState>
  void emplace(const CollectorState& /*state*/) {
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

  template<typename CollectorState>
  void emplace(const CollectorState& state) {
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
class TopTermsCollector : private util::Noncopyable {
 public:
  using state_type = State;
  static_assert(std::is_nothrow_move_assignable_v<state_type>);
  using key_type = typename state_type::key_type;
  using comparer_type = Comparer;

  // We disallow 0 size collectors for consistency since we're not
  // interested in this use case and don't want to burden "collect(...)"
  explicit TopTermsCollector(size_t size, const Comparer& comp = {})
    : _comparer{comp}, _size{std::max(size_t(1), size)} {
    _heap.reserve(size);
    _terms.reserve(size);  // ensure all iterators remain valid
  }

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

    if (_left) {
      hashed_bytes_view hashed_term{term};
      const auto res = _terms.try_emplace(hashed_term, hashed_term, key);

      if (res.second) {
        auto& old_key = const_cast<hashed_bytes_view&>(res.first->first);
        auto& value = res.first->second;
        hashed_bytes_view new_key{value.term, old_key.Hash()};
        SDB_ASSERT(old_key == new_key);
        old_key = new_key;
        _heap.emplace_back(res.first);

        if (!--_left) {
          make_heap();
        }
      }

      res.first->second.emplace(_state);

      return;
    }

    const auto min = _heap.front();
    const auto& min_state = min->second;

    if (!_comparer(min_state, key, term)) {
      // nothing to do
      return;
    }

    const auto hashed_term = hashed_bytes_view{term};
    const auto it = _terms.find(hashed_term);

    if (it == _terms.end()) {
      // update min entry
      pop();

      auto node = _terms.extract(min);
      SDB_ASSERT(!node.empty());
      node.mapped() = state_type{term, key};
      node.key() = hashed_bytes_view{node.mapped().term, hashed_term.Hash()};
      auto res = _terms.insert(std::move(node));
      SDB_ASSERT(res.inserted);
      SDB_ASSERT(res.node.empty());

      _heap.back() = res.position;
      push();

      res.position->second.emplace(_state);
    } else {
      SDB_ASSERT(it->second.key == key);
      // update existing entry
      it->second.emplace(_state);
    }
  }

  // FIXME rename
  template<typename Visitor>
  void Visit(const Visitor& visitor) noexcept {
    for (auto& entry : _terms) {
      visitor(entry.second);
    }
  }

 private:
  // We don't use absl hash table implementation as it doesn't guarantee
  // rehashing won't happen even if enough buckets are reserve()ed
  using states_map_t =
    std::unordered_map<hashed_bytes_view, state_type, HashedStrHash>;

  // Collector state
  struct CollectorState {
    const SubReader* segment{};
    const TermReader* field{};
    const SeekTermIterator* terms{};
    const bytes_view* term{};
    const uint32_t* docs_count{};
  };

  void make_heap() noexcept {
    absl::c_make_heap(_heap, [&](const auto lhs, const auto rhs) noexcept {
      return _comparer(rhs->second, lhs->second);
    });
  }

  void push() noexcept {
    absl::c_push_heap(_heap, [&](const auto lhs, const auto rhs) noexcept {
      return _comparer(rhs->second, lhs->second);
    });
  }

  void pop() noexcept {
    absl::c_pop_heap(_heap, [&](const auto lhs, const auto rhs) noexcept {
      return _comparer(rhs->second, lhs->second);
    });
  }

  [[no_unique_address]] comparer_type _comparer;
  CollectorState _state;
  std::vector<typename states_map_t::iterator> _heap;
  states_map_t _terms;
  size_t _size;
  size_t _left{_size};
};

}  // namespace irs
