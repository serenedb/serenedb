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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <concepts>
#include <limits>
#include <span>
#include <type_traits>
#include <vector>

#include "basics/containers/small_vector.h"
#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::ngram {

struct Position {
  Position() = default;

  template<typename Iterator>
    requires(!std::same_as<std::remove_cvref_t<Iterator>, Position>)
  explicit Position(Iterator& itr) noexcept
    : doc{&itr.value()}, pos{&PosAttr::get(itr)} {
    SDB_ASSERT(pos);
  }

  Position(const doc_id_t& doc, PosAttr& pos) noexcept : doc{&doc}, pos{&pos} {}

  const doc_id_t* doc = nullptr;
  PosAttr* pos = nullptr;
};

struct PositionWithOffset : Position {
  PositionWithOffset() = default;

  template<typename Iterator>
    requires(!std::same_as<std::remove_cvref_t<Iterator>, PositionWithOffset>)
  explicit PositionWithOffset(Iterator& itr) noexcept
    : Position{itr}, offs{irs::get<OffsAttr>(*this->pos)} {
    SDB_ASSERT(offs);
  }

  PositionWithOffset(const doc_id_t& doc, PosAttr& pos) noexcept
    : Position{doc, pos}, offs{irs::get<OffsAttr>(pos)} {
    SDB_ASSERT(offs);
  }

  const OffsAttr* offs = nullptr;
};

template<bool IsStart, typename T>
uint32_t GetOffset(const T& pos) noexcept {
  if constexpr (std::is_same_v<PositionWithOffset, T>) {
    if constexpr (IsStart) {
      return pos.offs->start;
    } else {
      return pos.offs->end;
    }
  } else {
    return 0;
  }
}

struct SearchState {
  static constexpr uint32_t kNoParent = std::numeric_limits<uint32_t>::max();

  template<typename T>
  SearchState(uint32_t p, const T& attrs)
    : origin{attrs.pos},
      len{1},
      pos{p},
      start{GetOffset<true>(attrs)},
      offs{start} {}

  template<typename T>
  SearchState(const SearchState& other, uint32_t idx, uint32_t p,
              const T& attrs)
    : origin{attrs.pos},
      parent{idx},
      len{other.len + 1},
      pos{p},
      start{other.start},
      offs{GetOffset<false>(attrs)} {}

  const PosAttr* origin;
  uint32_t parent{kNoParent};
  uint32_t len;
  uint32_t pos;
  uint32_t start;
  uint32_t offs;
};

struct FlatState {
  template<typename T>
  FlatState(uint32_t p, const T& attrs) : origin{attrs.pos}, len{1}, pos{p} {}

  template<typename T>
  FlatState(const FlatState& other, uint32_t p, const T& attrs)
    : origin{attrs.pos}, len{other.len + 1}, pos{p} {}

  const PosAttr* origin;
  uint32_t len;
  uint32_t pos;
};

struct Dummy {};

class NGramPosition : public PosAttr {
 public:
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return type == irs::Type<OffsAttr>::id() ? &_offset : nullptr;
  }

  bool next() final {
    if (_begin == std::end(_offsets)) {
      return false;
    }

    _offset = *_begin;
    ++_begin;
    return true;
  }

  void reset() final {
    _begin = std::begin(_offsets);
    _value = irs::pos_limits::invalid();
  }

  void ClearOffsets() noexcept {
    _offsets.clear();
    _begin = std::end(_offsets);
  }

  void PushOffset(const SearchState& state) {
    SDB_ASSERT(state.start <= state.offs);
    _offsets.emplace_back(OffsAttr{{}, state.start, state.offs});
  }

  std::span<const OffsAttr> Offsets() const noexcept {
    return {_offsets.data(), _offsets.size()};
  }

 private:
  using OffsetsBuffer = sdb::containers::SmallVector<OffsAttr, 16>;

  OffsAttr _offset;
  OffsetsBuffer _offsets;
  OffsetsBuffer::const_iterator _begin{std::begin(_offsets)};
};

template<typename Base, bool CollectAll = true, size_t N = 0>
class SerialPositionsChecker final : public Base {
 public:
  static constexpr bool kHasPosition = std::is_same_v<NGramPosition, Base>;
  static constexpr bool kCollectAll = CollectAll;

  SerialPositionsChecker(size_t size, size_t total_terms_count,
                         size_t min_match_count = 1)
    : _pos(size),
      _min_match_count{min_match_count},
      _total_terms_count{static_cast<score_t>(total_terms_count)} {}

  bool Match(size_t potential, irs::doc_id_t doc);

  Attribute* GetMutableAttr(TypeInfo::type_id type) noexcept {
    if constexpr (kHasPosition) {
      if (type == irs::Type<PosAttr>::id()) {
        return static_cast<Base*>(this);
      }
    }

    return nullptr;
  }

  score_t GetBoost() const noexcept { return _filter_boost; }
  uint32_t GetFreq() const noexcept { return _seq_freq; }

  std::span<const OffsAttr> Offsets() const noexcept
    requires(kHasPosition)
  {
    return static_cast<const Base&>(*this).Offsets();
  }

  using PositionType =
    std::conditional_t<kHasPosition, PositionWithOffset, Position>;

  PositionType& Slot(size_t i) noexcept {
    SDB_ASSERT(i < _pos.size());
    return _pos[i];
  }

 private:
  friend class NGramPosition;

  using StateRef = std::conditional_t<CollectAll, uint32_t, FlatState>;

  struct Candidates {
    struct Entry {
      uint32_t pos;
      StateRef state;
      [[no_unique_address]] utils::Need<CollectAll, bool> used;
    };
    using iterator = typename std::vector<Entry>::iterator;

    void clear() noexcept { _entries.clear(); }
    bool empty() const noexcept { return _entries.empty(); }
    auto begin() noexcept { return _entries.rbegin(); }
    auto end() noexcept { return _entries.rend(); }
    iterator Nil() noexcept { return _entries.end(); }
    const Entry& Last() const noexcept { return _entries.front(); }
    iterator Toward(iterator it) noexcept {
      return it == _entries.begin() ? _entries.end() : std::prev(it);
    }

    iterator LowerBound(uint32_t pos) noexcept {
      const auto it = Above(pos);
      return it == _entries.begin() ? _entries.end() : std::prev(it);
    }

    iterator Find(uint32_t pos) noexcept {
      const auto it = LowerBound(pos);
      return it != _entries.end() && it->pos == pos ? it : _entries.end();
    }

    bool TryEmplace(uint32_t pos, StateRef state) {
      const auto it = Above(pos);
      if (it != _entries.begin() && std::prev(it)->pos == pos) {
        return false;
      }
      _entries.emplace(it, Entry{pos, state, {}});
      return true;
    }

   private:
    iterator Above(uint32_t pos) noexcept {
      return std::upper_bound(
        _entries.begin(), _entries.end(), pos,
        [](uint32_t lhs, const Entry& rhs) noexcept { return lhs < rhs.pos; });
    }

    std::vector<Entry> _entries;
  };

  using SearchStates = Candidates;
  using PosTemp = std::vector<std::pair<uint32_t, StateRef>>;

  IRS_FORCE_INLINE const SearchState& State(uint32_t idx) const noexcept {
    SDB_ASSERT(idx < _states.size());
    return _states[idx];
  }

  IRS_FORCE_INLINE const auto& Deref(const StateRef& ref) const noexcept {
    if constexpr (CollectAll) {
      return State(ref);
    } else {
      return ref;
    }
  }

  template<typename T>
  IRS_FORCE_INLINE StateRef Begun(uint32_t p, const T& attrs) {
    if constexpr (CollectAll) {
      _states.emplace_back(p, attrs);
      return static_cast<uint32_t>(_states.size() - 1);
    } else {
      return FlatState{p, attrs};
    }
  }

  template<typename T>
  IRS_FORCE_INLINE StateRef Appended(const StateRef& parent, uint32_t p,
                                     const T& attrs) {
    if constexpr (CollectAll) {
      SearchState state{State(parent), parent, p, attrs};
      _states.push_back(state);
      return static_cast<uint32_t>(_states.size() - 1);
    } else {
      return FlatState{parent, p, attrs};
    }
  }

  using States = utils::Need<CollectAll, std::vector<SearchState>>;

  irs::search::RunOf<PositionType, N> _pos;
  std::vector<const PosAttr*> _longest_sequence;
  std::vector<uint32_t> _pos_sequence;
  size_t _min_match_count;
  SearchStates _search_buf;
  [[no_unique_address]] States _states;
  PosTemp _swap_cache;
  score_t _total_terms_count;
  score_t _filter_boost = kNoBoost;
  uint32_t _seq_freq = 0;
};

template<typename Base, bool CollectAll, size_t N>
bool SerialPositionsChecker<Base, CollectAll, N>::Match(size_t potential,
                                                        doc_id_t doc) {
  _search_buf.clear();
  if constexpr (CollectAll) {
    _states.clear();
  }
  uint32_t longest_sequence_len = 0;

  _seq_freq = 0;
  for (const auto& pos_iterator : _pos) {
    if (*pos_iterator.doc == doc) {
      auto& pos = *pos_iterator.pos;
      if (potential <= longest_sequence_len || potential < _min_match_count) {
        SDB_ASSERT(!_search_buf.empty());
        pos.seek(_search_buf.Last().pos + 1);
      } else {
        pos.next();
      }
      if (!pos_limits::eof(pos.value())) {
        _swap_cache.clear();
        auto last_found_pos = pos_limits::invalid();
        do {
          const auto current_pos = pos.value();
          if (auto found = _search_buf.LowerBound(current_pos);
              found != _search_buf.Nil()) {
            if (last_found_pos != found->pos) {
              last_found_pos = found->pos;
              const auto* found_state = &Deref(found->state);
              auto current_sequence = found;
              uint32_t current_found_len{
                (found->pos == current_pos ||
                 found_state->origin == pos_iterator.pos)
                  ? 0
                  : found_state->len + 1};
              auto initial_found = found;
              if (current_found_len > longest_sequence_len) {
                longest_sequence_len = current_found_len;
              } else {
                for (found = _search_buf.Toward(found);
                     found != _search_buf.Nil();
                     found = _search_buf.Toward(found)) {
                  found_state = &Deref(found->state);
                  if (found_state->origin != pos_iterator.pos &&
                      found_state->len + 1 > current_found_len) {
                    current_sequence = found;
                    current_found_len = found_state->len + 1;
                    if (current_found_len > longest_sequence_len) {
                      longest_sequence_len = current_found_len;
                      break;
                    }
                  }
                }
              }
              if (current_found_len) {
                auto new_candidate =
                  Appended(current_sequence->state, current_pos, pos_iterator);
                if (!_search_buf.TryEmplace(current_pos, new_candidate)) {
                  _swap_cache.emplace_back(current_pos,
                                           std::move(new_candidate));
                }
              } else if (Deref(initial_found->state).origin ==
                           pos_iterator.pos &&
                         potential > longest_sequence_len &&
                         potential >= _min_match_count) {
                _search_buf.TryEmplace(current_pos,
                                       Begun(current_pos, pos_iterator));
              }
            }
          } else if (potential > longest_sequence_len &&
                     potential >= _min_match_count) {
            _search_buf.TryEmplace(current_pos,
                                   Begun(current_pos, pos_iterator));
            if (!longest_sequence_len) {
              longest_sequence_len = 1;
            }
          }
          if constexpr (!CollectAll) {
            if (longest_sequence_len >= _min_match_count) {
              return true;
            }
          }
        } while (pos.next());
        for (auto& p : _swap_cache) {
          auto res = _search_buf.Find(p.first);
          SDB_ASSERT(res != _search_buf.Nil());
          std::swap(res->state, p.second);
        }
      }
      --potential;

      if (!potential) {
        break;
      }

      if (longest_sequence_len + potential < _min_match_count) {
        break;
      }
    }
  }

  if constexpr (CollectAll) {
    if (longest_sequence_len >= _min_match_count) {
      if constexpr (kHasPosition) {
        static_cast<NGramPosition&>(*this).ClearOffsets();
      }

      uint32_t freq{0};
      size_t count_longest{0};
      [[maybe_unused]] const SearchState* last_state{};

      for (auto& entry : _search_buf) {
        const auto& state = State(entry.state);
        if (state.len == longest_sequence_len) {
          ++count_longest;
          if constexpr (kHasPosition) {
            last_state = &state;
          }
          if (count_longest > 1) {
            break;
          }
        }
      }

      if (count_longest > 1) {
        const auto used = [&](uint32_t p) noexcept {
          const auto it = _search_buf.Find(p);
          SDB_ASSERT(it != _search_buf.Nil());
          return it->used;
        };
        _longest_sequence.clear();
        _longest_sequence.reserve(longest_sequence_len);
        _pos_sequence.reserve(longest_sequence_len);
        for (auto i = _search_buf.begin(); i != _search_buf.end(); ++i) {
          _pos_sequence.clear();
          const auto* state = &State(i->state);
          SDB_ASSERT(state->len <= longest_sequence_len);
          if (state->len != longest_sequence_len) {
            continue;
          }
          bool delete_candidate = false;
          if (_longest_sequence.empty()) {
            _longest_sequence.push_back(state->origin);
            _pos_sequence.push_back(state->pos);
            for (auto p = state->parent; p != SearchState::kNoParent;) {
              const auto& cur_parent = State(p);
              _longest_sequence.push_back(cur_parent.origin);
              _pos_sequence.push_back(cur_parent.pos);
              p = cur_parent.parent;
            }
          } else {
            if (used(state->pos) || state->origin != _longest_sequence[0]) {
              delete_candidate = true;
            } else {
              _pos_sequence.push_back(state->pos);
              size_t j = 1;
              for (auto p = state->parent; p != SearchState::kNoParent;) {
                const auto& cur_parent = State(p);
                SDB_ASSERT(j < _longest_sequence.size());
                if (_longest_sequence[j] != cur_parent.origin ||
                    used(cur_parent.pos)) {
                  delete_candidate = true;
                  break;
                }
                _pos_sequence.push_back(cur_parent.pos);
                p = cur_parent.parent;
                ++j;
              }
            }
          }
          if (!delete_candidate) {
            ++freq;
            for (const auto p : _pos_sequence) {
              const auto it = _search_buf.Find(p);
              SDB_ASSERT(it != _search_buf.Nil());
              it->used = true;
            }

            if constexpr (kHasPosition) {
              static_cast<NGramPosition&>(*this).PushOffset(*state);
            }
          }
        }
      } else {
        freq = 1;
        if constexpr (kHasPosition) {
          SDB_ASSERT(last_state);
          static_cast<NGramPosition&>(*this).PushOffset(*last_state);
        }
      }
      _seq_freq = freq;
      SDB_ASSERT(!_pos.empty());
      _filter_boost =
        static_cast<score_t>(longest_sequence_len) / _total_terms_count;

      if constexpr (kHasPosition) {
        static_cast<NGramPosition&>(*this).reset();
      }
    }
  }
  return longest_sequence_len >= _min_match_count;
}

}  // namespace irs::ngram
