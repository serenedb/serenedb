////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrei Abramov
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "ngram_similarity_query.hpp"

#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"  // for SubReader/TermReader definitions
#include "iresearch/search/min_match_disjunction.hpp"
#include "iresearch/search/ngram_similarity_filter.hpp"

namespace irs {
namespace {

struct Position {
  template<typename Iterator>
  explicit Position(Iterator& itr) noexcept
    : pos{&PosAttr::GetMutable(itr)},
      doc{irs::get<DocAttr>(itr)},
      scr{&irs::ScoreAttr::get(itr)} {
    SDB_ASSERT(pos);
    SDB_ASSERT(doc);
    SDB_ASSERT(scr);
  }

  PosAttr* pos;
  const DocAttr* doc;
  const ScoreAttr* scr;
};

struct PositionWithOffset : Position {
  template<typename Iterator>
  explicit PositionWithOffset(Iterator& itr) noexcept
    : Position{itr}, offs{irs::get<OffsAttr>(*this->pos)} {
    SDB_ASSERT(offs);
  }

  const OffsAttr* offs;
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
  template<typename T>
  SearchState(uint32_t p, const T& attrs)
    : scr{attrs.scr}, len{1}, pos{p}, offs{GetOffset<true>(attrs)} {}

  // appending constructor
  template<typename T>
  SearchState(std::shared_ptr<SearchState> other, uint32_t p, const T& attrs)
    : parent{std::move(other)},
      scr{attrs.scr},
      len{parent->len + 1},
      pos{p},
      offs{GetOffset<false>(attrs)} {}

  std::shared_ptr<SearchState> parent;
  const ScoreAttr* scr;
  uint32_t len;
  uint32_t pos;
  uint32_t offs;
};

using SearchStates =
  std::map<uint32_t, std::shared_ptr<SearchState>, std::greater<>>;

template<bool FullMatch>
class NGramApprox : public MinMatchDisjunction<NoopAggregator> {
  using Base = MinMatchDisjunction<NoopAggregator>;

 public:
  using Base::Base;
};

template<>
class NGramApprox<true> : public Conjunction<CostAdapter<>, NoopAggregator> {
  using Base = Conjunction<CostAdapter<>, NoopAggregator>;

 public:
  NGramApprox(CostAdapters&& itrs, size_t min_match_count)
    : Base{NoopAggregator{},
           [](auto&& itrs) {
             absl::c_sort(itrs, [](const auto& lhs, const auto& rhs) noexcept {
               return lhs.est < rhs.est;
             });
             return std::move(itrs);
           }(std::move(itrs))},
      _match_count{min_match_count} {}

  size_t MatchCount() const noexcept { return _match_count; }

 private:
  size_t _match_count;
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
    _offsets.emplace_back(OffsetFromState(state));
  }

 private:
  static OffsAttr OffsetFromState(const SearchState& state) noexcept {
    auto* cur = &state;
    for (auto* next = state.parent.get(); next;) {
      cur = next;
      next = next->parent.get();
    }

    SDB_ASSERT(cur->offs <= state.offs);
    return {{}, cur->offs, state.offs};
  }

  using Offsets = sdb::containers::SmallVector<OffsAttr, 16>;

  OffsAttr _offset;
  Offsets _offsets;
  Offsets::const_iterator _begin{std::begin(_offsets)};
};

template<typename Base>
class SerialPositionsChecker final : public Base {
 public:
  static constexpr bool kHasPosition = std::is_same_v<NGramPosition, Base>;

  template<typename Iterator>
  SerialPositionsChecker(Iterator begin, Iterator end, size_t total_terms_count,
                         size_t min_match_count = 1,
                         bool collect_all_states = false)
    : _pos(begin, end),
      _min_match_count{min_match_count},
      // avoid runtime conversion
      _total_terms_count{static_cast<score_t>(total_terms_count)},
      _collect_all_states{collect_all_states} {}

  bool Check(size_t potential, irs::doc_id_t doc);

  Attribute* GetMutableAttr(TypeInfo::type_id type) noexcept {
    if (type == irs::Type<FreqAttr>::id()) {
      return &_seq_freq;
    }

    if (type == irs::Type<FilterBoost>::id()) {
      return &_filter_boost;
    }

    if constexpr (kHasPosition) {
      if (type == irs::Type<PosAttr>::id()) {
        return static_cast<Base*>(this);
      }
    }

    return nullptr;
  }

 private:
  friend class NGramPosition;

  using SearchStates =
    std::map<uint32_t, std::shared_ptr<SearchState>, std::greater<>>;
  using PosTemp =
    std::vector<std::pair<uint32_t, std::shared_ptr<SearchState>>>;

  using PositionType =
    std::conditional_t<kHasPosition, PositionWithOffset, Position>;

  std::vector<PositionType> _pos;
  std::set<size_t> _used_pos;  // longest sequence positions overlaping detector
  std::vector<const ScoreAttr*> _longest_sequence;
  std::vector<size_t> _pos_sequence;
  size_t _min_match_count;
  SearchStates _search_buf;
  score_t _total_terms_count;
  FilterBoost _filter_boost;
  FreqAttr _seq_freq;
  bool _collect_all_states;
};

template<typename Base>
bool SerialPositionsChecker<Base>::Check(size_t potential, doc_id_t doc) {
  // how long max sequence could be in the best case
  _search_buf.clear();
  uint32_t longest_sequence_len = 0;

  _seq_freq.value = 0;
  for (const auto& pos_iterator : _pos) {
    if (pos_iterator.doc->value == doc) {
      PosAttr& pos = *(pos_iterator.pos);
      if (potential <= longest_sequence_len || potential < _min_match_count) {
        // this term could not start largest (or long enough) sequence.
        // skip it to first position to append to any existing candidates
        SDB_ASSERT(!_search_buf.empty());
        pos.seek(std::rbegin(_search_buf)->first + 1);
      } else {
        pos.next();
      }
      if (!pos_limits::eof(pos.value())) {
        PosTemp swap_cache;
        auto last_found_pos = pos_limits::invalid();
        do {
          const auto current_pos = pos.value();
          if (auto found = _search_buf.lower_bound(current_pos);
              found != std::end(_search_buf)) {
            if (last_found_pos != found->first) {
              last_found_pos = found->first;
              const auto* found_state = found->second.get();
              SDB_ASSERT(found_state);
              auto current_sequence = found;
              // if we hit same position - set length to 0 to force checking
              // candidates to the left
              uint32_t current_found_len{(found->first == current_pos ||
                                          found_state->scr == pos_iterator.scr)
                                           ? 0
                                           : found_state->len + 1};
              auto initial_found = found;
              if (current_found_len > longest_sequence_len) {
                longest_sequence_len = current_found_len;
              } else {
                // maybe some previous candidates could produce better
                // results. lets go leftward and check if there are any
                // candidates which could became longer if we stick this ngram
                // to them rather than the closest one found
                for (++found; found != std::end(_search_buf); ++found) {
                  found_state = found->second.get();
                  SDB_ASSERT(found_state);
                  if (found_state->scr != pos_iterator.scr &&
                      found_state->len + 1 > current_found_len) {
                    // we have better option. Replace this match!
                    current_sequence = found;
                    current_found_len = found_state->len + 1;
                    if (current_found_len > longest_sequence_len) {
                      longest_sequence_len = current_found_len;
                      break;  // this match is the best - nothing to search
                              // further
                    }
                  }
                }
              }
              if (current_found_len) {
                auto new_candidate = std::make_shared<SearchState>(
                  current_sequence->second, current_pos, pos_iterator);
                const auto res = _search_buf.try_emplace(
                  current_pos, std::move(new_candidate));
                if (!res.second) {
                  // pos already used. This could be if same ngram used several
                  // times. Replace with new length through swap cache - to not
                  // spoil candidate for following positions of same ngram
                  swap_cache.emplace_back(current_pos,
                                          std::move(new_candidate));
                }
              } else if (initial_found->second->scr == pos_iterator.scr &&
                         potential > longest_sequence_len &&
                         potential >= _min_match_count) {
                // we just hit same iterator and found no better place to
                // join, so it will produce new candidate
                _search_buf.emplace(
                  std::piecewise_construct, std::forward_as_tuple(current_pos),
                  std::forward_as_tuple(
                    std::make_shared<SearchState>(current_pos, pos_iterator)));
              }
            }
          } else if (potential > longest_sequence_len &&
                     potential >= _min_match_count) {
            // this ngram at this position  could potentially start a long
            // enough sequence so add it to candidate list
            _search_buf.emplace(
              std::piecewise_construct, std::forward_as_tuple(current_pos),
              std::forward_as_tuple(
                std::make_shared<SearchState>(current_pos, pos_iterator)));
            if (!longest_sequence_len) {
              longest_sequence_len = 1;
            }
          }
        } while (pos.next());
        for (auto& p : swap_cache) {
          auto res = _search_buf.find(p.first);
          SDB_ASSERT(res != std::end(_search_buf));
          std::swap(res->second, p.second);
        }
      }
      --potential;  // we are done with this term.
                    // next will have potential one less as less matches left

      if (!potential) {
        break;  // all further terms will not add anything
      }

      if (longest_sequence_len + potential < _min_match_count) {
        break;  // all further terms will not let us build long enough
                // sequence
      }

      // if we have no scoring - we could stop searh once we got enough
      // matches
      if (longest_sequence_len >= _min_match_count && !_collect_all_states) {
        break;
      }
    }
  }

  if (longest_sequence_len >= _min_match_count && _collect_all_states) {
    if constexpr (kHasPosition) {
      static_cast<NGramPosition&>(*this).ClearOffsets();
    }

    uint32_t freq{0};
    size_t count_longest{0};
    [[maybe_unused]] SearchState* last_state{};

    // try to optimize case with one longest candidate
    // performance profiling shows it is majority of cases
    for ([[maybe_unused]] auto& [_, state] : _search_buf) {
      if (state->len == longest_sequence_len) {
        ++count_longest;
        if constexpr (kHasPosition) {
          last_state = state.get();
        }
        if (count_longest > 1) {
          break;
        }
      }
    }

    if (count_longest > 1) {
      _longest_sequence.clear();
      _used_pos.clear();
      _longest_sequence.reserve(longest_sequence_len);
      _pos_sequence.reserve(longest_sequence_len);
      for (auto i = std::begin(_search_buf); i != std::end(_search_buf);) {
        _pos_sequence.clear();
        const auto* state = i->second.get();
        SDB_ASSERT(state && state->len <= longest_sequence_len);
        if (state->len == longest_sequence_len) {
          bool delete_candidate = false;
          // only first longest sequence will contribute to frequency
          if (_longest_sequence.empty()) {
            _longest_sequence.push_back(state->scr);
            _pos_sequence.push_back(state->pos);
            auto cur_parent = state->parent;
            while (cur_parent) {
              _longest_sequence.push_back(cur_parent->scr);
              _pos_sequence.push_back(cur_parent->pos);
              cur_parent = cur_parent->parent;
            }
          } else {
            if (_used_pos.find(state->pos) != std::end(_used_pos) ||
                state->scr != _longest_sequence[0]) {
              delete_candidate = true;
            } else {
              _pos_sequence.push_back(state->pos);
              auto cur_parent = state->parent;
              size_t j = 1;
              while (cur_parent) {
                SDB_ASSERT(j < _longest_sequence.size());
                if (_longest_sequence[j] != cur_parent->scr ||
                    _used_pos.find(cur_parent->pos) != std::end(_used_pos)) {
                  delete_candidate = true;
                  break;
                }
                _pos_sequence.push_back(cur_parent->pos);
                cur_parent = cur_parent->parent;
                ++j;
              }
            }
          }
          if (!delete_candidate) {
            ++freq;
            _used_pos.insert(std::begin(_pos_sequence),
                             std::end(_pos_sequence));

            if constexpr (kHasPosition) {
              static_cast<NGramPosition&>(*this).PushOffset(*state);
            }
          }
        }
        ++i;
      }
    } else {
      freq = 1;
      if constexpr (kHasPosition) {
        SDB_ASSERT(last_state);
        static_cast<NGramPosition&>(*this).PushOffset(*last_state);
      }
    }
    _seq_freq.value = freq;
    SDB_ASSERT(!_pos.empty());
    _filter_boost.value =
      static_cast<score_t>(longest_sequence_len) / _total_terms_count;

    if constexpr (kHasPosition) {
      static_cast<NGramPosition&>(*this).reset();
    }
  }
  return longest_sequence_len >= _min_match_count;
}

template<typename Approx, typename Checker>
class NGramSimilarityDocIterator : public DocIterator, private ScoreCtx {
 public:
  NGramSimilarityDocIterator(CostAdapters&& itrs, size_t total_terms_count,
                             size_t min_match_count, bool collect_all_states)
    : _checker{std::begin(itrs), std::end(itrs), total_terms_count,
               min_match_count, collect_all_states},
      // we are not interested in disjunction`s // scoring
      _approx{std::move(itrs), min_match_count} {
    // avoid runtime conversion
    std::get<AttributePtr<DocAttr>>(_attrs) =
      irs::GetMutable<DocAttr>(&_approx);

    // FIXME find a better estimation
    std::get<AttributePtr<CostAttr>>(_attrs) =
      irs::GetMutable<CostAttr>(&_approx);
  }

  NGramSimilarityDocIterator(CostAdapters&& itrs, const SubReader& segment,
                             const TermReader& field, score_t boost,
                             const byte_type* stats, size_t total_terms_count,
                             size_t min_match_count = 1,
                             const Scorers& ord = Scorers::kUnordered)
    : NGramSimilarityDocIterator{std::move(itrs), total_terms_count,
                                 min_match_count, !ord.empty()} {
    if (!ord.empty()) {
      auto& score = std::get<irs::ScoreAttr>(_attrs);
      CompileScore(score, ord.buckets(), segment, field, stats, *this, boost);
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    auto* attr = irs::GetMutable(_attrs, type);

    return attr != nullptr ? attr : _checker.GetMutableAttr(type);
  }

  doc_id_t value() const final {
    return std::get<AttributePtr<DocAttr>>(_attrs).ptr->value;
  }

  bool next() final {
    while (_approx.next()) {
      if (_checker.Check(_approx.MatchCount(), value())) {
        return true;
      }
    }
    return false;
  }

  doc_id_t seek(doc_id_t target) final {
    const auto prev = value();
    const auto curr = _approx.seek(target);
    if (prev == curr || _checker.Check(_approx.MatchCount(), curr)) {
      return curr;
    }
    next();
    return value();
  }

 private:
  using Attributes =
    std::tuple<AttributePtr<DocAttr>, AttributePtr<CostAttr>, ScoreAttr>;

  Checker _checker;
  Approx _approx;
  Attributes _attrs;
};

CostAdapters Execute(const NGramState& query_state,
                     IndexFeatures required_features,
                     IndexFeatures extra_features) {
  const auto* field = query_state.reader;

  if (field == nullptr ||
      required_features != (field->meta().index_features & required_features)) {
    return {};
  }

  required_features |= extra_features;

  CostAdapters itrs;
  itrs.reserve(query_state.terms.size());

  for (const auto& term_state : query_state.terms) {
    if (term_state == nullptr) [[unlikely]] {
      continue;
    }

    if (auto docs = field->postings(*term_state, required_features)) {
      auto& it = itrs.emplace_back(std::move(docs));

      if (!it) [[unlikely]] {
        itrs.pop_back();
      }
    }
  }

  return itrs;
}

}  // namespace

DocIterator::ptr NGramSimilarityQuery::execute(
  const ExecutionContext& ctx) const {
  const auto& ord = ctx.scorers;
  SDB_ASSERT(1 != _min_match_count || !ord.empty());

  const auto& segment = ctx.segment;
  const auto* query_state = _states.find(segment);

  if (query_state == nullptr) {
    return DocIterator::empty();
  }

  auto itrs = Execute(*query_state, kRequiredFeatures, ord.features());

  if (itrs.size() < _min_match_count) {
    return DocIterator::empty();
  }
  // TODO(mbkkt) itrs.size() == 1: return itrs_[0], but needs to add score
  // optimization for single ngram case
  if (itrs.size() == _min_match_count) {
    return memory::make_managed<NGramSimilarityDocIterator<
      NGramApprox<true>, SerialPositionsChecker<Dummy>>>(
      std::move(itrs), segment, *query_state->reader, _boost, _stats.c_str(),
      query_state->terms.size(), _min_match_count, ord);
  }
  // TODO(mbkkt) min_match_count_ == 1: disjunction for approx,
  // optimization for low threshold case
  return memory::make_managed<NGramSimilarityDocIterator<
    NGramApprox<false>, SerialPositionsChecker<Dummy>>>(
    std::move(itrs), segment, *query_state->reader, _boost, _stats.c_str(),
    query_state->terms.size(), _min_match_count, ord);
}

DocIterator::ptr NGramSimilarityQuery::ExecuteWithOffsets(
  const SubReader& rdr) const {
  const auto* query_state = _states.find(rdr);

  if (query_state == nullptr) {
    return DocIterator::empty();
  }

  auto itrs = Execute(*query_state, kRequiredFeatures | IndexFeatures::Offs,
                      IndexFeatures::None);

  if (itrs.size() < _min_match_count) {
    return DocIterator::empty();
  }
  // TODO(mbkkt) itrs.size() == 1: return itrs_[0], but needs to add score
  // optimization for single ngram case
  if (itrs.size() == _min_match_count) {
    return memory::make_managed<NGramSimilarityDocIterator<
      NGramApprox<true>, SerialPositionsChecker<NGramPosition>>>(
      std::move(itrs), query_state->terms.size(), _min_match_count, true);
  }
  // TODO(mbkkt) min_match_count_ == 1: disjunction for approx,
  // optimization for low threshold case
  return memory::make_managed<NGramSimilarityDocIterator<
    NGramApprox<false>, SerialPositionsChecker<NGramPosition>>>(
    std::move(itrs), query_state->terms.size(), _min_match_count, true);
}

}  // namespace irs
