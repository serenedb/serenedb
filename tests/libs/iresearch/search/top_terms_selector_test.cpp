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

#include <cstdint>

#include "basics/down_cast.h"
#include "basics/memory.hpp"
#include "iresearch/formats/empty_term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/top_terms_selector.hpp"
#include "tests_shared.hpp"

namespace {

struct TestPostingMeta : irs::PostingMeta {
  TestPostingMeta(uint32_t docs_count = 0, uint32_t freq = 0) noexcept {
    this->docs_count = docs_count;
    this->freq = freq;
  }
};

class TestSeekTermIterator : public irs::SeekTermIterator {
 public:
  typedef const std::pair<std::string_view, TestPostingMeta>* IteratorType;

  TestSeekTermIterator(IteratorType begin, IteratorType end)
    : _begin(begin), _end(end), _cookie_ptr(begin) {}

  irs::SeekResult seek_ge(irs::bytes_view) final {
    return irs::SeekResult::NotFound;
  }

  bool seek(irs::bytes_view) final { return false; }

  const irs::PostingMeta& cookie() const final { return _meta; }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    return type == irs::Type<irs::TermAttr>::id() ? &_value : nullptr;
  }

  bool next() noexcept final {
    if (_begin == _end) {
      return false;
    }

    _value.value = irs::ViewCast<irs::byte_type>(_begin->first);
    _cookie_ptr = _begin;
    _meta = _begin->second;
    // Two terms of one segment can carry identical counts (`D` and `J` below
    // are both {5,5}), so a meta alone does not say *which* term the selector
    // picked. Stamping the entry's identity into a field nothing else reads
    // keeps that assertion, which the pointer-valued cookie used to give.
    _meta.doc_start = reinterpret_cast<uintptr_t>(_begin);
    ++_begin;
    return true;
  }

  irs::bytes_view value() const noexcept final { return _value.value; }

  irs::TermPostings::ptr postings(irs::IndexFeatures /*features*/) const final {
    return irs::TermPostings::empty();
  }

  struct SeekPtr {
    explicit SeekPtr(IteratorType ptr) noexcept : ptr(ptr) {}

    IteratorType ptr;
  };

 private:
  TestPostingMeta _meta;
  irs::TermAttr _value;
  IteratorType _begin;
  IteratorType _end;
  IteratorType _cookie_ptr;
};

struct SubReader final : irs::SubReader {
  explicit SubReader(size_t num_docs) {
    info.docs_count = num_docs;
    info.live_docs_count = num_docs;
  }

  uint64_t CountMappedMemory() const final { return 0; }

  const irs::SegmentInfo& Meta() const final { return info; }

  const irs::DocumentMask* docs_mask() const final { return nullptr; }

  irs::lead::Node::ptr docs_iterator() const final {
    return irs::SubReader::empty().docs_iterator();
  }
  const irs::TermReader* field(irs::field_id) const final { return nullptr; }
  std::span<const irs::field_id> field_ids() const final { return {}; }
  irs::NormReader::ptr norms(irs::field_id) const final { return nullptr; }

  irs::SegmentInfo info;
};

struct State {
  struct SegmentState {
    const irs::TermReader* field;
    uint32_t docs_count;
    std::vector<const std::pair<std::string_view, TestPostingMeta>*> cookies;
  };

  std::map<const irs::SubReader*, SegmentState> segments;
};

struct StateVisitor {
  void operator()(const irs::SubReader& segment, const irs::TermReader& field,
                  uint32_t docs) const {
    auto it = expected_state.segments.find(&segment);
    ASSERT_NE(it, expected_state.segments.end());
    ASSERT_EQ(it->second.field, &field);
    ASSERT_EQ(it->second.docs_count, docs);
    expected_cookie = it->second.cookies.begin();
  }

  // A cookie is a value now, so the collected term carries its counts -- and
  // the identity the iterator stamped, which is what pins *which* term the
  // selector picked rather than merely one with the same counts.
  void operator()(irs::PostingMeta& cookie) const {
    ASSERT_EQ((*expected_cookie)->second.docs_count, cookie.docs_count);
    ASSERT_EQ((*expected_cookie)->second.freq, cookie.freq);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(*expected_cookie), cookie.doc_start);

    ++expected_cookie;
  }

  mutable decltype(State::SegmentState::cookies)::const_iterator
    expected_cookie;
  const struct State& expected_state;
};

}  // namespace

TEST(TopTermsSelector_test, test_top_k) {
  using CollectorType =
    irs::TopTermsSelector<irs::TopTermState<irs::byte_type>>;
  CollectorType collector(5);

  // segment 0
  irs::EmptyTermReader term_reader0(42);
  SubReader segment0(100);
  const std::pair<std::string_view, TestPostingMeta> term_s0[]{
    {"A", {3, 3}}, {"C", {15, 15}}, {"E", {2, 2}}, {"G", {5, 5}}, {"I", {1, 1}},
  };

  {
    TestSeekTermIterator it(std::begin(term_s0), std::end(term_s0));
    collector.Prepare(segment0, term_reader0, it);

    while (it.next()) {
      collector.Visit(it.value().front());
    }
  }

  // segment 1
  irs::EmptyTermReader term_reader1(42);
  SubReader segment1(100);
  const std::pair<std::string_view, TestPostingMeta> term_s1[]{
    {"B", {3, 3}}, {"D", {5, 5}}, {"F", {2, 2}}, {"H", {15, 15}}, {"J", {5, 5}},
  };

  {
    TestSeekTermIterator it(std::begin(term_s1), std::end(term_s1));
    collector.Prepare(segment1, term_reader1, it);

    while (it.next()) {
      collector.Visit(it.value().front());
    }
  }

  // top-5 distinct terms by key (byte value): J, I, H, G, F
  std::map<char, State> expected_states{
    {'J', {{{&segment1, {&term_reader1, 5, {term_s1 + 4}}}}}},
    {'I', {{{&segment0, {&term_reader0, 1, {term_s0 + 4}}}}}},
    {'H', {{{&segment1, {&term_reader1, 15, {term_s1 + 3}}}}}},
    {'G', {{{&segment0, {&term_reader0, 5, {term_s0 + 3}}}}}},
    {'F', {{{&segment1, {&term_reader1, 2, {term_s1 + 2}}}}}},
  };

  auto visitor = [&expected_states](CollectorType::state_type& state) {
    auto it = expected_states.find(char(state.key));
    ASSERT_NE(it, expected_states.end());
    ASSERT_EQ(it->first, state.key);
    ASSERT_EQ(irs::bstring(1, irs::byte_type(it->first)), state.term);

    ::StateVisitor state_visitor{{}, it->second};

    state.Visit(state_visitor);
  };

  collector.Visit(visitor);
}

TEST(TopTermsSelector_test, test_top_0) {
  using CollectorType =
    irs::TopTermsSelector<irs::TopTermState<irs::byte_type>>;
  CollectorType collector(0);  // same as collector(1)

  // segment 0
  irs::EmptyTermReader term_reader0(42);
  SubReader segment0(100);
  const std::pair<std::string_view, TestPostingMeta> term_s0[]{
    {"A", {3, 3}}, {"C", {15, 15}}, {"E", {2, 2}}, {"G", {5, 5}}, {"I", {1, 1}},
  };

  {
    TestSeekTermIterator it(std::begin(term_s0), std::end(term_s0));
    collector.Prepare(segment0, term_reader0, it);

    while (it.next()) {
      collector.Visit(it.value().front());
    }
  }

  // segment 1
  irs::EmptyTermReader term_reader1(42);
  SubReader segment1(100);
  const std::pair<std::string_view, TestPostingMeta> term_s1[]{
    {"B", {3, 3}}, {"D", {5, 5}}, {"F", {2, 2}}, {"H", {15, 15}}, {"J", {5, 5}},
  };

  {
    TestSeekTermIterator it(std::begin(term_s1), std::end(term_s1));
    collector.Prepare(segment1, term_reader1, it);

    while (it.next()) {
      collector.Visit(it.value().front());
    }
  }

  // limit 0 behaves as limit 1: only the single highest-key term survives
  std::map<char, State> expected_states{
    {'J', {{{&segment1, {&term_reader1, 5, {term_s1 + 4}}}}}},
  };

  auto visitor = [&expected_states](CollectorType::state_type& state) {
    auto it = expected_states.find(char(state.key));
    ASSERT_NE(it, expected_states.end());
    ASSERT_EQ(it->first, state.key);
    ASSERT_EQ(irs::bstring(1, irs::byte_type(it->first)), state.term);

    ::StateVisitor state_visitor{{}, it->second};

    state.Visit(state_visitor);
  };

  collector.Visit(visitor);
}
