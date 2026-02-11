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

#include <basics/memory.hpp>
#include <iresearch/formats/empty_term_reader.hpp>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/collectors.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/multiterm_query.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/search/top_terms_collector.hpp>

#include "basics/down_cast.h"
#include "tests_shared.hpp"

namespace {

struct TestTermMeta : irs::TermMeta {
  TestTermMeta(uint32_t docs_count = 0, uint32_t freq = 0) noexcept {
    this->docs_count = docs_count;
    this->freq = freq;
  }
};

}  // namespace

namespace irs {

// use base irs::TermMeta type for ancestors
template<>
struct Type<::TestTermMeta> : Type<irs::TermMeta> {};

}  // namespace irs

namespace {

struct Sort : irs::Scorer {
  struct FieldCollector final : irs::FieldCollector {
    uint64_t docs_with_field = 0;  // number of documents containing the matched
                                   // field (possibly without matching terms)
    uint64_t total_term_freq = 0;  // number of terms for processed field

    void collect(const irs::SubReader&, const irs::TermReader& field) final {
      docs_with_field += field.docs_count();

      auto* freq = irs::get<irs::FreqAttr>(field);

      if (freq) {
        total_term_freq += freq->value;
      }
    }

    void reset() noexcept final {
      docs_with_field = 0;
      total_term_freq = 0;
    }

    void collect(irs::bytes_view) final {}
    void write(irs::DataOutput&) const final {}
  };

  struct TermCollector final : irs::TermCollector {
    uint64_t docs_with_term =
      0;  // number of documents containing the matched term

    void collect(const irs::SubReader&, const irs::TermReader&,
                 const irs::AttributeProvider& term_attrs) final {
      auto* meta = irs::get<irs::TermMeta>(term_attrs);

      if (meta) {
        docs_with_term += meta->docs_count;
      }
    }

    void reset() noexcept final { docs_with_term = 0; }

    void collect(irs::bytes_view) final {}
    void write(irs::DataOutput&) const final {}
  };

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::None;
  }

  irs::WandWriter::ptr prepare_wand_writer(size_t) const final {
    return nullptr;
  }

  irs::WandSource::ptr prepare_wand_source() const final { return nullptr; }

  irs::FieldCollector::ptr PrepareFieldCollector() const final {
    return std::make_unique<FieldCollector>();
  }

  irs::TermCollector::ptr PrepareTermCollector() const final {
    return std::make_unique<TermCollector>();
  }

  irs::ScoreFunction PrepareScorer(const irs::ColumnProvider& /*segment*/,
                                   const irs::FieldProperties& /*field*/,
                                   const irs::byte_type* /*stats*/,
                                   const irs::AttributeProvider& /*doc_attrs*/,
                                   irs::score_t /*boost*/) const final {
    return irs::ScoreFunction::Default(1);
  }

  std::pair<size_t, size_t> stats_size() const final { return {0, 0}; }
};

class TestSeekTermIterator : public irs::SeekTermIterator {
 public:
  typedef const std::pair<std::string_view, TestTermMeta>* IteratorType;

  TestSeekTermIterator(IteratorType begin, IteratorType end)
    : _begin(begin), _end(end), _cookie_ptr(begin) {}

  irs::SeekResult seek_ge(irs::bytes_view) final {
    return irs::SeekResult::NotFound;
  }

  bool seek(irs::bytes_view) final { return false; }

  irs::SeekCookie::ptr cookie() const final {
    return std::make_unique<struct SeekPtr>(_cookie_ptr);
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<decltype(_meta)>::id()) {
      return &_meta;
    }
    if (type == irs::Type<irs::TermAttr>::id()) {
      return &_value;
    }
    return nullptr;
  }

  bool next() noexcept final {
    if (_begin == _end) {
      return false;
    }

    _value.value = irs::ViewCast<irs::byte_type>(_begin->first);
    _cookie_ptr = _begin;
    _meta = _begin->second;
    ++_begin;
    return true;
  }

  irs::bytes_view value() const noexcept final { return _value.value; }

  void read() final {}

  irs::DocIterator::ptr postings(irs::IndexFeatures /*features*/) const final {
    return irs::DocIterator::empty();
  }

  struct SeekPtr final : irs::SeekCookie {
    explicit SeekPtr(IteratorType ptr) noexcept : ptr(ptr) {}

    irs::Attribute* GetMutable(irs::TypeInfo::type_id) final { return nullptr; }

    bool IsEqual(const irs::SeekCookie& rhs) const noexcept final {
      return ptr == sdb::basics::downCast<SeekPtr>(rhs).ptr;
    }

    size_t Hash() const noexcept final { return absl::HashOf(ptr->first); }

    IteratorType ptr;
  };

 private:
  TestTermMeta _meta;
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

  irs::ColumnIterator::ptr columns() const final {
    return irs::ColumnIterator::empty();
  }
  const irs::ColumnReader* column(irs::field_id) const final { return nullptr; }
  const irs::ColumnReader* column(std::string_view) const final {
    return nullptr;
  }
  irs::DocIterator::ptr docs_iterator() const final {
    return irs::DocIterator::empty();
  }
  const irs::TermReader* field(std::string_view) const final { return nullptr; }
  irs::FieldIterator::ptr fields() const final {
    return irs::FieldIterator::empty();
  }
  const irs::ColumnReader* sort() const final { return nullptr; }

  irs::SegmentInfo info;
};

struct State {
  struct SegmentState {
    const irs::TermReader* field;
    uint32_t docs_count;
    std::vector<const std::pair<std::string_view, TestTermMeta>*> cookies;
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

  void operator()(irs::SeekCookie::ptr& cookie) const {
    auto* cookie_impl =
      static_cast<const ::TestSeekTermIterator::SeekPtr*>(cookie.get());

    ASSERT_EQ(*expected_cookie, cookie_impl->ptr);

    ++expected_cookie;
  }

  mutable decltype(State::SegmentState::cookies)::const_iterator
    expected_cookie;
  const struct State& expected_state;
};

}  // namespace

TEST(TopTermsCollector_test, test_top_k) {
  using CollectorType =
    irs::TopTermsCollector<irs::TopTermState<irs::byte_type>>;
  CollectorType collector(5);

  // segment 0
  irs::EmptyTermReader term_reader0(42);
  SubReader segment0(100);
  const std::pair<std::string_view, TestTermMeta> term_s0[]{
    {"F", {1, 1}}, {"G", {2, 2}},   {"H", {3, 3}},  {"B", {3, 3}},
    {"C", {3, 3}}, {"A", {3, 3}},   {"H", {2, 2}},  {"D", {5, 5}},
    {"E", {5, 5}}, {"I", {15, 15}}, {"J", {5, 25}}, {"K", {15, 35}},
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
  const std::pair<std::string_view, TestTermMeta> term_s1[]{
    {"F", {1, 1}}, {"G", {2, 2}}, {"H", {3, 3}},   {"B", {3, 3}},
    {"C", {3, 3}}, {"A", {3, 3}}, {"K", {15, 35}},
  };

  {
    TestSeekTermIterator it(std::begin(term_s1), std::end(term_s1));
    collector.Prepare(segment1, term_reader1, it);

    while (it.next()) {
      collector.Visit(it.value().front());
    }
  }

  std::map<char, State> expected_states{
    {'J', {{{&segment0, {&term_reader0, 5, {term_s0 + 10}}}}}},
    {'K',
     {{{&segment0, {&term_reader0, 15, {term_s0 + 11}}},
       {&segment1, {&term_reader1, 15, {term_s1 + 6}}}}}},
    {'I', {{{&segment0, {&term_reader0, 15, {term_s0 + 9}}}}}},
    {'H',
     {{{&segment0, {&term_reader0, 5, {term_s0 + 2, term_s0 + 6}}},
       {&segment1, {&term_reader1, 3, {term_s1 + 2}}}}}},
    {'G', {{{&segment0, {&term_reader0, 2, {term_s0 + 1}}}}}},
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

TEST(TopTermsCollector_test, test_top_0) {
  using CollectorType =
    irs::TopTermsCollector<irs::TopTermState<irs::byte_type>>;
  CollectorType collector(0);  // same as collector(1)

  // segment 0
  irs::EmptyTermReader term_reader0(42);
  SubReader segment0(100);
  const std::pair<std::string_view, TestTermMeta> term_s0[]{
    {"F", {1, 1}}, {"G", {2, 2}},   {"H", {3, 3}},  {"B", {3, 3}},
    {"C", {3, 3}}, {"A", {3, 3}},   {"H", {2, 2}},  {"D", {5, 5}},
    {"E", {5, 5}}, {"I", {15, 15}}, {"J", {5, 25}}, {"K", {15, 35}},
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
  const std::pair<std::string_view, TestTermMeta> term_s1[]{
    {"F", {1, 1}}, {"G", {2, 2}}, {"H", {3, 3}},   {"B", {3, 3}},
    {"C", {3, 3}}, {"A", {3, 3}}, {"K", {15, 35}},
  };

  {
    TestSeekTermIterator it(std::begin(term_s1), std::end(term_s1));
    collector.Prepare(segment1, term_reader1, it);

    while (it.next()) {
      collector.Visit(it.value().front());
    }
  }

  std::map<char, State> expected_states{
    {'K',
     {{{&segment0, {&term_reader0, 15, {term_s0 + 11}}},
       {&segment1, {&term_reader1, 15, {term_s1 + 6}}}}}},
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
