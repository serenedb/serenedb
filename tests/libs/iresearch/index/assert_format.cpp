////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "assert_format.hpp"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <iostream>
#include <iresearch/analysis/token_sinks.hpp>
#include <unordered_set>

#include "basics/bit_utils.hpp"
#include "basics/down_cast.h"
#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/index/comparer.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/directory_reader_impl.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/states/term_state.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_iterator.hpp"
#include "iresearch/search/term_predicate.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/fstext/fst_table_matcher.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

namespace tests {

void AssertTerm(size_t segment_index, size_t field_index, size_t term_index,
                const irs::TermIterator& expected_term,
                const irs::TermIterator& actual_term,
                irs::IndexFeatures requested_features);

namespace {

bool MemcmpLess(const irs::byte_type* lhs, size_t lhs_size,
                const irs::byte_type* rhs, size_t rhs_size) noexcept {
  SDB_ASSERT(lhs && rhs);
  const size_t size = std::min(lhs_size, rhs_size);
  const auto res = ::memcmp(lhs, rhs, size);
  if (0 == res) {
    return lhs_size < rhs_size;
  }
  return res < 0;
}

}  // namespace

void Posting::insert(uint32_t pos) {
  _positions.emplace(pos, std::numeric_limits<uint32_t>::max(),
                     std::numeric_limits<uint32_t>::max(), irs::bytes_view{});
}

void Posting::insert(uint32_t pos, uint32_t offs_start, bool has_offs,
                     uint32_t tok_offs_start, uint32_t tok_offs_end) {
  uint32_t start = std::numeric_limits<uint32_t>::max();
  uint32_t end = std::numeric_limits<uint32_t>::max();
  if (has_offs) {
    start = offs_start + tok_offs_start;
    end = offs_start + tok_offs_end;
  }

  _positions.emplace(pos, start, end, irs::bytes_view{});
}

Posting& Term::insert(irs::doc_id_t id) {
  return const_cast<Posting&>(*postings.emplace(id).first);
}

Term::Term(irs::bytes_view data) : value(data) {}

bool Term::operator<(const Term& rhs) const {
  return MemcmpLess(value.c_str(), value.size(), rhs.value.c_str(),
                    rhs.value.size());
}

void Term::sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs) {
  std::set<Posting> resorted_postings;

  for (auto& posting : postings) {
    resorted_postings.emplace(
      docs.at(posting._id),
      std::move(const_cast<tests::Posting&>(posting)._positions));
  }

  postings = std::move(resorted_postings);
}

Field::Field(irs::field_id id, irs::IndexFeatures index_features)
  : FieldMeta(id, index_features), stats{} {}

Term& Field::insert(irs::bytes_view t) {
  auto res = terms.emplace(t);
  return const_cast<Term&>(*res.first);
}

Term* Field::find(irs::bytes_view t) {
  auto it = terms.find(Term{t});
  return terms.end() == it ? nullptr : const_cast<Term*>(&*it);
}

size_t Field::remove(irs::bytes_view t) { return terms.erase(Term{t}); }

irs::bytes_view Field::min() const {
  EXPECT_FALSE(terms.empty());
  return std::begin(terms)->value;
}

irs::bytes_view Field::max() const {
  EXPECT_FALSE(terms.empty());
  return std::rbegin(terms)->value;
}

uint64_t Field::total_freq() const {
  uint64_t value = 0;
  for (auto& term : terms) {
    for (auto& post : term.postings) {
      const auto sum = value + post.positions().size();
      EXPECT_GE(sum, value);
      EXPECT_GE(sum, post.positions().size());
      value += post.positions().size();
    }
  }

  return value;
}

uint64_t Field::total_doc_freq() const {
  uint64_t value = 0;
  for (auto& term : terms) {
    value += term.postings.size();
  }
  return value;
}

void Field::sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs) {
  for (auto& term : terms) {
    const_cast<tests::Term&>(term).sort(docs);
  }
}

void ColumnValues::insert(irs::doc_id_t key, irs::bytes_view value) {
  ASSERT_TRUE(irs::doc_limits::valid(key));
  ASSERT_TRUE(!irs::doc_limits::eof(key));

  const auto res = _values.emplace(key, value);

  if (!res.second) {
    res.first->second.append(value.data(), value.size());
  }
}

void ColumnValues::sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs) {
  std::map<irs::doc_id_t, irs::bstring> resorted_values;

  for (auto& value : _values) {
    resorted_values.emplace(docs.at(value.first), std::move(value.second));
  }

  _values = std::move(resorted_values);
}

void IndexSegment::compute_features() {}

void IndexSegment::insert_sorted(const Ifield* f) {
  if (f) {
    _buf.clear();
    irs::BytesOutput out{_buf};
    if (f->Write(out)) {
      _sort.emplace_back(std::move(_buf), doc(), _empty_count);
      _empty_count = 0;
    } else {
      ++_empty_count;
    }
  } else {
    ++_empty_count;
  }
}

void IndexSegment::insert_stored(const Ifield& f) {
  _buf.clear();
  irs::BytesOutput out{_buf};
  if (!f.Write(out)) {
    return;
  }

  const size_t id = _columns.size();
  EXPECT_LE(id, std::numeric_limits<irs::field_id>::max());

  auto res = _named_columns.emplace(f.Name(), nullptr);

  if (res.second) {
    res.first->second = &_columns.emplace_back(std::string{f.Name()}, id);
  }

  auto* column = res.first->second;
  ASSERT_NE(nullptr, column);
  EXPECT_LT(column->id(), _columns.size());
  column->insert(doc(), _buf);
}

void IndexSegment::insert_indexed(const Ifield& f) {
  EXPECT_TRUE(irs::field_limits::valid(f.Id()))
    << "insert_indexed: field must have a catalog-allocated id, not "
       "field_limits::invalid()";
  const auto requested_features = f.GetIndexFeatures();
  const auto features = requested_features & (~irs::IndexFeatures::Offs);

  const auto res = _fields.emplace(f.Id(), Field{f.Id(), features});

  Field& field = res.first->second;

  if (res.second) {
    _id_to_field.emplace_back(&field);

    if (irs::IsSubsetOf(irs::IndexFeatures::Norm, requested_features)) {
      const size_t id = _columns.size();
      EXPECT_LE(id, std::numeric_limits<irs::field_id>::max());
      _columns.emplace_back(irs::field_id{id});
      field.feature_infos.emplace_back(Field::FeatureInfo{irs::field_id{id}});
      field.norm = irs::field_id{id};
    }
  }

  _doc_fields.insert(&field);

  if (const auto block_terms = f.BlockTerms(); block_terms.has_value()) {
    const auto doc_id = doc();
    uint32_t inc_value = 1;
    for (const auto& block_term : *block_terms) {
      tests::Term& trm = field.insert(block_term);
      if (trm.postings.empty() ||
          std::prev(std::end(trm.postings))->id() != doc_id) {
        ++field.stats.num_unique;
      }
      tests::Posting& pst = trm.insert(doc_id);
      field.stats.pos += inc_value;
      field.stats.num_overlap += static_cast<uint32_t>(0 == inc_value);
      ++field.stats.len;
      pst.insert(field.stats.pos);
      field.stats.max_term_freq =
        std::max(field.stats.max_term_freq,
                 static_cast<decltype(field.stats.max_term_freq)>(
                   pst.positions().size()));
      inc_value = 0;
    }
    if (!block_terms->empty()) {
      field.docs.emplace(doc_id);
    }
    return;
  }

  auto& analyzer = f.GetTokens();

  const bool has_offs = analyzer.Traits().offsets;
  if (irs::IndexFeatures::Offs ==
        (requested_features & irs::IndexFeatures::Offs) &&
      has_offs) {
    field.index_features |= irs::IndexFeatures::Offs;
  }

  const auto doc_id = doc();

  irs::ValueAnalyzer value_analyzer;
  irs::ValueTokens<irs::TokenLayout::TermsPosOffs> tokens{analyzer.Traits()};
  const auto fv = f.Value();
  value_analyzer.Analyze(
    analyzer, duckdb::string_t{fv.data(), static_cast<uint32_t>(fv.size())},
    tokens);

  const auto terms = tokens.terms();
  const auto positions = tokens.pos();
  const auto offs_start = tokens.offs_start();
  const auto offs_end = tokens.offs_end();
  uint32_t prev_pos = 0;
  uint32_t last_offs_end = 0;
  for (size_t i = 0; i < terms.size(); ++i) {
    tests::Term& trm = field.insert(irs::AsBytesView(terms[i]));

    if (trm.postings.empty() ||
        std::prev(std::end(trm.postings))->id() != doc_id) {
      ++field.stats.num_unique;
    }

    tests::Posting& pst = trm.insert(doc_id);
    const uint32_t pos = positions[i];
    const uint32_t start = offs_start.empty() ? 0 : offs_start[i];
    const uint32_t end = offs_end.empty() ? 0 : offs_end[i];
    const uint32_t inc = pos - prev_pos;
    prev_pos = pos;
    field.stats.pos += inc;
    field.stats.num_overlap += static_cast<uint32_t>(0 == inc);
    ++field.stats.len;
    pst.insert(field.stats.pos, field.stats.offs, has_offs, start, end);
    field.stats.max_term_freq = std::max(
      field.stats.max_term_freq,
      static_cast<decltype(field.stats.max_term_freq)>(pst.positions().size()));

    last_offs_end = end;
  }

  if (!terms.empty()) {
    field.docs.emplace(doc_id);
  }

  if (has_offs) {
    field.stats.offs += last_offs_end;
  }
}

void IndexSegment::sort(const irs::Comparer& comparator) {
  if (_sort.empty()) {
    return;
  }

  std::stable_sort(
    _sort.begin(), _sort.end(), [&](const auto& lhs, const auto& rhs) {
      return comparator.Compare(std::get<0>(lhs), std::get<0>(rhs)) < 0;
    });

  irs::doc_id_t new_doc_id = irs::doc_limits::min();
  std::map<irs::doc_id_t, irs::doc_id_t> order;

  for (auto& [_, doc, prev] : _sort) {
    for (auto i = prev; i; --i) {
      order[doc - i] = new_doc_id++;
    }
    order[doc] = new_doc_id++;
  }
  while (order.size() < this->doc_count()) {
    order[static_cast<irs::doc_id_t>(order.size()) + 1] = new_doc_id++;
    ASSERT_LE(order.size(), this->doc_count());
  }
  for (auto& field : _fields) {
    field.second.sort(order);
  }
  for (auto& column : _columns) {
    column.sort(order);
  }
  for (auto& [_, doc, __] : _sort) {
    doc = order.at(doc);
  }
}

class PostingsImpl : public irs::TermPostings {
 public:
  PostingsImpl(irs::IndexFeatures features, const tests::Term& data);

  irs::doc_id_t Advance() final {
    if (_next == _data.postings.end()) {
      return _doc = irs::doc_limits::eof();
    }

    _prev = _next, ++_next;
    _doc = _prev->id();
    _freq = static_cast<uint32_t>(_prev->positions().size());
    _pos.Clear();

    return _doc;
  }

  uint32_t GetFreq() const final { return _freq; }

  irs::PosAttr* Positions() noexcept final { return _positions; }

 private:
  class PosIterator final : public irs::PosAttr {
   public:
    PosIterator(const PostingsImpl& owner, irs::IndexFeatures features)
      : _owner(owner) {
      if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Offs)) {
        _poffs = &_offs;
      }
    }

    Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      if (irs::Type<irs::OffsAttr>::id() == type) {
        return _poffs;
      }

      return nullptr;
    }

    void Clear() {
      _next = _owner._prev->positions().begin();
      _value = irs::pos_limits::invalid();
      _offs.clear();
    }

    bool next() final {
      if (_next == _owner._prev->positions().end()) {
        _value = irs::pos_limits::eof();
        return false;
      }

      _value = _next->pos;
      _offs.start = _next->start;
      _offs.end = _next->end;
      ++_next;

      return true;
    }

    void reset() final { ASSERT_TRUE(false); }

   private:
    std::set<Posting::Position>::const_iterator _next;
    irs::OffsAttr _offs;
    irs::OffsAttr* _poffs{};
    const PostingsImpl& _owner;
  };

  const tests::Term& _data;
  uint32_t _freq = 0;
  PosIterator _pos;
  irs::PosAttr* _positions{};
  std::set<Posting>::const_iterator _prev;
  std::set<Posting>::const_iterator _next;
};

PostingsImpl::PostingsImpl(irs::IndexFeatures features, const tests::Term& data)
  : _data(data), _pos(*this, features) {
  _next = _data.postings.begin();

  if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Pos)) {
    _positions = &_pos;
  }
}

class TermIterator : public irs::SeekTermIterator {
 public:
  explicit TermIterator(const tests::Field& data) noexcept : _data(data) {
    _next = _data.terms.begin();
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    return type == irs::Type<irs::TermAttr>::id() ? &_value : nullptr;
  }

  irs::bytes_view value() const noexcept final { return _value.value; }

  bool next() final {
    if (_next == _data.terms.end()) {
      Unposition();
      return false;
    }

    _prev = _next, ++_next;
    Position();
    return true;
  }

  bool seek(irs::bytes_view value) final {
    auto it = _data.terms.find(Term{value});

    if (it == _data.terms.end()) {
      _prev = _next = it;
      Unposition();
      return false;
    }

    _prev = it;
    _next = ++it;
    Position();
    return true;
  }

  irs::SeekResult seek_ge(irs::bytes_view value) final {
    auto it = _data.terms.lower_bound(Term{value});
    if (it == _data.terms.end()) {
      _prev = _next = it;
      Unposition();
      return irs::SeekResult::End;
    }

    _prev = it;
    _next = ++it;
    Position();
    return this->value() == value ? irs::SeekResult::Found
                                  : irs::SeekResult::NotFound;
  }

  irs::TermPostings::ptr postings(irs::IndexFeatures features) const final {
    return irs::memory::make_managed<PostingsImpl>(
      _data.index_features & features, *_prev);
  }

  const irs::PostingMeta& cookie() const final { return _meta; }

 private:
  void Position() {
    _value.value = _prev->value;
    _meta.docs_count = _prev->docs_count();
  }

  void Unposition() {
    _value.value = {};
    _meta.clear();
  }

  const tests::Field& _data;
  std::set<tests::Term>::const_iterator _prev;
  std::set<tests::Term>::const_iterator _next;
  irs::TermAttr _value;
  irs::PostingMeta _meta;
};

irs::SeekTermIterator::ptr Field::iterator() const {
  return irs::memory::make_managed<TermIterator>(*this);
}

template<typename PostingsFactory>
void AssertDocs(irs::IndexFeatures features,
                irs::TermPostings::ptr expected_docs,
                PostingsFactory&& factory) {
  ASSERT_NE(nullptr, expected_docs);

  auto actual_docs = factory();
  ASSERT_NE(nullptr, actual_docs);

  ASSERT_TRUE(!irs::doc_limits::valid(expected_docs->Value()));
  ASSERT_TRUE(!irs::doc_limits::valid(actual_docs->Value()));

  const bool has_freq =
    irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq);

  size_t doc_index = 0;
  while (!irs::doc_limits::eof(expected_docs->Advance())) {
    SCOPED_TRACE(absl::StrCat("doc_index=", doc_index++));
    const auto expected_doc = expected_docs->Value();

    ASSERT_TRUE(!irs::doc_limits::eof(actual_docs->Advance()));
    ASSERT_EQ(expected_doc, actual_docs->Value());

    if (!has_freq) {
      continue;
    }
    ASSERT_EQ(expected_docs->GetFreq(), actual_docs->GetFreq());

    auto* expected_pos = expected_docs->Positions();
    auto* actual_pos = actual_docs->Positions();
    ASSERT_EQ(!expected_pos, !actual_pos);

    if (!expected_pos) {
      continue;
    }

    auto* expected_offs = irs::get<irs::OffsAttr>(*expected_pos);
    auto* actual_offs = irs::get<irs::OffsAttr>(*actual_pos);
    ASSERT_EQ(!expected_offs, !actual_offs);

    auto* expected_pay = irs::get<irs::PayAttr>(*expected_pos);
    auto* actual_pay = irs::get<irs::PayAttr>(*actual_pos);
    ASSERT_EQ(!expected_pay, !actual_pay);

    ASSERT_TRUE(!irs::pos_limits::valid(expected_pos->value()));
    ASSERT_TRUE(!irs::pos_limits::valid(actual_pos->value()));
    size_t pos_index = 0;
    for (; expected_pos->next();) {
      SCOPED_TRACE(absl::StrCat("pos_index=", pos_index++));
      ASSERT_TRUE(actual_pos->next());
      ASSERT_EQ(expected_pos->value(), actual_pos->value());

      if (expected_offs) {
        ASSERT_EQ(expected_offs->start, actual_offs->start);
        ASSERT_EQ(expected_offs->end, actual_offs->end);
      }

      if (expected_pay) {
        ASSERT_EQ(expected_pay->value, actual_pay->value);
      }
    }
    ASSERT_FALSE(actual_pos->next());
    ASSERT_TRUE(irs::pos_limits::eof(expected_pos->value()));
    ASSERT_TRUE(irs::pos_limits::eof(actual_pos->value()));
  }

  ASSERT_TRUE(irs::doc_limits::eof(expected_docs->Value()));
  ASSERT_FALSE(!irs::doc_limits::eof(actual_docs->Advance()));
  ASSERT_TRUE(irs::doc_limits::eof(actual_docs->Value()));
}

irs::lead::Node::ptr MakeLeadDocs(const irs::SubReader& segment,
                                  const irs::TermReader& actual_terms,
                                  const irs::PostingMeta& actual_cookie) {
  const irs::search::PostingClause posting{
    .state = irs::TermState{&actual_terms, actual_cookie}};
  if (irs::search::DocOf(actual_terms) == nullptr) {
    return {};
  }
  return irs::lead::MakePostingDocs(posting, segment);
}

// A `lead::Node` answers documents and nothing else, so the frequencies and
// positions the postings above carry are not asked of it -- what a seeking
// plan owes is that it lands on the same documents a full walk does.
void AssertSeek(const irs::SubReader& segment,
                const irs::TermIterator& expected_term,
                const irs::TermReader& actual_terms,
                const irs::PostingMeta& actual_cookie,
                irs::IndexFeatures requested_features) {
  auto expected_docs = expected_term.postings(requested_features);
  ASSERT_NE(nullptr, expected_docs);

  auto seq_docs = MakeLeadDocs(segment, actual_terms, actual_cookie);
  ASSERT_NE(nullptr, seq_docs);

  auto seek_docs = MakeLeadDocs(segment, actual_terms, actual_cookie);
  ASSERT_NE(nullptr, seek_docs);

  ASSERT_TRUE(!irs::doc_limits::valid(expected_docs->Value()));
  ASSERT_TRUE(!irs::doc_limits::valid(seq_docs->Value()));
  ASSERT_TRUE(!irs::doc_limits::valid(seek_docs->Value()));

  size_t doc_index = 0;
  while (!irs::doc_limits::eof(expected_docs->Advance())) {
    SCOPED_TRACE(absl::StrCat("doc_index=", doc_index++));
    const auto expected_doc = expected_docs->Value();

    ASSERT_TRUE(!irs::doc_limits::eof(seq_docs->Advance()));
    ASSERT_EQ(expected_doc, seq_docs->Value());

    ASSERT_EQ(expected_doc, seek_docs->Seek(expected_doc));
    ASSERT_EQ(expected_doc, seek_docs->Value());
  }

  ASSERT_TRUE(irs::doc_limits::eof(expected_docs->Value()));
  ASSERT_TRUE(irs::doc_limits::eof(seq_docs->Advance()));
  ASSERT_TRUE(irs::doc_limits::eof(seek_docs->Advance()));

  // FIXME(gnusi): check BitUnion
}

void AssertTerm(irs::TermIterator& expected_term,
                irs::TermIterator& actual_term,
                irs::IndexFeatures requested_features) {
  ASSERT_EQ(expected_term.value(), actual_term.value());

  ASSERT_EQ(expected_term.cookie().docs_count, actual_term.cookie().docs_count);

  AssertDocs(requested_features, expected_term.postings(requested_features),
             [&] { return actual_term.postings(requested_features); });
}

irs::SeekTermIterator::ptr ExpectedTerms(
  const Field& expected_field, irs::automaton_table_matcher* matcher) {
  auto terms = expected_field.iterator();
  if (!matcher) {
    return terms;
  }
  return irs::memory::make_managed<irs::AutomatonTermIterator>(
    matcher->GetFst(), std::move(terms));
}

irs::SeekTermIterator::ptr ActualTerms(const irs::TermReader& actual_field,
                                       irs::automaton_table_matcher* matcher) {
  return matcher ? actual_field.iterator(*matcher) : actual_field.iterator();
}

void AssertTermsNext(const irs::SubReader& segment, const Field& expected_field,
                     const irs::TermReader& actual_field,
                     irs::IndexFeatures features,
                     irs::automaton_table_matcher* matcher) {
  irs::bytes_view actual_min{};
  irs::bytes_view actual_max{};
  irs::bstring actual_min_buf;
  irs::bstring actual_max_buf;
  size_t actual_size = 0;

  auto expected_term = ExpectedTerms(expected_field, matcher);

  auto actual_term = ActualTerms(actual_field, matcher);

  size_t term_index = 0;
  for (; expected_term->next(); ++actual_size) {
    SCOPED_TRACE(absl::StrCat("term_index=", term_index++));
    ASSERT_TRUE(actual_term->next());

    AssertTerm(*expected_term, *actual_term, features);
    AssertSeek(segment, *expected_term, actual_field, actual_term->cookie(),
               features);

    if (irs::IsNull(actual_min)) {
      actual_min_buf = actual_term->value();
      actual_min = actual_min_buf;
    }

    actual_max_buf = actual_term->value();
    actual_max = actual_max_buf;
  }

  if (!matcher) {
    ASSERT_EQ(expected_field.terms.size(), actual_size);
    ASSERT_EQ((expected_field.min)(), actual_min);
    ASSERT_EQ((expected_field.max)(), actual_max);
  }
}

void AssertTermsSeek(const Field& expected_field,
                     const irs::TermReader& actual_field,
                     irs::IndexFeatures features,
                     irs::automaton_table_matcher* matcher,
                     size_t lookahead = 10) {
  auto expected_term = ExpectedTerms(expected_field, matcher);

  auto actual_term_with_state = ActualTerms(actual_field, matcher);
  ASSERT_NE(nullptr, actual_term_with_state);

  auto actual_term_with_state_random_only = actual_field.iterator();
  ASSERT_NE(nullptr, actual_term_with_state_random_only);

  size_t term_index = 0;
  for (; expected_term->next();) {
    SCOPED_TRACE(absl::StrCat("term_index=", term_index));
    {
      ASSERT_TRUE(actual_term_with_state->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term_with_state, features);
    }

    {
      auto actual_term = actual_field.iterator();
      ASSERT_TRUE(actual_term->seek(expected_term->value()));

      AssertTerm(*expected_term, *actual_term, features);
    }

    {
      ASSERT_TRUE(
        actual_term_with_state_random_only->seek(expected_term->value()));

      AssertTerm(*expected_term, *actual_term_with_state_random_only, features);
    }

    irs::PostingMeta cookie;
    {
      auto actual_term = actual_field.iterator();
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);
      cookie = actual_term->cookie();

      {
        auto copy_expected_term =
          irs::memory::make_managed<TermIterator>(expected_field);

        ASSERT_TRUE(copy_expected_term->seek(expected_term->value()));
        ASSERT_EQ(expected_term->value(), copy_expected_term->value());
        for (size_t i = 0; i < lookahead; ++i) {
          const bool copy_expected_next = copy_expected_term->next();
          const bool actual_next = actual_term->next();
          ASSERT_EQ(copy_expected_next, actual_next);
          if (!copy_expected_next) {
            break;
          }
          AssertTerm(*copy_expected_term, *actual_term, features);
        }
      }

      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);
    }

    {
      auto actual_term = actual_field.iterator();
      ASSERT_EQ(irs::SeekResult::Found,
                actual_term->seek_ge(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);

      {
        auto copy_expected_term =
          irs::memory::make_managed<TermIterator>(expected_field);
        ASSERT_TRUE(copy_expected_term->seek(expected_term->value()));
        ASSERT_EQ(expected_term->value(), copy_expected_term->value());
        for (size_t i = 0; i < lookahead; ++i) {
          const bool copy_expected_next = copy_expected_term->next();
          const bool actual_next = actual_term->next();
          ASSERT_EQ(copy_expected_next, actual_next);
          if (!copy_expected_next) {
            break;
          }
          AssertTerm(*copy_expected_term, *actual_term, features);
        }
      }

      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);
    }

    {
      auto actual_term = actual_field.iterator();

      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);

      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);

      ASSERT_EQ(irs::SeekResult::Found,
                actual_term->seek_ge(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);
    }
  }
}

void AssertIndex(irs::IndexReader::ptr actual_index,
                 const index_t& expected_index, irs::IndexFeatures features,
                 size_t skip, irs::automaton_table_matcher* matcher) {
  ASSERT_EQ(expected_index.size(), actual_index->size());
  size_t i = 0;
  size_t segment_index = 0;
  for (auto& actual_segment : *actual_index) {
    SCOPED_TRACE(absl::StrCat("segment_index=", segment_index++));
    if (skip) {
      ++i;
      --skip;
      continue;
    }

    const tests::IndexSegment& expected_segment = expected_index[i];

    ASSERT_EQ(1, actual_segment.size());
    ASSERT_EQ(&actual_segment, &*actual_segment.begin());

    auto& expected_fields = expected_segment.fields();
    auto expected_field = expected_fields.begin();

    auto actual_field_ids = actual_segment.field_ids();
    size_t field_index = 0;
    auto actual_id_it = actual_field_ids.begin();
    for (; actual_id_it != actual_field_ids.end();
         ++actual_id_it, ++expected_field) {
      SCOPED_TRACE(absl::StrCat("field_index=", field_index++));
      ASSERT_NE(expected_fields.end(), expected_field);
      ASSERT_EQ(expected_field->second.id, *actual_id_it);

      const auto* actual_terms = actual_segment.field(*actual_id_it);
      ASSERT_NE(nullptr, actual_terms);
      ASSERT_EQ(expected_field->second.id, actual_terms->meta().id);
      ASSERT_EQ(expected_field->second.index_features,
                actual_terms->meta().index_features);

      ASSERT_EQ((expected_field->second.min)(), (actual_terms->min)());
      ASSERT_EQ((expected_field->second.max)(), (actual_terms->max)());
      ASSERT_EQ(expected_field->second.terms.size(), actual_terms->size());
      ASSERT_EQ(expected_field->second.docs.size(), actual_terms->docs_count());

      const irs::FieldMeta& expected_meta = expected_field->second;
      const irs::FieldMeta& actual_meta = actual_terms->meta();
      ASSERT_EQ(expected_meta.id, actual_meta.id);
      ASSERT_EQ(expected_meta.index_features, actual_meta.index_features);
      ASSERT_EQ(irs::field_limits::valid(expected_meta.norm),
                irs::field_limits::valid(actual_meta.norm));
      ASSERT_EQ(
        irs::IsSubsetOf(irs::IndexFeatures::Norm, expected_meta.index_features),
        irs::field_limits::valid(expected_meta.norm));
      ASSERT_EQ(
        irs::IsSubsetOf(irs::IndexFeatures::Norm, actual_meta.index_features),
        irs::field_limits::valid(actual_meta.norm));

      auto* actual_freq = irs::get<irs::FreqAttr>(*actual_terms);
      ASSERT_NE(nullptr, actual_freq);
      if (irs::IndexFeatures::None !=
          (expected_field->second.index_features & irs::IndexFeatures::Freq)) {
        ASSERT_EQ(expected_field->second.total_freq(), actual_freq->value);
      } else {
        ASSERT_EQ(expected_field->second.total_doc_freq(), actual_freq->value);
      }

      const auto field_features =
        expected_field->second.index_features & features;
      AssertTermsNext(actual_segment, expected_field->second, *actual_terms,
                      field_features, matcher);
      AssertTermsSeek(expected_field->second, *actual_terms, field_features,
                      matcher);
    }
    ASSERT_EQ(actual_field_ids.end(), actual_id_it);

    ++i;
    ASSERT_EQ(expected_fields.end(), expected_field);
  }
}

void AssertIndex(const irs::Directory& dir, irs::Format::ptr codec,
                 const index_t& expected_index, irs::IndexFeatures features,
                 size_t skip, irs::automaton_table_matcher* matcher) {
  auto reader =
    irs::DirectoryReader(dir, codec, ::irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, reader);

  AssertIndex(reader.GetImpl(), expected_index, features, skip, matcher);
}

}  // namespace tests
