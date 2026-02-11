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

#include <algorithm>
#include <basics/bit_utils.hpp>
#include <iostream>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/index/comparer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/cost.hpp>
#include <iresearch/search/score.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/tfidf.hpp>
#include <iresearch/store/data_output.hpp>
#include <iresearch/utils/automaton_utils.hpp>
#include <iresearch/utils/bytes_output.hpp>
#include <iresearch/utils/fstext/fst_table_matcher.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <unordered_set>

#include "basics/down_cast.h"
#include "tests_shared.hpp"

namespace {

bool Visit(const irs::ColumnReader& reader,
           const std::function<bool(irs::doc_id_t, irs::bytes_view)>& visitor) {
  auto it = reader.iterator(irs::ColumnHint::Consolidation);

  irs::PayAttr dummy;
  auto* doc = irs::get<irs::DocAttr>(*it);
  if (!doc) {
    return false;
  }
  auto* payload = irs::get<irs::PayAttr>(*it);
  if (!payload) {
    payload = &dummy;
  }

  while (it->next()) {
    if (!visitor(doc->value, payload->value)) {
      return false;
    }
  }

  return true;
}

}  // namespace

namespace tests {

void AssertTerm(const irs::TermIterator& expected_term,
                const irs::TermIterator& actual_term,
                irs::IndexFeatures requested_features);

void Posting::insert(uint32_t pos, uint32_t offs_start,
                     const irs::AttributeProvider& attrs) {
  auto* offs = irs::get<irs::OffsAttr>(attrs);
  auto* pay = irs::get<irs::PayAttr>(attrs);

  uint32_t start = std::numeric_limits<uint32_t>::max();
  uint32_t end = std::numeric_limits<uint32_t>::max();
  if (offs) {
    start = offs_start + offs->start;
    end = offs_start + offs->end;
  }

  _positions.emplace(pos, start, end, pay ? pay->value : irs::bytes_view{});
}

Posting& Term::insert(irs::doc_id_t id) {
  return const_cast<Posting&>(*postings.emplace(id).first);
}

Term::Term(irs::bytes_view data) : value(data) {}

bool Term::operator<(const Term& rhs) const {
  return irs::MemcmpLess(value.c_str(), value.size(), rhs.value.c_str(),
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

Field::Field(const std::string_view& name, irs::IndexFeatures index_features)
  : FieldMeta(name, index_features), stats{} {}

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
  using FreqT = decltype(irs::FreqAttr{}.value);
  static_assert(std::is_unsigned_v<FreqT>);

  FreqT value{0};
  for (auto& term : terms) {
    for (auto& post : term.postings) {
      const auto sum = value + post.positions().size();
      EXPECT_GE(sum, value);
      EXPECT_GE(sum, post.positions().size());
      value += static_cast<FreqT>(post.positions().size());
    }
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

irs::bstring ColumnValues::payload() const {
  if (!_payload.has_value() && _writer) {
    _payload.emplace();
    irs::BytesOutput writer{_payload.value()};
    _writer->finish(writer);
    _payload = _payload->substr(sizeof(uint32_t));  // skip size
  }

  return _payload.value_or(irs::bstring{});
}

void ColumnValues::sort(const std::map<irs::doc_id_t, irs::doc_id_t>& docs) {
  std::map<irs::doc_id_t, irs::bstring> resorted_values;

  for (auto& value : _values) {
    resorted_values.emplace(docs.at(value.first), std::move(value.second));
  }

  _values = std::move(resorted_values);
}

void ColumnValues::rewrite() {
  if (_writer && _factory) {
    irs::bstring hdr = payload();
    const irs::bytes_view ref = hdr;
    auto writer = _factory({&ref, 1});
    ASSERT_NE(nullptr, writer);

    std::map<irs::doc_id_t, irs::bstring> values;
    for (auto& value : _values) {
      irs::BytesOutput out{values[value.first]};
      writer->write(out, value.second);
    }

    ASSERT_TRUE(_payload.has_value());
    _payload.value().clear();
    irs::BytesOutput payload_writer{_payload.value()};
    writer->finish(payload_writer);
    _payload = _payload.value().substr(sizeof(uint32_t));  // skip size
    _values = std::move(values);
  }
}

void IndexSegment::compute_features() {
  struct ColumnOutput final : public irs::ColumnOutput {
    explicit ColumnOutput(irs::bstring& buf) noexcept : buf{&buf} {}

    void Prepare(irs::doc_id_t) final { written = true; }

    void WriteByte(irs::byte_type b) final { (*buf) += b; }

    void WriteBytes(const irs::byte_type* b, size_t size) final {
      buf->append(b, size);
    }

    void Reset() final { buf->clear(); }

    irs::bstring* buf;
    bool written{false};
  } out{_buf};

  for (auto* field : _doc_fields) {
    for (auto& entry : field->feature_infos) {
      if (entry.writer) {
        _buf.clear();

        const auto doc_id = doc();
        out.written = false;
        entry.writer->write(field->stats, doc_id, out);

        if (out.written) {
          _columns[entry.id].insert(doc_id, _buf);
        }
      }
    }
  }
}

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
  const std::string_view field_name = f.Name();

  const auto requested_features = f.GetIndexFeatures();
  const auto features = requested_features &
                        (~(irs::IndexFeatures::Offs | irs::IndexFeatures::Pay));

  const auto res = _fields.emplace(field_name, Field{field_name, features});

  Field& field = res.first->second;

  if (res.second) {
    auto& new_field = res.first->second;
    _id_to_field.emplace_back(&new_field);

    auto write_feature = [&](irs::IndexFeatures feature) -> irs::field_id {
      if (feature != irs::IndexFeatures::None) {
        auto handler = _field_features(feature);

        auto feature_writer = handler.second ? (*handler.second)({}) : nullptr;

        if (feature_writer) {
          const size_t id = _columns.size();
          EXPECT_LE(id, std::numeric_limits<irs::field_id>::max());
          _columns.emplace_back(id, handler.second, feature_writer.get());

          new_field.feature_infos.emplace_back(Field::FeatureInfo{
            irs::field_id{id}, handler.second, std::move(feature_writer)});

          return irs::field_id{id};
        }
      }

      return irs::field_limits::invalid();
    };

    new_field.norm =
      write_feature(requested_features & irs::IndexFeatures::Norm);
    const_cast<std::string_view&>(res.first->first) = field.name;
  }

  _doc_fields.insert(&field);

  auto& stream = f.GetTokens();

  auto* term = irs::get<irs::TermAttr>(stream);
  SDB_ASSERT(term);
  auto* inc = irs::get<irs::IncAttr>(stream);
  SDB_ASSERT(inc);
  auto* offs = irs::get<irs::OffsAttr>(stream);
  if (irs::IndexFeatures::Offs ==
        (requested_features & irs::IndexFeatures::Offs) &&
      offs) {
    field.index_features |= irs::IndexFeatures::Offs;
  }
  auto* pay = irs::get<irs::PayAttr>(stream);
  if (irs::IndexFeatures::Pay ==
        (requested_features & irs::IndexFeatures::Pay) &&
      pay) {
    field.index_features |= irs::IndexFeatures::Pay;
  }

  bool empty = true;
  const auto doc_id = doc();

  while (stream.next()) {
    tests::Term& trm = field.insert(term->value);

    if (trm.postings.empty() ||
        std::prev(std::end(trm.postings))->id() != doc_id) {
      ++field.stats.num_unique;
    }

    tests::Posting& pst = trm.insert(doc_id);
    field.stats.pos += inc->value;
    field.stats.num_overlap += static_cast<uint32_t>(0 == inc->value);
    ++field.stats.len;
    pst.insert(field.stats.pos, field.stats.offs, stream);
    field.stats.max_term_freq = std::max(
      field.stats.max_term_freq,
      static_cast<decltype(field.stats.max_term_freq)>(pst.positions().size()));

    empty = false;
  }

  if (!empty) {
    field.docs.emplace(doc_id);
  }

  if (offs) {
    field.stats.offs += offs->end;
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

class DocIteratorImpl : public irs::DocIterator {
 public:
  DocIteratorImpl(irs::IndexFeatures features, const tests::Term& data);

  irs::doc_id_t value() const final { return _doc.value; }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    const auto it = _attrs.find(type);
    return it == _attrs.end() ? nullptr : it->second;
  }

  irs::doc_id_t advance() final {
    if (_next == _data.postings.end()) {
      return _doc.value = irs::doc_limits::eof();
    }

    _prev = _next, ++_next;
    _doc.value = _prev->id();
    _freq.value = static_cast<uint32_t>(_prev->positions().size());
    _pos.Clear();

    return _doc.value;
  }

  irs::doc_id_t seek(irs::doc_id_t id) final {
    auto it = _data.postings.find(Posting{id});

    if (it == _data.postings.end()) {
      _prev = _next = it;
      return irs::doc_limits::eof();
    }

    _prev = it;
    _next = ++it;
    _doc.value = _prev->id();
    _pos.Clear();

    return _doc.value;
  }

 private:
  class PosIterator final : public irs::PosAttr {
   public:
    PosIterator(const DocIteratorImpl& owner, irs::IndexFeatures features)
      : _owner(owner) {
      if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Offs)) {
        _poffs = &_offs;
      }

      if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Pay)) {
        _ppay = &_pay;
      }
    }

    Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      if (irs::Type<irs::OffsAttr>::id() == type) {
        return _poffs;
      }

      if (irs::Type<irs::PayAttr>::id() == type) {
        return _ppay;
      }

      return nullptr;
    }

    void Clear() {
      _next = _owner._prev->positions().begin();
      _value = irs::pos_limits::invalid();
      _offs.clear();
      _pay.value = irs::bytes_view{};
    }

    bool next() final {
      if (_next == _owner._prev->positions().end()) {
        _value = irs::pos_limits::eof();
        return false;
      }

      _value = _next->pos;
      _offs.start = _next->start;
      _offs.end = _next->end;
      _pay.value = _next->payload;
      ++_next;

      return true;
    }

    void reset() final {
      ASSERT_TRUE(false);  // unsupported
    }

   private:
    std::set<Posting::Position>::const_iterator _next;
    irs::OffsAttr _offs;
    irs::PayAttr _pay;
    irs::OffsAttr* _poffs{};
    irs::PayAttr* _ppay{};
    const DocIteratorImpl& _owner;
  };

  const tests::Term& _data;
  std::map<irs::TypeInfo::type_id, irs::Attribute*> _attrs;
  irs::DocAttr _doc;
  irs::FreqAttr _freq;
  irs::CostAttr _cost;
  irs::ScoreAttr _score;
  PosIterator _pos;
  std::set<Posting>::const_iterator _prev;
  std::set<Posting>::const_iterator _next;
};

DocIteratorImpl::DocIteratorImpl(irs::IndexFeatures features,
                                 const tests::Term& data)
  : _data(data), _pos(*this, features) {
  _next = _data.postings.begin();

  _cost.reset(_data.postings.size());
  _attrs[irs::Type<irs::CostAttr>::id()] = &_cost;

  _attrs[irs::Type<irs::DocAttr>::id()] = &_doc;
  _attrs[irs::Type<irs::ScoreAttr>::id()] = &_score;

  if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq)) {
    _attrs[irs::Type<irs::FreqAttr>::id()] = &_freq;
  }

  if (irs::IndexFeatures::None != (features & irs::IndexFeatures::Pos)) {
    _attrs[irs::Type<irs::PosAttr>::id()] = &_pos;
  }
}

class TermIterator : public irs::SeekTermIterator {
 public:
  struct TermCookie final : irs::SeekCookie {
    explicit TermCookie(irs::bytes_view term) noexcept : term(term) {}

    irs::Attribute* GetMutable(irs::TypeInfo::type_id) final { return nullptr; }

    bool IsEqual(const irs::SeekCookie& rhs) const noexcept final {
      return term == sdb::basics::downCast<TermCookie>(rhs).term;
    }

    size_t Hash() const noexcept final { return absl::HashOf(term); }

    irs::bytes_view term;
  };

  explicit TermIterator(const tests::Field& data) noexcept : _data(data) {
    _next = _data.terms.begin();
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<irs::TermAttr>::id()) {
      return &_value;
    }
    if (type == irs::Type<irs::TermMeta>::id()) {
      return &_meta;
    }
    return nullptr;
  }

  irs::bytes_view value() const final { return _value.value; }

  bool next() final {
    if (_next == _data.terms.end()) {
      _value.value = {};
      return false;
    }

    _prev = _next, ++_next;
    _value.value = _prev->value;
    return true;
  }

  void read() noexcept final { _meta.docs_count = _prev->docs_count(); }

  bool seek(irs::bytes_view value) final {
    auto it = _data.terms.find(Term{value});

    if (it == _data.terms.end()) {
      _prev = _next = it;
      _value.value = {};
      return false;
    }

    _prev = it;
    _next = ++it;
    _value.value = _prev->value;
    return true;
  }

  irs::SeekResult seek_ge(irs::bytes_view value) final {
    auto it = _data.terms.lower_bound(Term{value});
    if (it == _data.terms.end()) {
      _prev = _next = it;
      _value.value = irs::bytes_view{};
      return irs::SeekResult::End;
    }

    _prev = it;
    _next = ++it;
    _value.value = _prev->value;
    return this->value() == value ? irs::SeekResult::Found
                                  : irs::SeekResult::NotFound;
  }

  DocIteratorImpl::ptr postings(irs::IndexFeatures features) const final {
    return irs::memory::make_managed<DocIteratorImpl>(
      _data.index_features & features, *_prev);
  }

  irs::SeekCookie::ptr cookie() const final {
    return std::make_unique<TermCookie>(_value.value);
  }

 private:
  const tests::Field& _data;
  std::set<tests::Term>::const_iterator _prev;
  std::set<tests::Term>::const_iterator _next;
  irs::TermAttr _value;
  irs::TermMeta _meta;
};

irs::SeekTermIterator::ptr Field::iterator() const {
  return irs::memory::make_managed<TermIterator>(*this);
}

template<typename IteratorFactory>
void AssertDocs(irs::DocIterator::ptr expected_docs,
                IteratorFactory&& factory) {
  ASSERT_NE(nullptr, expected_docs);

  auto seek_docs = factory();
  ASSERT_NE(nullptr, seek_docs);

  auto seq_docs = factory();
  ASSERT_NE(nullptr, seq_docs);

  ASSERT_TRUE(!irs::doc_limits::valid(expected_docs->value()));
  ASSERT_TRUE(!irs::doc_limits::valid(seq_docs->value()));
  ASSERT_TRUE(!irs::doc_limits::valid(seek_docs->value()));

  while (expected_docs->next()) {
    const auto expected_doc = expected_docs->value();

    ASSERT_TRUE(seq_docs->next());
    ASSERT_EQ(expected_doc, seq_docs->value());

    ASSERT_EQ(expected_doc, seek_docs->seek(expected_doc));
    ASSERT_EQ(expected_doc, seek_docs->value());

    // check document attributes
    {
      auto* expected_freq = irs::get<irs::FreqAttr>(*expected_docs);
      auto* actual_freq = irs::get<irs::FreqAttr>(*seq_docs);

      if (expected_freq) {
        ASSERT_FALSE(!actual_freq);
        ASSERT_EQ(expected_freq->value, actual_freq->value);
      }

      auto* expected_pos = irs::GetMutable<irs::PosAttr>(expected_docs.get());
      auto* actual_pos = irs::GetMutable<irs::PosAttr>(seq_docs.get());

      if (expected_pos) {
        ASSERT_FALSE(!actual_pos);

        auto* expected_offs = irs::get<irs::OffsAttr>(*expected_pos);
        auto* actual_offs = irs::get<irs::OffsAttr>(*actual_pos);
        if (expected_offs)
          ASSERT_FALSE(!actual_offs);

        auto* expected_pay = irs::get<irs::PayAttr>(*expected_pos);
        auto* actual_pay = irs::get<irs::PayAttr>(*actual_pos);
        if (expected_pay)
          ASSERT_FALSE(!actual_pay);
        ASSERT_TRUE(!irs::pos_limits::valid(expected_pos->value()));
        ASSERT_TRUE(!irs::pos_limits::valid(actual_pos->value()));
        for (; expected_pos->next();) {
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
    }
  }

  ASSERT_TRUE(irs::doc_limits::eof(expected_docs->value()));
  ASSERT_FALSE(seq_docs->next());
  ASSERT_TRUE(irs::doc_limits::eof(seq_docs->value()));
  ASSERT_FALSE(seek_docs->next());
  ASSERT_TRUE(irs::doc_limits::eof(seek_docs->value()));
}

void AssertDocs(const irs::TermIterator& expected_term,
                const irs::TermReader& actual_terms,
                irs::SeekCookie::ptr actual_cookie,
                irs::IndexFeatures requested_features) {
  AssertDocs(expected_term.postings(requested_features), [&]() {
    return actual_terms.postings(*actual_cookie, requested_features);
  });

  AssertDocs(expected_term.postings(requested_features), [&]() {
    // FIXME(gnusi)
    const irs::WanderatorOptions options{
      .factory = [](const irs::AttributeProvider&) {
        return irs::ScoreFunction::Default(1);
      }};

    return actual_terms.wanderator(*actual_cookie, requested_features, options,
                                   {.index = 0, .strict = false});
  });

  // FIXME(gnusi): check BitUnion
}

void AssertTerm(irs::TermIterator& expected_term,
                irs::TermIterator& actual_term,
                irs::IndexFeatures requested_features) {
  ASSERT_EQ(expected_term.value(), actual_term.value());

  auto* expected_meta = irs::get<irs::TermMeta>(expected_term);
  ASSERT_NE(nullptr, expected_meta);
  auto* actual_meta = irs::get<irs::TermMeta>(actual_term);
  ASSERT_NE(nullptr, actual_meta);

  expected_term.read();
  actual_term.read();

  ASSERT_EQ(expected_meta->docs_count, actual_meta->docs_count);
  // FIXME(gnusi): uncomment
  // ASSERT_EQ(expected_meta->freq, actual_meta->freq);

  AssertDocs(expected_term.postings(requested_features),
             [&]() { return actual_term.postings(requested_features); });
}

void AssertTermsNext(const Field& expected_field,
                     const irs::TermReader& actual_field,
                     irs::IndexFeatures features,
                     irs::automaton_table_matcher* matcher) {
  irs::bytes_view actual_min{};
  irs::bytes_view actual_max{};
  irs::bstring actual_min_buf;
  irs::bstring actual_max_buf;
  size_t actual_size = 0;

  auto expected_term = expected_field.iterator();
  if (matcher) {
    expected_term = irs::memory::make_managed<irs::AutomatonTermIterator>(
      matcher->GetFst(), std::move(expected_term));
  }

  auto actual_term = matcher ? actual_field.iterator(*matcher)
                             : actual_field.iterator(irs::SeekMode::NORMAL);

  for (; expected_term->next(); ++actual_size) {
    ASSERT_TRUE(actual_term->next());

    AssertTerm(*expected_term, *actual_term, features);
    AssertDocs(*expected_term, actual_field, actual_term->cookie(), features);

    if (irs::IsNull(actual_min)) {
      actual_min_buf = actual_term->value();
      actual_min = actual_min_buf;
    }

    actual_max_buf = actual_term->value();
    actual_max = actual_max_buf;
  }
  // FIXME(@gnusi): currently `SeekTermIterator` crashes
  //                if next() is called after iterator is exhausted
  // ASSERT_FALSE(actual_term->next());
  // ASSERT_FALSE(actual_term->next());

  // check term reader
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
  auto expected_term = expected_field.iterator();
  if (matcher) {
    expected_term = irs::memory::make_managed<irs::AutomatonTermIterator>(
      matcher->GetFst(), std::move(expected_term));
  }

  auto actual_term_with_state =
    matcher ? actual_field.iterator(*matcher)
            : actual_field.iterator(irs::SeekMode::NORMAL);
  ASSERT_NE(nullptr, actual_term_with_state);

  auto actual_term_with_state_random_only =
    actual_field.iterator(irs::SeekMode::RandomOnly);
  ASSERT_NE(nullptr, actual_term_with_state_random_only);

  for (; expected_term->next();) {
    // seek with state
    {
      ASSERT_TRUE(actual_term_with_state->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term_with_state, features);
    }

    // seek without state random only
    {
      auto actual_term = actual_field.iterator(irs::SeekMode::RandomOnly);
      ASSERT_TRUE(actual_term->seek(expected_term->value()));

      AssertTerm(*expected_term, *actual_term, features);
    }

    // seek with state random only
    {
      ASSERT_TRUE(
        actual_term_with_state_random_only->seek(expected_term->value()));

      AssertTerm(*expected_term, *actual_term_with_state_random_only, features);
    }

    // seek without state, iterate forward
    irs::SeekCookie::ptr cookie;
    {
      auto actual_term = actual_field.iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);
      actual_term->read();
      cookie = actual_term->cookie();

      // iterate forward
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

      // seek back to initial term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);
    }

    // seek greater or equal without state, iterate forward
    {
      auto actual_term = actual_field.iterator(irs::SeekMode::NORMAL);
      ASSERT_EQ(irs::SeekResult::Found,
                actual_term->seek_ge(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);

      // iterate forward
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

      // seek back to initial term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);
    }

    // seek to cookie without state, iterate to the end
    {
      auto actual_term = actual_field.iterator(irs::SeekMode::NORMAL);

      // seek to the same term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);

      // seek to the same term
      ASSERT_TRUE(actual_term->seek(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);

      // seek greater equal to the same term
      ASSERT_EQ(irs::SeekResult::Found,
                actual_term->seek_ge(expected_term->value()));
      AssertTerm(*expected_term, *actual_term, features);
    }
  }
}

void AssertPk(const irs::ColumnReader& actual_reader,
              const auto& expected_values) {
  ASSERT_EQ(expected_values.size(), actual_reader.size());

  // check iterators & values
  {
    auto actual_it = actual_reader.iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, actual_it);

    auto actual_seek_it = actual_reader.iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, actual_seek_it);

    auto* actual_key = irs::get<irs::DocAttr>(*actual_it);
    ASSERT_NE(nullptr, actual_key);
    auto* actual_value = irs::get<irs::PayAttr>(*actual_it);
    ASSERT_NE(nullptr, actual_value);

    for (auto& [expected_value, expected_key, _] : expected_values) {
      ASSERT_TRUE(actual_it->next());

      auto actual_stateless_seek_it =
        actual_reader.iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, actual_stateless_seek_it);

      ASSERT_EQ(expected_key, actual_it->value());
      ASSERT_EQ(expected_key, actual_key->value);
      ASSERT_EQ(expected_value, actual_value->value);
      ASSERT_EQ(expected_key, actual_seek_it->seek(expected_key));
      ASSERT_EQ(expected_key, actual_stateless_seek_it->seek(expected_key));
    }
    ASSERT_FALSE(actual_it->next());
    ASSERT_FALSE(actual_it->next());
  }

  // check visit
  {
    auto begin = expected_values.begin();

    Visit(actual_reader,
          [&begin](auto actual_key, const auto& actual_value) mutable {
            auto& [expected_value, expected_key, _] = *begin;
            EXPECT_EQ(expected_key, actual_key);
            EXPECT_EQ(expected_value, actual_value);
            ++begin;
            return true;
          });
    ASSERT_EQ(begin, expected_values.end());
  }
}

void AssertColumn(const irs::ColumnReader* actual_reader,
                  const ColumnValues& expected_values) {
  if (!actual_reader) {
    ASSERT_TRUE(expected_values.empty());
    return;
  }

  if (irs::IsNull(expected_values.name())) {
    // field features are stored as annonymous columns
    ASSERT_TRUE(irs::IsNull(actual_reader->name()));
  } else {
    ASSERT_EQ(expected_values.name(), actual_reader->name());
  }

  if (!irs::IsNull(actual_reader->payload())) {
    // old formats may not support column header payload
    auto p1 = expected_values.payload();
    auto p2 = actual_reader->payload();
    ASSERT_EQ(p1, p2);
  }

  ASSERT_EQ(expected_values.size(), actual_reader->size());

  // check iterators & values
  {
    auto actual_it = actual_reader->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, actual_it);

    auto actual_seek_it = actual_reader->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, actual_seek_it);

    auto* actual_key = irs::get<irs::DocAttr>(*actual_it);
    ASSERT_NE(nullptr, actual_key);
    auto* actual_value = irs::get<irs::PayAttr>(*actual_it);
    ASSERT_NE(nullptr, actual_value);

    for (auto& [expected_key, expected_value] : expected_values) {
      ASSERT_TRUE(actual_it->next());

      auto actual_stateless_seek_it =
        actual_reader->iterator(irs::ColumnHint::Normal);
      ASSERT_NE(nullptr, actual_stateless_seek_it);

      ASSERT_EQ(expected_key, actual_it->value());
      ASSERT_EQ(expected_key, actual_key->value);
      ASSERT_EQ(expected_value, actual_value->value);
      ASSERT_EQ(expected_key, actual_seek_it->seek(expected_key));
      ASSERT_EQ(expected_key, actual_stateless_seek_it->seek(expected_key));
    }
    ASSERT_FALSE(actual_it->next());
    ASSERT_FALSE(actual_it->next());
  }

  // check visit
  {
    auto begin = expected_values.begin();

    Visit(*actual_reader,
          [&begin](auto actual_key, const auto& actual_value) mutable {
            auto& [expected_key, expected_value] = *begin;
            EXPECT_EQ(expected_key, actual_key);
            EXPECT_EQ(expected_value, actual_value);
            ++begin;
            return true;
          });
    ASSERT_EQ(begin, expected_values.end());
  }
}

void AssertColumnstore(irs::IndexReader::ptr actual_index,
                       const index_t& expected_index, size_t skip /*= 0*/) {
  // check number of segments
  ASSERT_EQ(expected_index.size(), actual_index->size());
  size_t i = 0;
  for (auto& actual_segment : *actual_index) {
    // skip segment if validation not required
    if (skip) {
      ++i;
      --skip;
      continue;
    }

    const tests::IndexSegment& expected_segment = expected_index[i];

    // check pk if present
    if (auto& expected_pk = expected_segment.pk(); !expected_pk.empty()) {
      auto* actual_pk = actual_segment.sort();
      ASSERT_NE(nullptr, actual_pk);
      AssertPk(*actual_pk, expected_pk);
    }

    // check stored columns
    auto& expected_columns = expected_segment.named_columns();
    auto expected_columns_begin = expected_columns.begin();
    auto actual_columns = actual_segment.columns();

    for (; actual_columns->next(); ++expected_columns_begin) {
      auto& actual_column = actual_columns->value();
      ASSERT_EQ(expected_columns_begin->first, actual_column.name());
      // column id is format dependent
      ASSERT_TRUE(
        irs::field_limits::valid(expected_columns_begin->second->id()));
      ASSERT_TRUE(irs::field_limits::valid(actual_column.id()));
      ASSERT_LT(expected_columns_begin->second->id(),
                expected_segment.columns().size());

      const auto* actual_column_reader =
        actual_segment.column(actual_column.id());
      ASSERT_EQ(actual_column_reader,
                actual_segment.column(actual_column.name()));

      AssertColumn(
        actual_column_reader,
        expected_segment.columns()[expected_columns_begin->second->id()]);
    }
    ASSERT_FALSE(actual_columns->next());
    ASSERT_EQ(expected_columns_begin, expected_columns.end());

    // check stored features
    auto check_feature = [&](irs::field_id expected, irs::field_id actual) {
      // we don't check column ids as they are format dependent
      ASSERT_EQ(irs::field_limits::valid(expected),
                irs::field_limits::valid(actual));
      if (irs::field_limits::valid(expected)) {
        ASSERT_LT(expected, expected_segment.columns().size());
        const auto* actual_column = actual_segment.column(actual);
        AssertColumn(actual_column, expected_segment.columns()[expected]);
      }
    };

    const auto& expected_fields = expected_segment.fields();
    auto expected_field = expected_fields.begin();
    auto actual_fields = actual_segment.fields();
    for (; actual_fields->next(); ++expected_field) {
      check_feature(expected_field->second.norm,
                    actual_fields->value().meta().norm);
    }
    ASSERT_FALSE(actual_fields->next());
    ASSERT_EQ(expected_field, expected_fields.end());

    ++i;
  }
}

void AssertColumnstore(const irs::Directory& dir, irs::Format::ptr codec,
                       const index_t& expected_index, size_t skip /*= 0*/) {
  auto reader = irs::DirectoryReader(dir, codec);
  ASSERT_NE(nullptr, reader);

  AssertColumnstore(reader.GetImpl(), expected_index, skip);
}

void AssertIndex(irs::IndexReader::ptr actual_index,
                 const index_t& expected_index, irs::IndexFeatures features,
                 size_t skip /*= 0*/,
                 irs::automaton_table_matcher* matcher /*=nullptr*/) {
  // check number of segments
  ASSERT_EQ(expected_index.size(), actual_index->size());
  size_t i = 0;
  for (auto& actual_segment : *actual_index) {
    // skip segment if validation not required
    if (skip) {
      ++i;
      --skip;
      continue;
    }

    const tests::IndexSegment& expected_segment = expected_index[i];

    // segment normally returns a reference to itself
    ASSERT_EQ(1, actual_segment.size());
    ASSERT_EQ(&actual_segment, &*actual_segment.begin());

    // get field name iterators
    auto& expected_fields = expected_segment.fields();
    auto expected_field = expected_fields.begin();

    // iterate over fields
    auto actual_fields = actual_segment.fields();
    for (; actual_fields->next(); ++expected_field) {
      ASSERT_EQ(expected_field->first, actual_fields->value().meta().name);
      ASSERT_EQ(expected_field->second.name,
                actual_fields->value().meta().name);
      ASSERT_EQ(expected_field->second.index_features,
                actual_fields->value().meta().index_features);

      // check field terms
      const auto* actual_terms = actual_segment.field(expected_field->first);
      ASSERT_NE(nullptr, actual_terms);
      ASSERT_EQ(actual_fields->value().meta(), actual_terms->meta());

      // check term reader
      ASSERT_EQ((expected_field->second.min)(), (actual_terms->min)());
      ASSERT_EQ((expected_field->second.max)(), (actual_terms->max)());
      ASSERT_EQ(expected_field->second.terms.size(), actual_terms->size());
      ASSERT_EQ(expected_field->second.docs.size(), actual_terms->docs_count());

      // check field meta
      const irs::FieldMeta& expected_meta = expected_field->second;
      const irs::FieldMeta& actual_meta = actual_terms->meta();
      ASSERT_EQ(expected_meta.name, actual_meta.name);
      ASSERT_EQ(expected_meta.index_features, actual_meta.index_features);
      // we don't check column ids as they are format dependent
      ASSERT_EQ(irs::field_limits::valid(expected_meta.norm),
                irs::field_limits::valid(actual_meta.norm));
      ASSERT_EQ(
        irs::IsSubsetOf(irs::IndexFeatures::Norm, expected_meta.index_features),
        irs::field_limits::valid(expected_meta.norm));
      ASSERT_EQ(
        irs::IsSubsetOf(irs::IndexFeatures::Norm, actual_meta.index_features),
        irs::field_limits::valid(actual_meta.norm));

      auto* actual_freq = irs::get<irs::FreqAttr>(*actual_terms);
      if (irs::IndexFeatures::None !=
          (expected_field->second.index_features & irs::IndexFeatures::Freq)) {
        ASSERT_NE(nullptr, actual_freq);
        ASSERT_EQ(expected_field->second.total_freq(), actual_freq->value);
      } else {
        ASSERT_EQ(nullptr, actual_freq);
      }

      AssertTermsNext(expected_field->second, *actual_terms, features, matcher);
      AssertTermsSeek(expected_field->second, *actual_terms, features, matcher);
    }
    ASSERT_FALSE(actual_fields->next());

    ++i;
    ASSERT_EQ(expected_fields.end(), expected_field);
  }
}

void AssertIndex(const irs::Directory& dir, irs::Format::ptr codec,
                 const index_t& expected_index, irs::IndexFeatures features,
                 size_t skip /*= 0*/,
                 irs::automaton_table_matcher* matcher /*= nullptr*/) {
  auto reader = irs::DirectoryReader(dir, codec);
  ASSERT_NE(nullptr, reader);

  AssertIndex(reader.GetImpl(), expected_index, features, skip, matcher);
}

}  // namespace tests

namespace irs {

// use base irs::position type for ancestors
template<>
struct Type<tests::DocIteratorImpl::PosIterator> : Type<irs::PosAttr> {};

}  // namespace irs
