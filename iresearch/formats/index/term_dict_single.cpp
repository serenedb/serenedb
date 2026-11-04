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

#include "iresearch/formats/index/term_dict_single.hpp"

#include <cstring>
#include <limits>
#include <string_view>
#include <utility>

#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/formats/posting/reader.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::term_dict {
namespace {

template<bool HasPayload>
class SingleIterator : public SeekTermIterator {
 public:
  explicit SingleIterator(const SingleTermReader& field,
                          byte_type payload = 0) noexcept
    : _field{&field}, _payload{payload} {
    if constexpr (HasPayload) {
      _pay.value = {&_payload, sizeof(_payload)};
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if constexpr (HasPayload) {
      if (type == irs::Type<PayAttr>::id()) {
        return &_pay;
      }
    }
    return type == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  bytes_view value() const noexcept final { return _term.value; }

  bool next() final {
    if (_position != Position::Before) {
      return End();
    }
    _position = Position::At;
    _term.value = _field->min();
    return true;
  }

  SeekResult seek_ge(bytes_view target) final {
    const auto term = _field->min();
    if (term < target) {
      End();
      return SeekResult::End;
    }
    _position = Position::At;
    _term.value = term;
    return term == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return seek_ge(target) == SeekResult::Found;
  }

  const PostingMeta& cookie() const final { return _field->SingleMeta(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return ReusePostings(features, {});
  }

  TermPostings::ptr ReusePostings(IndexFeatures features,
                                  TermPostings::ptr reuse) const final {
    return _field->PostingsOf(features, _field->SingleMeta(), std::move(reuse));
  }

 private:
  bool End() noexcept {
    _position = Position::After;
    _term.value = {};
    return false;
  }

  const SingleTermReader* _field;
  TermAttr _term;
  PayAttr _pay;
  byte_type _payload;
  Position _position = Position::Before;
};

template<typename A>
SeekTermIterator::ptr SingleFor(const SingleTermReader& field, const A& a) {
  if constexpr (A::kHasPayload) {
    typename A::PayloadType payload{};
    if (a.Matches(field.min(), payload)) {
      return memory::make_managed<SingleIterator<true>>(field, payload);
    }
  } else if (a.Matches(field.min())) {
    return memory::make_managed<SingleIterator<false>>(field);
  }
  return SeekTermIterator::empty();
}

}  // namespace

SingleTermReader::SingleTermReader(FieldReader& owner, const FieldHead& head)
  : _owner{&owner},
    _meta{head.id, head.features},
    _term{ViewCast<byte_type>(std::string_view{head.min})},
    _total_term_freq{head.text.total_freq},
    _has_score_bounds{head.text.has_score_bounds} {
  _meta.norm = head.text.norm;
  _meta.dict = {.layout = head.layout, .member = head.member};
  key_width = head.width;

  const auto corrupted = [&] { Corrupted("record", head.id); };
  const auto size = head.record.size();
  if (head.width > kMaxStride || size > kMaxRecordBytes) {
    corrupted();
  }
  RecordBytes record{};
  std::memcpy(record.bytes.data(), head.record.data(), size);
  if (PostingsReader::decode(record.bytes.data(), head.features, _single) !=
        size ||
      head.record_inline.size() != _single.inline_size) {
    corrupted();
  }
  if (_single.inline_size != 0) {
    _single.inline_data = owner.KeepInline(
      ViewCast<byte_type>(std::string_view{head.record_inline}));
  }
}

SeekTermIterator::ptr SingleTermReader::iterator() const {
  return memory::make_managed<SingleIterator<false>>(*this);
}

SeekTermIterator::ptr SingleTermReader::iterator(
  const RegexpAcceptor& a) const {
  return SingleFor(*this, a);
}

SeekTermIterator::ptr SingleTermReader::iterator(
  const LevenshteinAcceptor& a) const {
  return SingleFor(*this, a);
}

SeekTermIterator::ptr SingleTermReader::iterator(
  const RegexpConjunction& a) const {
  return SingleFor(*this, a);
}

SeekTermIterator::ptr SingleTermReader::iterator(
  const FuzzyConjunction& a) const {
  return SingleFor(*this, a);
}

void SingleTermReader::LookupBatch(size_t count, TermAt term,
                                   FoundTerm found) const {
  for (size_t i = 0; i != count; ++i) {
    if (term(i) == min()) {
      found(i, _single);
    }
  }
}

void SingleTermReader::VisitRange(bytes_view from, bytes_view to,
                                  RangeVisitor visit) const {
  if ((from.empty() || from <= min()) && (to.empty() || min() < to)) {
    visit(min(), _single);
  }
}

std::unique_ptr<IndexInput> SingleTermReader::ReopenPayload() const {
  return _owner->Postings().ReopenPayload();
}

PostingsHandles SingleTermReader::Handles() const noexcept {
  return _owner->Postings().Handles();
}

TermPostings::ptr SingleTermReader::PostingsOf(IndexFeatures features,
                                               const PostingMeta& meta,
                                               TermPostings::ptr reuse) const {
  return _owner->Postings().Postings(_meta.index_features, features, meta,
                                     _has_score_bounds, std::move(reuse));
}

}  // namespace irs::term_dict
