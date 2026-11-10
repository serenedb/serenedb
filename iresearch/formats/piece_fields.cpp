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

#include "iresearch/formats/piece_fields.hpp"

#include <absl/algorithm/container.h>

#include <array>
#include <limits>
#include <utility>

#include "iresearch/formats/index/term_dict_reader.hpp"
#include "iresearch/utils/conjunction_acceptor.hpp"
#include "iresearch/utils/levenshtein_acceptor.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"

namespace irs {
namespace {

enum class Position : uint8_t {
  Before,
  At,
  After,
};

template<bool kHasPayload>
class PieceTermIterator : public SeekTermIterator {
 public:
  explicit PieceTermIterator(const PieceTermReader& field) noexcept
    : _field{&field}, _count{field.size()} {}

  PieceTermIterator(const PieceTermReader& field, std::vector<uint32_t> matches,
                    std::vector<byte_type> payloads) noexcept
    : _field{&field},
      _matches{std::move(matches)},
      _payloads{std::move(payloads)},
      _count{_matches.size()},
      _filtered{true} {
    if constexpr (kHasPayload) {
      _pay.value = {&_payload, sizeof(_payload)};
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if constexpr (kHasPayload) {
      if (type == irs::Type<PayAttr>::id()) {
        return &_pay;
      }
    }
    return type == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  bytes_view value() const noexcept final { return _term.value; }

  bool next() final {
    switch (_position) {
      case Position::After:
        return false;
      case Position::Before:
        _at = 0;
        break;
      case Position::At:
        ++_at;
        break;
    }
    return Stand();
  }

  SeekResult seek_ge(bytes_view target) final {
    if (_filtered) {
      _at = static_cast<size_t>(
        absl::c_lower_bound(_matches, target,
                            [&](uint32_t entry, bytes_view term) {
                              return _field->TermOf(entry) < term;
                            }) -
        _matches.begin());
    } else {
      _at = _field->LowerBound(target);
    }
    if (!Stand()) {
      return SeekResult::End;
    }
    return _term.value == target ? SeekResult::Found : SeekResult::NotFound;
  }

  bool seek(bytes_view target) final {
    return seek_ge(target) == SeekResult::Found;
  }

  const PostingMeta& cookie() const final { return _field->MetaAt(Entry()); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return ReusePostings(features, {});
  }

  TermPostings::ptr ReusePostings(IndexFeatures features,
                                  TermPostings::ptr reuse) const final {
    return _field->PostingsOf(features, _field->MetaAt(Entry()),
                              std::move(reuse));
  }

 private:
  size_t Entry() const noexcept { return _filtered ? _matches[_at] : _at; }

  bool Stand() noexcept {
    if (_at >= _count) {
      _position = Position::After;
      _term.value = {};
      return false;
    }
    _position = Position::At;
    _term.value = _field->TermOf(Entry());
    if constexpr (kHasPayload) {
      _payload = _payloads[_at];
    }
    return true;
  }

  const PieceTermReader* _field;
  std::vector<uint32_t> _matches;
  std::vector<byte_type> _payloads;
  TermAttr _term;
  PayAttr _pay;
  byte_type _payload = 0;
  size_t _count;
  size_t _at = 0;
  Position _position = Position::Before;
  bool _filtered = false;
};

template<typename A>
SeekTermIterator::ptr Filtered(const PieceTermReader& field, const A& a) {
  std::vector<uint32_t> matches;
  std::vector<byte_type> payloads;
  for (size_t i = 0; i != field.size(); ++i) {
    if constexpr (A::kHasPayload) {
      typename A::PayloadType payload{};
      if (a.Matches(field.TermOf(i), payload)) {
        matches.push_back(static_cast<uint32_t>(i));
        payloads.push_back(static_cast<byte_type>(payload));
      }
    } else if (a.Matches(field.TermOf(i))) {
      matches.push_back(static_cast<uint32_t>(i));
    }
  }
  if (matches.empty()) {
    return SeekTermIterator::empty();
  }
  return memory::make_managed<PieceTermIterator<A::kHasPayload>>(
    field, std::move(matches), std::move(payloads));
}

}  // namespace

PieceTermReader::PieceTermReader(const PieceFields& owner, FieldMeta meta,
                                 uint32_t width) noexcept
  : _owner{&owner}, _meta{std::move(meta)} {
  key_width = width;
}

void PieceTermReader::Add(bytes_view term, const PostingMeta& meta) {
  SDB_ASSERT(meta.docs_count != 0);
  SDB_ASSERT(_entries.empty() || TermOf(_entries.size() - 1) < term);
  _entries.push_back({.begin = static_cast<uint32_t>(_terms.size()),
                      .size = static_cast<uint32_t>(term.size()),
                      .inline_at = static_cast<uint32_t>(_inline.size()),
                      .meta = meta});
  _terms.append(term);
  _inline.append(meta.Inline());
}

void PieceTermReader::Seal(uint64_t docs, uint64_t total_freq) {
  SDB_ENSURE(total_freq <= std::numeric_limits<uint32_t>::max(),
             "piece: total frequency ", total_freq, " exceeds uint32_t::max");
  _docs = docs;
  _freq.value = static_cast<uint32_t>(total_freq);
  for (auto& entry : _entries) {
    entry.meta.inline_data =
      entry.meta.inline_size == 0 ? nullptr : _inline.data() + entry.inline_at;
  }
}

size_t PieceTermReader::LowerBound(bytes_view term) const noexcept {
  size_t lo = 0;
  size_t hi = _entries.size();
  while (lo < hi) {
    const auto mid = lo + (hi - lo) / 2;
    if (TermOf(mid) < term) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

size_t PieceTermReader::Find(bytes_view term) const noexcept {
  const auto i = LowerBound(term);
  return i != _entries.size() && TermOf(i) == term ? i : _entries.size();
}

SeekTermIterator::ptr PieceTermReader::iterator() const {
  return memory::make_managed<PieceTermIterator<false>>(*this);
}

SeekTermIterator::ptr PieceTermReader::iterator(const RegexpAcceptor& a) const {
  return Filtered(*this, a);
}

SeekTermIterator::ptr PieceTermReader::iterator(
  const LevenshteinAcceptor& a) const {
  return Filtered(*this, a);
}

SeekTermIterator::ptr PieceTermReader::iterator(
  const RegexpConjunction& a) const {
  return Filtered(*this, a);
}

SeekTermIterator::ptr PieceTermReader::iterator(
  const FuzzyConjunction& a) const {
  return Filtered(*this, a);
}

PostingMeta PieceTermReader::Lookup(bytes_view term) const {
  const auto i = Find(term);
  return i == _entries.size() ? PostingMeta{} : _entries[i].meta;
}

void PieceTermReader::LookupBatch(size_t count, TermAt term,
                                  FoundTerm found) const {
  for (size_t i = 0; i != count; ++i) {
    if (const auto at = Find(term(i)); at != _entries.size()) {
      found(i, _entries[at].meta);
    }
  }
}

void PieceTermReader::ReadDocs(bytes_view term, Acceptor acceptor) const {
  term_dict::ReadPostingDocs(*this, Lookup(term), acceptor);
}

void PieceTermReader::VisitRange(bytes_view from, bytes_view to,
                                 RangeVisitor visit) const {
  for (auto i = from.empty() ? 0 : LowerBound(from); i != _entries.size();
       ++i) {
    const auto term = TermOf(i);
    if (!to.empty() && !(term < to)) {
      return;
    }
    visit(term, _entries[i].meta);
  }
}

void PieceTermReader::SampleTerms(
  uint64_t bytes,
  absl::FunctionRef<void(std::span<const bytes_view>)> visit) const {
  constexpr size_t kRun = 16;
  constexpr uint64_t kRunBytes = 256;
  const auto runs = std::max<uint64_t>(1, bytes / kRunBytes);
  const auto step =
    std::max<size_t>(kRun, static_cast<size_t>(_entries.size() / runs));
  std::array<bytes_view, kRun> run;
  uint64_t taken = 0;
  for (size_t i = 0; i < _entries.size() && taken < bytes; i += step) {
    size_t n = 0;
    for (; n != kRun && i + n != _entries.size(); ++n) {
      run[n] = TermOf(i + n);
      taken += run[n].size();
    }
    visit({run.data(), n});
  }
}

std::unique_ptr<IndexInput> PieceTermReader::ReopenPayload() const {
  return _owner->Postings().ReopenPayload();
}

PostingsHandles PieceTermReader::Handles() const noexcept {
  return _owner->Postings().Handles();
}

Attribute* PieceTermReader::GetMutable(TypeInfo::type_id type) noexcept {
  return type == irs::Type<FreqAttr>::id() &&
             _meta.dict.member == DictMember::Strings
           ? &_freq
           : nullptr;
}

TermPostings::ptr PieceTermReader::PostingsOf(IndexFeatures features,
                                              const PostingMeta& meta,
                                              TermPostings::ptr reuse) const {
  return _owner->Postings().Postings(_meta.index_features, features, meta,
                                     false, std::move(reuse));
}

std::span<const TermReader* const> PieceFields::fields(field_id id) const {
  const auto it = _by_id.find(id);
  if (it == _by_id.end()) {
    return {};
  }
  return std::span{_dicts}.subspan(it->second.first, it->second.count);
}

PieceTermReader& PieceFields::Add(FieldMeta meta, uint32_t width) {
  return _readers.emplace_back(*this, std::move(meta), width);
}

void PieceFields::Seal(PieceTermReader& reader, uint64_t docs,
                       uint64_t total_freq) {
  SDB_ASSERT(&reader == &_readers.back());
  if (reader.size() == 0) {
    _readers.pop_back();
    return;
  }
  reader.Seal(docs, total_freq);
  const auto id = reader.meta().id;
  _dicts.push_back(&reader);
  ++_by_id
      .try_emplace(id,
                   Members{.first = static_cast<uint32_t>(_dicts.size() - 1)})
      .first->second.count;
  if (_ids.empty() || _ids.back() != id) {
    _ids.push_back(id);
  }
}

void PieceFields::Open(const ReaderState& state, IndexFeatures features) {
  _pr.prepare(state, features);
}

size_t PieceFields::Bytes() const noexcept {
  size_t bytes = _dicts.capacity() * sizeof(const TermReader*);
  for (const auto& reader : _readers) {
    bytes += sizeof(PieceTermReader) + reader.Bytes();
  }
  return bytes;
}

void PieceFieldWriter::prepare(const FlushState& state, uint64_t) {
  _features = state.index_features;
  _pw.Prepare(state);
}

void PieceFieldWriter::write(DictRef dict, const BasicTermReader& reader) {
  const auto props = reader.properties();
  _pw.SetTermPayloadWriter(reader.PayloadWriter());
  _pw.BeginField(props);
  FieldMeta meta{reader.id(), props.index_features};
  meta.norm = props.norm;
  meta.dict = dict;
  auto& field = _fields->Add(std::move(meta), reader.key_width);
  const bool freq =
    IndexFeatures::None != (props.index_features & IndexFeatures::Freq);
  uint64_t total_freq = 0;
  const auto consume = [&](bytes_view term, const PostingMeta& posting) {
    total_freq += freq ? posting.freq : posting.docs_count;
    if (posting.docs_count != 0) {
      field.Add(term, posting);
    }
  };
  auto terms = reader.iterator();
  SDB_ASSERT(terms);
  if (_compaction) {
    while (terms->next()) {
      PostingMeta posting;
      _pw.Write(*terms->postings(props.index_features), posting);
      consume(terms->value(), posting);
    }
  } else {
    constexpr size_t kTermBatch = 128;
    bytes_view term_batch[kTermBatch];
    PostingRows posting_batch[kTermBatch];
    while (const size_t n = terms->NextTermsWithPostings(
             term_batch, posting_batch, props.index_features)) {
      for (size_t i = 0; i != n; ++i) {
        PostingMeta posting;
        _pw.WritePostings(posting_batch[i], posting);
        consume(term_batch[i], posting);
      }
    }
  }
  const auto stats = _pw.EndField();
  _fields->Seal(field, stats.docs_count, total_freq);
}

void PieceFieldWriter::WriteSlot(field_id id, DictRef dict,
                                 std::span<const doc_id_t> docs) {
  auto& field = BeginSlot(id, dict);
  PostingMeta posting;
  _pw.WritePostings(
    PostingRows{.span = {.docs = docs.data(), .count = docs.size()}}, posting);
  EndSlot(field, posting);
}

void PieceFieldWriter::WriteSlot(field_id id, DictRef dict,
                                 TermPostings& docs) {
  auto& field = BeginSlot(id, dict);
  PostingMeta posting;
  _pw.Write(docs, posting);
  EndSlot(field, posting);
}

PieceTermReader& PieceFieldWriter::BeginSlot(field_id id, DictRef dict) {
  _pw.SetTermPayloadWriter(nullptr);
  _pw.BeginField({});
  FieldMeta meta{id, IndexFeatures::None};
  meta.dict = dict;
  return _fields->Add(std::move(meta), 0);
}

void PieceFieldWriter::EndSlot(PieceTermReader& field,
                               const PostingMeta& posting) {
  if (posting.docs_count != 0) {
    field.Add(kSlotTerm, posting);
  }
  _pw.EndField();
  _fields->Seal(field, posting.docs_count, 0);
}

void PieceFieldWriter::end() { _pw.End(); }

}  // namespace irs
