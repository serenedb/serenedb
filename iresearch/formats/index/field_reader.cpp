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

#include "iresearch/formats/index/field_reader.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::term_dict {
namespace {

constexpr size_t kBatchTerms = 64;

std::vector<uint32_t> DecodePrefixes(bytes_view encoded, field_id id) {
  std::vector<uint32_t> prefixes;
  prefixes.reserve(
    absl::c_count_if(encoded, [](byte_type b) { return b < 0x80; }));
  uint64_t value = 0;
  for (const auto* p = encoded.data(); p != encoded.data() + encoded.size();) {
    uint64_t delta = 0;
    for (uint32_t shift = 0;; shift += 7) {
      if (p == encoded.data() + encoded.size() || shift > 28) {
        Corrupted("prefixes", id);
      }
      const byte_type b = *p++;
      delta |= uint64_t{b & 0x7Fu} << shift;
      if (b < 0x80) {
        break;
      }
    }
    value += delta;
    if ((delta == 0 && !prefixes.empty()) ||
        value > std::numeric_limits<uint32_t>::max()) {
      Corrupted("prefixes", id);
    }
    prefixes.push_back(static_cast<uint32_t>(value));
  }
  return prefixes;
}

constexpr uint64_t kWordBits = 64;

void SetBitOf(std::vector<uint64_t>& words, uint64_t bit) noexcept {
  SetBit(words[bit / kWordBits], bit % kWordBits);
}

bool BitOf(std::span<const uint64_t> words, uint64_t bit) noexcept {
  return CheckBit(words[bit / kWordBits], bit % kWordBits);
}

uint32_t Find(const BlockView& view, Probe key,
              const duckdb_fsst_decoder_t* fsst) noexcept {
  if (!view.fixed) {
    const auto bound = VarLowerBound(view, key, fsst);
    return bound.equal ? bound.entry : kNoEntry;
  }
  if (key.size != size_t{view.prefix} + view.stride ||
      PaddedMismatch(Prefix(view).data(), key.data, view.prefix) !=
        view.prefix) {
    return kNoEntry;
  }
  const auto entry = FixedLowerBound(view, key);
  if (entry == view.count ||
      PaddedMismatch(FixedKey(view, entry).data(), key.data + view.prefix,
                     view.stride) != view.stride) {
    return kNoEntry;
  }
  return entry;
}

const byte_type* MapAt(IndexInput& in, uint64_t offset, uint64_t size,
                       bstring& copy) {
  if (const auto* p = in.ReadStable(offset, size)) {
    return p;
  }
  copy.resize(size);
  in.ReadData(offset, copy.data(), size);
  return copy.data();
}

IndexInput& Reopened(BlockBuffer& buffer, const IndexInput& in) {
  if (!buffer.in) {
    buffer.in = in.Reopen();
  }
  return *buffer.in;
}

PostingMeta DecodeRecord(const TermReaderImpl& field, const BlockView& view,
                         uint32_t block, uint32_t entry) {
  uint32_t count = 0;
  auto cursor = FieldRecords(field, view, block, entry, count);
  PostingMeta meta;
  DecodeRecords(cursor, count, field.meta().index_features, meta);
  return meta;
}

class TermIteratorImpl : public SeekTermIterator {
 public:
  explicit TermIteratorImpl(const TermReaderImpl& field) noexcept
    : _cursor{field} {}

  void ReadaheadUntil(uint32_t end) noexcept {
    _cursor.ReadaheadUntil(end, true);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return type == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  bytes_view value() const noexcept final { return _term.value; }

  bool next() final {
    switch (_position) {
      case Position::After:
        return false;
      case Position::Before:
        if (_cursor.Field().size() == 0) {
          return End();
        }
        _cursor.First();
        break;
      case Position::At:
        if (!_cursor.Advance()) {
          return End();
        }
        break;
    }
    _position = Position::At;
    _term.value = _cursor.Term();
    return true;
  }

  SeekResult seek_ge(bytes_view term) final { return Seek(term, false); }

  bool seek(bytes_view term) final {
    return Seek(term, true) == SeekResult::Found;
  }

  const PostingMeta& cookie() const final { return _cursor.Meta(); }

  TermPostings::ptr postings(IndexFeatures features) const final {
    return ReusePostings(features, {});
  }

  TermPostings::ptr ReusePostings(IndexFeatures features,
                                  TermPostings::ptr reuse) const final {
    return _cursor.Field().PostingsOf(features, _cursor.Meta(),
                                      std::move(reuse));
  }

 private:
  SeekResult Seek(bytes_view term, bool prefetch) {
    const auto result = _cursor.SeekGe(term, prefetch);
    if (result == SeekResult::End) {
      End();
      return result;
    }
    _position = Position::At;
    _term.value = _cursor.Term();
    return result;
  }

  bool End() noexcept {
    _position = Position::After;
    _term.value = {};
    return false;
  }

  mutable Cursor _cursor;
  TermAttr _term;
  Position _position = Position::Before;
};

}  // namespace

PrefixIndex::PrefixIndex(std::vector<uint32_t> prefixes)
  : _prefixes{std::move(prefixes)} {
  for (size_t i = 0; i != _prefixes.size(); ++i) {
    const uint32_t first = _prefixes[i] >> 24;
    const uint32_t pair = _prefixes[i] >> 16;
    if (i == 0 || pair != _prefixes[i - 1] >> 16) {
      _pair_starts.push_back(static_cast<uint32_t>(i));
      _pairs[pair / 64] |= uint64_t{1} << (pair % 64);
      ++_pair_rows[first + 1];
    }
    _firsts[first / 64] |= uint64_t{1} << (first % 64);
    ++_buckets[first + 1];
  }
  _pair_starts.push_back(static_cast<uint32_t>(_prefixes.size()));
  for (size_t i = 1; i != _buckets.size(); ++i) {
    _buckets[i] += _buckets[i - 1];
    _pair_rows[i] += _pair_rows[i - 1];
  }
}

InlinePages::InlinePages(bytes_view starts, uint32_t blocks, uint64_t offset,
                         uint64_t size)
  : _begin{offset}, _end{offset + size}, _blocks{blocks} {
  const auto corrupted = [] { Corrupted("inline starts"); };
  if (blocks > starts.size()) {
    corrupted();
  }
  bstring padded{starts};
  padded.append(kSlack, byte_type{0});
  const auto* p = padded.data();
  const auto* const end = p + starts.size();
  const uint64_t first = offset / kPage;
  const uint64_t words = (uint64_t{blocks} + kWordBits - 1) / kWordBits;
  _ones.resize((blocks + (offset + size) / kPage - first + kWordBits - 1) /
               kWordBits);
  _empty.resize(words);
  _samples.reserve(words);
  uint64_t start = 0;
  for (uint32_t block = 0; block != blocks; ++block) {
    if (p >= end) {
      corrupted();
    }
    const auto delta = vread<uint64_t>(p);
    if (p > end || delta > size - start) {
      corrupted();
    }
    if (block != 0 && delta == 0) {
      SetBitOf(_empty, block - 1);
    }
    start += delta;
    const uint64_t at = block + (offset + start) / kPage - first;
    SetBitOf(_ones, at);
    if (block % kWordBits == 0) {
      _samples.push_back(at);
    }
  }
  if (p != end) {
    corrupted();
  }
  if (blocks != 0 && start == size) {
    SetBitOf(_empty, blocks - 1);
  }
}

InlinePages::Span InlinePages::Of(uint32_t block) const noexcept {
  if (block >= _blocks || BitOf(_empty, block)) {
    return {};
  }
  const uint64_t first = _begin / kPage;
  const uint64_t at = Select(block);
  const uint64_t from = std::max(_begin, (first + at - block) * kPage);
  if (block + 1 == _blocks) {
    return {.from = from, .to = _end};
  }
  const uint64_t next = first + Next(at) - block - 1;
  return {.from = from, .to = std::min(_end, (next + 1) * kPage)};
}

uint64_t InlinePages::Select(uint64_t rank) const noexcept {
  const uint64_t sample = _samples[rank / kWordBits];
  uint64_t left = rank % kWordBits;
  size_t word = sample / kWordBits;
  uint64_t bits = _ones[word] & (~uint64_t{0} << (sample % kWordBits));
  for (auto count = static_cast<uint64_t>(std::popcount(bits)); count <= left;
       count = static_cast<uint64_t>(std::popcount(bits))) {
    left -= count;
    bits = _ones[++word];
  }
  return word * kWordBits + SelectInWord(bits, left);
}

uint64_t InlinePages::Next(uint64_t at) const noexcept {
  size_t word = at / kWordBits;
  uint64_t bits = _ones[word] & (~uint64_t{1} << (at % kWordBits));
  while (bits == 0) {
    bits = _ones[++word];
  }
  return word * kWordBits + static_cast<uint64_t>(std::countr_zero(bits));
}

TermReaderImpl::TermReaderImpl(FieldReader& owner, const FieldHead& head,
                               IndexInput& in, const Separators& separators)
  : _owner{&owner},
    _meta{head.key, head.features},
    _terms{head.term_count},
    _docs{head.doc_count},
    _has_score_bounds{head.has_score_bounds} {
  _meta.norm = head.norm;
  _meta.ranges = head.ranges;
  const auto total =
    IndexFeatures::None != (head.features & IndexFeatures::Freq)
      ? head.total_term_freq
      : head.total_doc_freq;
  SDB_ENSURE(total <= std::numeric_limits<uint32_t>::max(),
             "term dictionary: total frequency ", total,
             " exceeds uint32_t::max");
  _freq.value = static_cast<uint32_t>(total);

  const auto corrupted = [&] { Corrupted("record", head.key.id); };
  if (head.width > kMaxStride ||
      (!head.range_counts.empty() && head.record.size() != 0) ||
      (!head.prefixes.empty() && (head.width != 0 || !head.record.empty()))) {
    corrupted();
  }
  bstring prefix;
  AppendFieldPrefix(prefix, head.ordinal);
  _prefix_size = static_cast<uint32_t>(prefix.size());
  _meta.key_width = head.width;
  uint64_t bytes = 0;
  _min_key.assign(prefix);
  _min_key.append(head.min);
  if (!head.record.empty()) {
    _max_key = _min_key;
    RecordBytes record{};
    const auto size = head.record.size();
    if (size > kMaxRecordBytes) {
      corrupted();
    }
    std::memcpy(record.bytes.data(), head.record.data(), size);
    if (PostingsReader::DecodeDetached(record.bytes.data(), head.features,
                                       _single) != size ||
        head.record_inline.size() != _single.inline_size) {
      corrupted();
    }
    std::memcpy(_single.inline_data.data(), head.record_inline.data(),
                _single.inline_size);
  } else {
    _max_key.assign(prefix);
    _max_key.append(head.max);
    _first_block = head.first_block;
    _first_entry = head.first_entry;
    _first_record = head.first_record;
    const auto last_block = head.last_block;
    _last_entry = head.last_entry;
    if (last_block < _first_block || last_block >= owner.BlockCount() ||
        _max_key <= _min_key) {
      corrupted();
    }
    _blocks = last_block - _first_block + 1;
    uint64_t inline_size = 0;
    if (_blocks > 1) {
      std::vector<bytes_view> slice;
      slice.reserve(_blocks - 1);
      for (uint32_t block = _first_block; block != last_block; ++block) {
        slice.push_back(SeparatorAt(separators, block));
      }
      _root = Root{MakeSeparators(slice), _meta.key_width != 0};
      bytes += _root.MemoryBytes();
      if (!head.prefixes.empty()) {
        _prefixes = std::make_unique<PrefixIndex>(
          DecodePrefixes(head.prefixes, head.key.id));
        bytes += _prefixes->MemoryBytes();
      }
    } else if (!head.prefixes.empty()) {
      corrupted();
    } else {
      _inline_base = head.inline_base;
      inline_size = head.inline_size;
      if (_inline_base > owner.InlineEnd() - owner.InlineBegin() ||
          inline_size >
            owner.InlineEnd() - owner.InlineBegin() - _inline_base) {
        corrupted();
      }
    }
    if (!head.range_counts.empty()) {
      bytes += ReadRanges(owner, in, head);
    } else if (head.range_size != 0) {
      corrupted();
    }
    if (_blocks == 1) {
      _small = owner.CopyBlock(_first_block, in.Length());
      if (inline_size != 0) {
        _small_inline =
          owner.Copy(owner.InlineBegin() + _inline_base, inline_size);
      }
    }
  }
  owner.Account(bytes);
}

uint64_t TermReaderImpl::ReadRanges(FieldReader& owner, IndexInput& in,
                                    const FieldHead& head) {
  const auto corrupted = [&] { Corrupted("range levels", _meta.id); };
  const auto offset = head.range_offset;
  const auto size = head.range_size;
  const auto height = head.range_counts.size();
  _range_prefix = head.range_prefix;
  if (size == 0 || offset > owner.RangesSize() ||
      size > owner.RangesSize() - offset || height > kMaxRangeLevels ||
      _meta.key_width == 0 || _range_prefix > _meta.key_width ||
      min().substr(0, _range_prefix) != max().substr(0, _range_prefix)) {
    corrupted();
  }
  const auto* p = owner.Load(in, owner.RangesBegin() + offset, size).data();
  const auto* const end = p + size;
  const uint64_t stride = _meta.key_width - _range_prefix;
  _levels.reserve(height);
  for (size_t k = 0; k != height; ++k) {
    const auto count = head.range_counts[k];
    if (count < 2 || count > std::numeric_limits<uint32_t>::max()) {
      corrupted();
    }
    RangeLevel level{};
    level.count = static_cast<uint32_t>(count);
    const uint64_t firsts = k == 0 ? 0 : level.count * sizeof(uint32_t);
    const uint64_t offsets = (uint64_t{level.count} + 1) * sizeof(uint32_t);
    if (static_cast<uint64_t>(end - p) <
        level.count * stride + firsts + offsets) {
      corrupted();
    }
    level.starts = p;
    p += level.count * stride;
    level.firsts = k == 0 ? nullptr : p;
    p += firsts;
    level.offsets = p;
    p += offsets;
    level.records = p;
    const uint32_t records =
      absl::little_endian::Load32(level.offsets + offsets - sizeof(uint32_t));
    if (static_cast<uint64_t>(end - p) < records) {
      corrupted();
    }
    p += records;
    _levels.push_back(level);
  }
  if (static_cast<uint64_t>(end - p) != kSlack) {
    corrupted();
  }
  return _levels.capacity() * sizeof(RangeLevel);
}

void TermReaderImpl::ReadInline(PostingMeta& meta, BlockBuffer& buffer) const {
  const uint64_t begin = Small() ? _inline_base : 0;
  const uint64_t end =
    Small() ? _inline_base + _small_inline.size() : InlineEnd() - InlineBegin();
  if (meta.inline_end < begin + meta.inline_size || meta.inline_end > end) {
    Corrupted("inline postings", _meta.id);
  }
  const uint64_t at = meta.inline_end - meta.inline_size;
  if (Small()) {
    std::memcpy(meta.inline_data.data(), _small_inline.data() + (at - begin),
                meta.inline_size);
    return;
  }
  const uint64_t offset = InlineBegin() + at;
  if (const auto* base = _owner->Base()) {
    std::memcpy(meta.inline_data.data(), base + offset, meta.inline_size);
    return;
  }
  Reopened(buffer, _owner->Input())
    .ReadData(offset, meta.inline_data.data(), meta.inline_size);
}

void TermReaderImpl::SampleTerms(
  uint64_t bytes,
  absl::FunctionRef<void(std::span<const bytes_view>)> visit) const {
  constexpr size_t kRun = 16;
  constexpr uint64_t kRunBytes = 256;
  if (_terms == 0) {
    return;
  }
  if (Single()) {
    const auto term = min();
    visit({&term, 1});
    return;
  }
  const uint64_t runs = std::max<uint64_t>(1, bytes / kRunBytes);
  const uint32_t step =
    std::max<uint32_t>(1, static_cast<uint32_t>(_blocks / runs));
  Cursor cursor{*this};
  std::array<bstring, kRun> keys;
  std::array<bytes_view, kRun> run;
  uint64_t taken = 0;
  for (uint32_t block = 0; block < _blocks && taken < bytes; block += step) {
    if (block == 0) {
      cursor.First();
    } else {
      cursor.Load(block);
      cursor.Next();
    }
    size_t n = 0;
    for (;;) {
      keys[n].assign(cursor.Term());
      run[n] = keys[n];
      taken += keys[n].size();
      if (++n == kRun || cursor.AtBlockEnd()) {
        break;
      }
      cursor.Next();
    }
    visit({run.data(), n});
  }
}

const byte_type* TermReaderImpl::Block(size_t block,
                                       BlockBuffer& buffer) const {
  if (_small) {
    return _small;
  }
  const uint64_t offset = BlockOffset(block);
  if (const auto* base = _owner->Base()) {
    return base + offset;
  }
  auto& in = Reopened(buffer, _owner->Input());
  return MapAt(in, offset,
               std::min(BlockOffset(block + 1), in.Length()) - offset,
               buffer.copy);
}

void TermReaderImpl::PrefetchLookup(uint32_t block) const {
  if (Small()) {
    return;
  }
  const auto span = InlineSpan(block);
  if (span.from == span.to) {
    return;
  }
  std::array ranges{
    std::pair{BlockOffset(block), BlockOffset(block + 1)},
    std::pair{span.from, span.to},
  };
  PrefetchRanges(_owner->Input(), ranges);
}

SeekTermIterator::ptr TermReaderImpl::iterator() const {
  if (Single()) {
    return memory::make_managed<SingleIterator<false>>(*this);
  }
  return memory::make_managed<TermIteratorImpl>(*this);
}

SeekTermIterator::ptr TermReaderImpl::IteratorUntil(bytes_view upper) const {
  if (Single()) {
    return iterator();
  }
  auto it = memory::make_managed<TermIteratorImpl>(*this);
  if (!upper.empty() && !Small()) {
    it->ReadaheadUntil(BlockEnd(upper));
  }
  return it;
}

uint32_t TermReaderImpl::BlockEnd(bytes_view upper) const {
  if (upper.empty() || upper > max()) {
    return _blocks;
  }
  PaddedKey key;
  key.Assign(KeyPrefix(), upper);
  return BlockOf(key.Key()) + 1;
}

bool TermReaderImpl::Excludes(bytes_view term) const noexcept {
  return _terms == 0 || term < min() || max() < term ||
         (_meta.key_width != 0 && term.size() != _meta.key_width);
}

PostingMeta TermReaderImpl::Lookup(bytes_view term) const {
  if (Excludes(term)) {
    return {};
  }
  if (Single()) {
    return _single;
  }
  PaddedKey padded;
  const auto probe = padded.Assign(KeyPrefix(), term);
  const uint32_t block = BlockOf(padded.Key());
  PrefetchLookup(block);
  BlockBuffer buffer;
  const auto view = Parse(Block(block, buffer), Fsst());
  const auto entry = Find(view, probe, Fsst());
  if (entry == kNoEntry) {
    return {};
  }
  auto meta = DecodeRecord(*this, view, block, entry);
  if (meta.inline_size != 0) {
    ReadInline(meta, buffer);
  }
  return meta;
}

void TermReaderImpl::LookupBatch(std::span<const bytes_view> terms,
                                 std::span<PostingMeta> out) const {
  SDB_ASSERT(terms.size() == out.size());
  if (Single()) {
    for (size_t i = 0; i != terms.size(); ++i) {
      out[i] = Excludes(terms[i]) ? PostingMeta{} : _single;
    }
    return;
  }
  containers::SmallVector<uint32_t, kBatchTerms> blocks(terms.size(), kNoBlock);
  containers::SmallVector<std::pair<uint64_t, uint64_t>, 2 * kBatchTerms>
    ranges;
  PaddedKey padded;
  uint32_t last = kNoBlock;
  bytes_view last_term;
  for (size_t i = 0; i != terms.size(); ++i) {
    const auto term = terms[i];
    if (Excludes(term)) {
      continue;
    }
    if (Small()) {
      blocks[i] = 0;
      continue;
    }
    padded.Assign(KeyPrefix(), term);
    const auto block = last == kNoBlock || term < last_term
                         ? BlockOf(padded.Key())
                         : BlockFrom(last, padded.Key());
    if (block != last) {
      ranges.emplace_back(BlockOffset(block), BlockOffset(block + 1));
      if (const auto span = InlineSpan(block); span.from != span.to) {
        ranges.emplace_back(span.from, span.to);
      }
    }
    blocks[i] = last = block;
    last_term = term;
  }
  if (ranges.size() > 1) {
    PrefetchRanges(_owner->Input(), ranges);
  }

  BlockBuffer buffer;
  BlockView view{};
  uint32_t parsed = kNoBlock;
  for (size_t i = 0; i != terms.size(); ++i) {
    out[i] = {};
    if (blocks[i] == kNoBlock) {
      continue;
    }
    if (blocks[i] != parsed) {
      parsed = blocks[i];
      view = Parse(Block(parsed, buffer), Fsst());
    }
    const auto entry = Find(view, padded.Assign(KeyPrefix(), terms[i]), Fsst());
    if (entry == kNoEntry) {
      continue;
    }
    out[i] = DecodeRecord(*this, view, parsed, entry);
  }

  for (auto& meta : out) {
    if (meta.inline_size != 0) {
      ReadInline(meta, buffer);
    }
  }
}

void TermReaderImpl::ReadDocs(bytes_view term, Acceptor acceptor) const {
  const auto meta = Lookup(term);
  if (meta.docs_count == 0) {
    return;
  }
  if (meta.docs_count == 1) {
    acceptor(doc_limits::min() + meta.doc_delta);
    return;
  }
  auto docs_it = PostingsOf(IndexFeatures::None, meta, {});
  doc_id_t docs[doc_limits::kBlockSize];
  while (const auto n = docs_it->NextDocs(docs, nullptr)) {
    for (uint32_t i = 0; i != n; ++i) {
      SDB_ASSERT(doc_limits::valid(docs[i]));
      if (!acceptor(docs[i])) {
        return;
      }
    }
  }
}

std::unique_ptr<IndexInput> TermReaderImpl::ReopenPayload() const {
  return _owner->Postings().ReopenPayload();
}

PostingsHandles TermReaderImpl::Handles() const noexcept {
  return _owner->Postings().Handles();
}

Attribute* TermReaderImpl::GetMutable(TypeInfo::type_id type) noexcept {
  return type == irs::Type<FreqAttr>::id() ? &_freq : nullptr;
}

FieldReader::~FieldReader() { _rm->DecreaseChecked(_bytes); }

uint64_t FieldReader::CountMappedMemory() const {
  uint64_t bytes = _pr.CountMappedMemory();
  if (_in) {
    bytes += _in->CountMappedMemory();
  }
  return bytes;
}

bytes_view FieldReader::Load(IndexInput& in, uint64_t offset, uint64_t size) {
  if (_base) {
    return {_base + offset, size};
  }
  const auto [it, inserted] = _copies.try_emplace(offset);
  auto& copy = it->second;
  if (inserted) {
    Account(size);
    copy.resize(size);
    in.ReadData(offset, copy.data(), size);
  }
  SDB_ASSERT(copy.size() == size);
  return copy;
}

bytes_view FieldReader::Copy(uint64_t offset, uint64_t size) {
  const auto [it, inserted] = _copies.try_emplace(offset);
  auto& copy = it->second;
  if (inserted) {
    Account(size);
    copy.resize(size);
    _pending.push_back({.offset = offset, .size = size, .data = copy.data()});
  }
  SDB_ASSERT(copy.size() == size);
  return copy;
}

const byte_type* FieldReader::CopyBlock(uint32_t block, uint64_t length) {
  const uint64_t offset = PageOffset(block);
  return Copy(offset, std::min(PageOffset(block + 1), length) - offset).data();
}

void FieldReader::FillCopies() {
  absl::c_sort(_pending, [](const PendingCopy& lhs, const PendingCopy& rhs) {
    return lhs.offset < rhs.offset;
  });
  for (size_t i = 0; i != _pending.size();) {
    const uint64_t begin = _pending[i].offset;
    uint64_t end = begin + _pending[i].size;
    while (++i != _pending.size() && _pending[i].offset <= end) {
      end = std::max(end, _pending[i].offset + _pending[i].size);
    }
    _in->Prefetch(begin, end - begin);
  }
  for (const auto& copy : _pending) {
    _in->ReadData(copy.offset, copy.data, copy.size);
  }
  _pending.clear();
  _pending.shrink_to_fit();
}

Separators FieldReader::Bind(TermsMeta& terms) {
  _block_count = terms.blocks;
  _first_page = terms.first_page;
  _inline_offset = terms.inline_offset;
  _inline_size = terms.inline_size;
  _ranges_size = terms.ranges_size;
  const auto blocks = _block_count;
  if (terms.spans.size() > blocks) {
    Corrupted("block directory");
  }
  _spans.reserve(terms.spans.size());
  uint64_t extra = 0;
  for (const auto& span : terms.spans) {
    extra += span.extra;
    if (span.extra == 0 || span.block >= blocks ||
        (!_spans.empty() && span.block <= _spans.back().block) ||
        extra > std::numeric_limits<uint32_t>::max()) {
      Corrupted("block directory");
    }
    _spans.push_back(
      {.block = span.block, .extra = static_cast<uint32_t>(extra)});
  }
  if (blocks != 0 && (_first_page > _inline_offset / kPage ||
                      PageOffset(blocks - 1) >= _inline_offset)) {
    Corrupted("block directory");
  }

  Separators decoded;
  auto& separators = terms.separators;
  if (blocks != 0) {
    const auto size = separators.size();
    separators.append(kSlack, byte_type{0});
    decoded = DecodeSeparators({separators.data(), size}, blocks - 1);
  } else if (!separators.empty()) {
    Corrupted("separators");
  }
  if (const auto& table = terms.fsst_table; !table.empty()) {
    auto decoder = std::make_unique<duckdb_fsst_decoder_t>();
    const auto consumed =
      duckdb_fsst_import(decoder.get(), table.data(), table.size());
    if (consumed != table.size()) {
      Corrupted("FSST table");
    }
    _fsst = std::move(decoder);
  }
  if (terms.inline_starts.empty() != (_inline_size == 0)) {
    Corrupted("inline starts");
  }
  if (_inline_size != 0) {
    _inline_pages =
      InlinePages{terms.inline_starts, blocks, _inline_offset, _inline_size};
  }
  Account(_spans.capacity() * sizeof(SpanningBlock) +
          _inline_pages.MemoryBytes() +
          (_fsst ? sizeof(duckdb_fsst_decoder_t) : 0));
  return decoded;
}

void FieldReader::prepare(const ReaderState& state) {
  SDB_ASSERT(state.idx);
  auto terms = state.idx->TakeTerms();
  _in = state.idx->ReopenIn();
  if (!_in) {
    SDB_ENSURE(terms.fields.empty(),
               "term dictionary: fields but no `.idx` stream");
    return;
  }

  _base = _in->ReadStable(0, _in->Length());
  const auto& heads = terms.fields;
  Separators separators;
  if (!heads.empty()) {
    separators = Bind(terms);
  }

  IndexFeatures features = IndexFeatures::None;
  for (const auto& head : heads) {
    features |= head.features;
  }
  _pr.prepare(state, features);

  _keys.reserve(heads.size());
  _by_id.reserve(heads.size());
  for (size_t i = 0; i != heads.size(); ++i) {
    const auto& head = heads[i];
    if (i != 0 && (head.key <= heads[i - 1].key ||
                   head.ordinal <= heads[i - 1].ordinal)) {
      Corrupted("field order");
    }
    const auto& field = _fields.emplace_back(*this, head, *_in, separators);
    _by_id[head.key.id].dicts[static_cast<size_t>(head.key.dict)] = &field;
    if (_ids.empty() || _ids.back() != head.key.id) {
      _ids.push_back(head.key.id);
    }
    _keys.push_back(head.key);
  }
  FillCopies();
}

const TermReader* FieldReader::field(FieldKey key) const {
  const auto it = _by_id.find(key.id);
  return it == _by_id.end() ? nullptr
                            : it->second.dicts[static_cast<size_t>(key.dict)];
}

}  // namespace irs::term_dict
