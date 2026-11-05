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

#include "iresearch/formats/index/term_dict_reader.hpp"

#include <absl/algorithm/container.h>

#include <array>
#include <cstring>
#include <limits>
#include <span>
#include <string_view>
#include <utility>

#include "iresearch/formats/index/field_reader.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/small_vector.hpp"

namespace irs::term_dict {
namespace {

constexpr size_t kBatchTerms = 64;
constexpr size_t kBatchKeyBytes = 4096;

template<typename Ranges>
void DropBefore(Ranges& ranges, uint64_t at) {
  const auto rest = absl::c_find_if(
    ranges, [&](const FileRange& range) { return range.second > at; });
  ranges.erase(ranges.begin(), rest);
  if (!ranges.empty()) {
    ranges.front().first = std::max(ranges.front().first, at);
  }
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

PostingMeta DecodeRecord(const TermReaderImpl& field, const BlockView& view,
                         uint32_t block, uint32_t entry) {
  uint32_t count = 0;
  auto cursor = FieldRecords(field, view, block, entry, count);
  PostingMeta meta;
  uint64_t inline_end = 0;
  DecodeRecords(cursor, count, field.meta().index_features, meta, inline_end);
  if (meta.inline_size != 0) {
    meta.inline_data = field.ResolveInline(inline_end, meta.inline_size);
  }
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

TermReaderImpl::TermReaderImpl(FieldReader& owner, const FieldHead& head,
                               bytes_view prefix, const InputFile& in,
                               const Separators& separators)
  : _owner{&owner},
    _meta{head.id, head.features},
    _terms{head.term_count},
    _docs{head.doc_count},
    _has_score_bounds{head.text.has_score_bounds} {
  _meta.norm = head.text.norm;
  _meta.dict = {.layout = head.layout, .member = head.member};
  SDB_ENSURE(head.text.total_freq <= std::numeric_limits<uint32_t>::max(),
             "term dictionary: total frequency ", head.text.total_freq,
             " exceeds uint32_t::max");
  _freq.value = static_cast<uint32_t>(head.text.total_freq);

  const auto corrupted = [&] { Corrupted("record", head.id); };
  SDB_ASSERT(head.term_count > 1);
  if (head.width > kMaxStride ||
      (!head.text.prefixes.empty() && head.width != 0)) {
    corrupted();
  }
  _prefix_size = static_cast<uint32_t>(prefix.size());
  key_width = head.width;
  uint64_t bytes = 0;
  _min_key.assign(prefix);
  _min_key.append(ViewCast<byte_type>(std::string_view{head.min}));
  _max_key.assign(prefix);
  _max_key.append(ViewCast<byte_type>(std::string_view{head.max}));
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
    _root = Root{Slice(separators, _first_block, _blocks - 1), key_width != 0};
    bytes += _root.MemoryBytes();
    if (!head.text.prefixes.empty()) {
      _prefixes = std::make_unique<PrefixIndex>(DecodePrefixes(
        ViewCast<byte_type>(std::string_view{head.text.prefixes}), head.id));
      bytes += _prefixes->MemoryBytes();
    }
  } else if (!head.text.prefixes.empty()) {
    corrupted();
  } else {
    _inline_base = head.inline_base;
    inline_size = head.inline_size;
    if (_inline_base > owner.InlineEnd() - owner.InlineBegin() ||
        inline_size > owner.InlineEnd() - owner.InlineBegin() - _inline_base) {
      corrupted();
    }
  }
  if (!head.ranges.counts.empty()) {
    bytes += ReadRanges(owner, in, head.ranges);
  } else if (head.ranges.size != 0) {
    corrupted();
  }
  if (_blocks == 1) {
    _small = owner.CopyBlock(_first_block, in.Length());
    if (inline_size != 0) {
      _small_inline = {owner.InlineRegion() + _inline_base, inline_size};
      if (owner.Base()) {
        const auto copy =
          owner.Copy(owner.InlineBegin() + _inline_base, inline_size + kSlack);
        _small_inline = {copy.data(),
                         std::min(inline_size, copy.size() - kSlack)};
      }
    }
  }
  owner.Account(bytes);
}

uint64_t TermReaderImpl::ReadRanges(FieldReader& owner, const InputFile& in,
                                    const RangesHead& ranges) {
  const auto corrupted = [&] { Corrupted("range levels", _meta.id); };
  const auto offset = ranges.offset;
  const auto size = ranges.size;
  const auto height = ranges.counts.size();
  _range_prefix = ranges.prefix;
  if (size == 0 || offset > owner.RangesSize() ||
      size > owner.RangesSize() - offset || height > kMaxRangeLevels ||
      key_width == 0 || _range_prefix > key_width ||
      min().substr(0, _range_prefix) != max().substr(0, _range_prefix)) {
    corrupted();
  }
  const auto* p = owner.Load(owner.RangesBegin() + offset, size).data();
  const auto* const end = p + size;
  const uint64_t stride = key_width - _range_prefix;
  _levels.reserve(height);
  for (size_t k = 0; k != height; ++k) {
    const auto count = ranges.counts[k];
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

void TermReaderImpl::SampleTerms(
  uint64_t bytes,
  absl::FunctionRef<void(std::span<const bytes_view>)> visit) const {
  constexpr size_t kRun = 16;
  constexpr uint64_t kRunBytes = 256;
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
  const auto& in = _owner->Input();
  buffer.pinned =
    in.Pin(offset, std::min(BlockOffset(block + 1), in.Length()) - offset);
  return buffer.pinned.bytes.data();
}

void TermReaderImpl::PrefetchLookup(uint32_t block) const {
  if (Small() || !_owner->Base()) {
    return;
  }
  if (const auto span = InlineSpan(block); span.from != span.to) {
    HintBlock(block);
  }
}

bool TermReaderImpl::HintBlock(uint32_t block) const {
  const auto& in = _owner->Input();
  const uint64_t offset = BlockOffset(block);
  bool hinted =
    in.Hint(offset, std::min(BlockOffset(block + 1), in.Length()) - offset);
  if (const auto span = InlineSpan(block); span.from != span.to) {
    hinted |= in.Hint(span.from, span.to - span.from);
  }
  return hinted;
}

SeekTermIterator::ptr TermReaderImpl::iterator() const {
  return memory::make_managed<TermIteratorImpl>(*this);
}

SeekTermIterator::ptr TermReaderImpl::IteratorUntil(bytes_view upper) const {
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
  return BlockOf(key.Assign(KeyPrefix(), upper)) + 1;
}

bool TermReaderImpl::Excludes(bytes_view term) const noexcept {
  return term < min() || max() < term ||
         (key_width != 0 && term.size() != key_width);
}

PostingMeta TermReaderImpl::Lookup(bytes_view term) const {
  if (Excludes(term)) {
    return {};
  }
  PaddedKey padded;
  const auto probe = padded.Assign(KeyPrefix(), term);
  const uint32_t block = BlockOf(probe);
  PrefetchLookup(block);
  BlockBuffer buffer;
  const auto view = Parse(Block(block, buffer), Fsst());
  const auto entry = Find(view, probe, Fsst());
  if (entry == kNoEntry) {
    return {};
  }
  return DecodeRecord(*this, view, block, entry);
}

void TermReaderImpl::LookupBatch(size_t count, TermAt term,
                                 FoundTerm found) const {
  struct Probed {
    uint32_t index;
    uint32_t block;
    uint32_t key;
    uint32_t size;
  };
  containers::SmallVector<Probed, kBatchTerms> probes;
  containers::SmallVector<byte_type, kBatchKeyBytes> keys;
  containers::SmallVector<FileRange, kBatchTerms> blocks;
  containers::SmallVector<FileRange, kBatchTerms> inlines;
  const auto prefix = KeyPrefix();
  const bool mapped = _owner->Base() != nullptr;
  uint32_t last = kNoBlock;
  size_t visited = 0;
  bytes_view last_term;
  for (size_t i = 0; i != count; ++i) {
    const auto value = term(i);
    SDB_ASSERT(probes.empty() || last_term <= value);
    if (Excludes(value)) {
      continue;
    }
    const auto at = keys.size();
    const auto size = prefix.size() + value.size();
    keys.resize(at + size + kKeyPad, boost::container::default_init);
    auto* key = keys.data() + at;
    std::memcpy(key, prefix.data(), prefix.size());
    if (!value.empty()) {
      std::memcpy(key + prefix.size(), value.data(), value.size());
    }
    std::memset(key + size, 0, kKeyPad);
    uint32_t block = 0;
    if (!Small()) {
      const Probe probe{key, size};
      block = last == kNoBlock ? BlockOf(probe) : BlockFrom(last, probe);
      if (block != last) {
        ++visited;
        AppendRange(blocks, BlockOffset(block), BlockOffset(block + 1));
        if (mapped) {
          if (const auto span = InlineSpan(block); span.from != span.to) {
            AppendRange(inlines, span.from, span.to);
          }
        }
      }
    }
    probes.push_back({
      .index = static_cast<uint32_t>(i),
      .block = block,
      .key = static_cast<uint32_t>(at),
      .size = static_cast<uint32_t>(size),
    });
    last = block;
    last_term = value;
  }
  const auto& in = _owner->Input();
  const bool hinted = visited > 1 || !inlines.empty();
  if (hinted) {
    const auto budget = HintRanges(in, blocks);
    HintRanges(in, inlines, budget);
  }
  auto epoch = file_utils::ResidencyEpoch();

  BlockBuffer buffer;
  BlockView view{};
  uint32_t parsed = kNoBlock;
  for (const auto& probe : probes) {
    if (probe.block != parsed) {
      parsed = probe.block;
      const uint64_t offset = BlockOffset(parsed);
      if (hinted &&
          in.Evicted(offset, BlockOffset(parsed + 1) - offset, epoch)) {
        DropBefore(blocks, offset);
        if (!inlines.empty()) {
          DropBefore(inlines, InlineSpan(parsed).from);
        }
        const auto budget = HintRanges(in, blocks);
        HintRanges(in, inlines, budget);
      }
      view = Parse(Block(parsed, buffer), Fsst());
    }
    const auto entry =
      Find(view, Probe{keys.data() + probe.key, probe.size}, Fsst());
    if (entry != kNoEntry) {
      found(probe.index, DecodeRecord(*this, view, parsed, entry));
    }
  }
}

bool TermReaderImpl::Prefetch(bytes_view term) const {
  if (Excludes(term)) {
    return false;
  }
  PaddedKey padded;
  return HintBlock(BlockOf(padded.Assign(KeyPrefix(), term)));
}

PostingsHandles TermReaderImpl::Handles() const noexcept {
  return _owner->Postings().Handles();
}

Attribute* TermReaderImpl::GetMutable(TypeInfo::type_id type) noexcept {
  return type == irs::Type<FreqAttr>::id() &&
             _meta.dict.member == DictMember::Strings
           ? &_freq
           : nullptr;
}

}  // namespace irs::term_dict
