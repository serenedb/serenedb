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
  const auto rest =
    absl::c_find_if(ranges, [&](const duckdb::FileRange& range) {
      return range.offset + range.size > at;
    });
  ranges.erase(ranges.begin(), rest);
  if (!ranges.empty() && ranges.front().offset < at) {
    auto& first = ranges.front();
    first.size -= at - first.offset;
    first.offset = at;
  }
}

uint32_t BlocksOf(const BlockStore& store, const FieldHead& head) {
  if (head.last_block < head.first_block ||
      head.last_block >= store.BlockCount()) {
    Corrupted("record", head.id);
  }
  return head.last_block - head.first_block + 1;
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

  SeekResult seek_ge(bytes_view term) final { return Seek(term); }

  bool seek(bytes_view term) final { return Seek(term) == SeekResult::Found; }

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
  SeekResult Seek(bytes_view term) {
    const auto result = _cursor.SeekGe(term);
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
                               bytes_view prefix, const Separators& separators,
                               duckdb::ReadStream& in,
                               std::span<const TermReaderImpl> levels)
  : _owner{&owner},
    _meta{head.id, head.features},
    _terms{head.term_count},
    _docs{head.doc_count},
    _index{head, prefix, BlocksOf(owner.Store(), head), separators, in},
    _has_score_bounds{head.text.has_score_bounds},
    _range_levels{levels} {
  _meta.norm = head.text.norm;
  _meta.dict = {.layout = head.layout, .member = head.member};
  SDB_ENSURE(head.text.total_freq <= std::numeric_limits<uint32_t>::max(),
             "term dictionary: total frequency ", head.text.total_freq,
             " exceeds uint32_t::max");
  _freq.value = static_cast<uint32_t>(head.text.total_freq);

  auto& store = owner.Store();
  const auto corrupted = [&] { Corrupted("record", head.id); };
  SDB_ASSERT(head.term_count > 1);
  if (head.width > kMaxStride) {
    corrupted();
  }
  key_width = head.width;
  single_valued = head.single_valued;
  _first_block = head.first_block;
  _first_entry = head.first_entry;
  _first_record = head.first_record;
  _last_entry = head.last_entry;
  if (const auto& table = head.fsst_table; !table.empty()) {
    _fsst = MakeTrackedArray<duckdb_fsst_decoder_t>(1);
    const auto consumed = duckdb_fsst_import(
      _fsst.get(), reinterpret_cast<const unsigned char*>(table.data()),
      table.size());
    if (consumed != table.size()) {
      Corrupted("FSST table", head.id);
    }
  }
  if (Small()) {
    _small = store.CopyBlock(_first_block);
  }
}

void TermReaderImpl::SampleTerms(
  uint64_t bytes,
  absl::FunctionRef<void(std::span<const bytes_view>)> visit) const {
  constexpr size_t kRun = 16;
  constexpr uint64_t kRunBytes = 256;
  const uint64_t runs = std::max<uint64_t>(1, bytes / kRunBytes);
  const uint32_t step =
    std::max<uint32_t>(1, static_cast<uint32_t>(Blocks() / runs));
  Cursor cursor{*this};
  std::array<bstring, kRun> keys;
  std::array<bytes_view, kRun> run;
  uint64_t taken = 0;
  for (uint32_t block = 0; block < Blocks() && taken < bytes; block += step) {
    cursor.Start(block);
    size_t n = 0;
    for (;;) {
      keys[n].assign(cursor.Term());
      run[n] = keys[n];
      taken += keys[n].size();
      if (++n == kRun || cursor.AtBlockEnd()) {
        break;
      }
      cursor.Advance();
    }
    visit({run.data(), n});
  }
}

void TermReaderImpl::HintBlock(uint32_t block) const noexcept {
  if (Small() || !Store().Mapped()) {
    return;
  }
  const uint64_t offset = BlockOffset(block);
  const uint64_t size = BlockOffset(block + 1) - offset;
  if (size <= kPage) {
    return;
  }
  const auto& in = Store().Input();
  if (in.Warm(offset, size) == size && !duckdb::PageResidency::Poll(kPage)) {
    return;
  }
  in.Prefetch(offset, size);
  in.MarkWarm(offset, size);
}

bool TermReaderImpl::Prefetch(bytes_view term) const {
  if (Small() || Excludes(term)) {
    return false;
  }
  PaddedKey padded;
  const uint32_t block = _index.BlockOf(padded.Assign(KeyPrefix(), term));
  const uint64_t offset = BlockOffset(block);
  return Store().Input().Hint(offset, BlockOffset(block + 1) - offset);
}

const byte_type* TermReaderImpl::PinBlock(size_t block,
                                          PinnedRange& pinned) const {
  const auto& in = Store().Input();
  const uint64_t offset = BlockOffset(block);
  const uint64_t end = BlockOffset(block + 1);
  if (offset < pinned.start || end - pinned.start > pinned.pinned.Size()) {
    const uint64_t cached = in.BlockSize();
    const uint64_t first = offset - offset % cached;
    const uint64_t last = std::min(first + cached, in.GetSize());
    pinned.start = end <= last ? first : offset;
    pinned.pinned = in.Pin(pinned.start, std::max(end, last) - pinned.start);
  }
  return pinned.pinned.Ptr() + (offset - pinned.start);
}

SeekTermIterator::ptr TermReaderImpl::iterator() const {
  return memory::make_managed<TermIteratorImpl>(*this);
}

SeekTermIterator::ptr TermReaderImpl::IteratorUntil(bytes_view upper) const {
  auto it = memory::make_managed<TermIteratorImpl>(*this);
  if (!upper.empty() && !Small()) {
    it->ReadaheadUntil(_index.BlockEnd(upper));
  }
  return it;
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
  const uint32_t block = _index.BlockOf(probe);
  HintBlock(block);
  PinnedRange pinned;
  const auto view = Parse(Block(block, pinned), Fsst());
  uint32_t from = 0;
  const auto entry = Find(view, probe, Fsst(), from);
  if (entry == kNoEntry) {
    return {};
  }
  RecordCursor records{meta().index_features};
  return records.Read(*this, view, block, entry);
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
  containers::SmallVector<duckdb::FileRange, kBatchTerms> blocks;
  const auto prefix = KeyPrefix();
  uint32_t last = kNoBlock;
  size_t visited = 0;
  bytes_view last_term;
  bool below_min = true;
  for (size_t i = 0; i != count; ++i) {
    const auto value = term(i);
    SDB_ASSERT(i == 0 || last_term <= value);
    last_term = value;
    if (below_min) {
      if (value < min()) {
        continue;
      }
      below_min = false;
    }
    if (max() < value) {
      break;
    }
    if (key_width != 0 && value.size() != key_width) {
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
    const Probe probe{key, size};
    const uint32_t block =
      last == kNoBlock ? _index.BlockOf(probe) : _index.BlockFrom(last, probe);
    if (!Small() && block != last) {
      ++visited;
      const uint64_t offset = BlockOffset(block);
      blocks.push_back(
        {.offset = offset, .size = BlockOffset(block + 1) - offset});
    }
    probes.push_back({
      .index = static_cast<uint32_t>(i),
      .block = block,
      .key = static_cast<uint32_t>(at),
      .size = static_cast<uint32_t>(size),
    });
    last = block;
  }
  const auto& in = Store().Input();
  const bool hinted = visited > 1;
  if (hinted) {
    in.Hint(blocks);
  }
  auto epoch = duckdb::PageResidency::Epoch();

  PinnedRange pinned;
  BlockView view{};
  RecordCursor records{meta().index_features};
  uint32_t parsed = kNoBlock;
  uint32_t from = 0;
  for (const auto& probe : probes) {
    if (probe.block != parsed) {
      parsed = probe.block;
      from = 0;
      const uint64_t offset = BlockOffset(parsed);
      if (hinted &&
          in.Evicted(offset, BlockOffset(parsed + 1) - offset, epoch)) {
        DropBefore(blocks, offset);
        in.Hint(blocks);
      }
      if (!hinted) {
        HintBlock(parsed);
      }
      view = Parse(Block(parsed, pinned), Fsst());
      records.Reset();
    }
    const auto entry =
      Find(view, Probe{keys.data() + probe.key, probe.size}, Fsst(), from);
    if (entry != kNoEntry) {
      found(probe.index, records.Holds(entry)
                           ? records.Meta()
                           : records.Read(*this, view, parsed, entry));
    }
  }
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
