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
                               bytes_view prefix,
                               std::vector<const TermReaderImpl*> levels)
  : _owner{&owner},
    _meta{head.id, head.features},
    _terms{head.term_count},
    _docs{head.doc_count},
    _index{head, prefix, BlocksOf(owner.Store(), head)},
    _has_score_bounds{head.text.has_score_bounds},
    _range_levels{std::move(levels)} {
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
  uint64_t bytes = _index.MemoryBytes();
  _first_block = head.first_block;
  _first_entry = head.first_entry;
  _first_record = head.first_record;
  _last_entry = head.last_entry;
  bytes += _range_levels.capacity() * sizeof(const TermReaderImpl*);
  if (const auto& table = head.fsst_table; !table.empty()) {
    _fsst = std::make_unique<duckdb_fsst_decoder_t>();
    const auto consumed = duckdb_fsst_import(
      _fsst.get(), reinterpret_cast<const unsigned char*>(table.data()),
      table.size());
    if (consumed != table.size()) {
      Corrupted("FSST table", head.id);
    }
    bytes += sizeof(duckdb_fsst_decoder_t);
  }
  if (Small()) {
    _small = store.CopyBlock(_first_block);
  }
  store.Account(bytes);
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
  if (Small()) {
    return;
  }
  const uint64_t offset = BlockOffset(block);
  const uint64_t size = BlockOffset(block + 1) - offset;
  if (size <= kPage) {
    return;
  }
  const auto& in = Store().Input();
  if (in.Warm(offset, size) == size && !file_utils::PollResidency(kPage)) {
    return;
  }
  in.Prefetch(offset, size);
  in.MarkWarm(offset, size);
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
  const auto located =
    Small() ? Located{.block = 0, .absent = false} : _index.Locate(probe);
  if (located.absent && _index.Gated()) {
    return {};
  }
  for (uint32_t block = located.block;; ++block) {
    HintBlock(block);
    const auto view = Parse(Block(block), Fsst());
    uint32_t from = 0;
    const auto found = Find(view, probe, Fsst(), from);
    if (found.entry != kNoEntry) {
      RecordCursor records{meta().index_features};
      return records.Read(*this, view, block, found.entry);
    }
    if (!found.past || block + 1 == Blocks()) {
      return {};
    }
  }
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
    uint32_t block = 0;
    if (!Small()) {
      const auto located = _index.Locate(Probe{key, size});
      if (located.absent && _index.Gated()) {
        keys.resize(at);
        continue;
      }
      block = located.block;
      if (block != last) {
        ++visited;
        AppendRange(blocks, BlockOffset(block), BlockOffset(block + 1));
      }
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
    HintRanges(in, blocks);
  }
  auto epoch = file_utils::ResidencyEpoch();

  BlockView view{};
  RecordCursor records{meta().index_features};
  uint32_t parsed = kNoBlock;
  uint32_t from = 0;
  for (const auto& probe : probes) {
    for (uint32_t block = std::max(probe.block, parsed == kNoBlock ? 0 : parsed);
         ; ++block) {
      if (block != parsed) {
        parsed = block;
        from = 0;
        const uint64_t offset = BlockOffset(parsed);
        if (hinted && file_utils::Evicted(
                        in, offset, BlockOffset(parsed + 1) - offset, epoch)) {
          DropBefore(blocks, offset);
          HintRanges(in, blocks);
        }
        if (!hinted) {
          HintBlock(parsed);
        }
        view = Parse(Block(parsed), Fsst());
        records.Reset();
      }
      const auto at =
        Find(view, Probe{keys.data() + probe.key, probe.size}, Fsst(), from);
      if (at.entry != kNoEntry) {
        found(probe.index, records.Holds(at.entry)
                             ? records.Meta()
                             : records.Read(*this, view, parsed, at.entry));
        break;
      }
      if (!at.past || block + 1 == Blocks()) {
        break;
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
  return type == irs::Type<FreqAttr>::id() &&
             _meta.dict.member == DictMember::Strings
           ? &_freq
           : nullptr;
}

}  // namespace irs::term_dict
