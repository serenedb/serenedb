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

#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::term_dict {

FieldReader::~FieldReader() {
  if (_reserved != 0) {
    _buffers->FreeReservedMemory(_reserved);
  }
}

bytes_view FieldReader::Load(uint64_t offset, uint64_t size) {
  if (_base) {
    return {_base + offset, size};
  }
  const auto [it, inserted] = _copies.try_emplace(offset);
  auto& copy = it->second;
  if (inserted) {
    copy = _buffers->GetBufferAllocator().Allocate(size);
    IndexInput{*_in}.ReadData(offset, copy.get(), size);
  }
  SDB_ASSERT(copy.GetSize() == size);
  return {copy.get(), size};
}

bytes_view FieldReader::Copy(uint64_t offset, uint64_t size) {
  const auto [it, inserted] = _copies.try_emplace(offset);
  auto& copy = it->second;
  if (inserted) {
    copy = _buffers->GetBufferAllocator().Allocate(size);
    _pending.push_back({.offset = offset, .size = size, .data = copy.get()});
  }
  return {copy.get(), size};
}

uint64_t FieldReader::DecodedBytes() const noexcept {
  uint64_t bytes = _spans.capacity() * sizeof(SpanningBlock) +
                   SeparatorBytes(_separators) + _inline_pages.MemoryBytes() +
                   _single_inline.capacity() +
                   (_fsst ? sizeof(duckdb_fsst_decoder_t) : 0);
  for (const auto& reader : _keyed) {
    bytes += reader.MemoryBytes();
  }
  return bytes;
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
  IndexInput in{*_in};
  for (const auto& copy : _pending) {
    in.ReadData(copy.offset, copy.data, copy.size);
  }
  _pending.clear();
  _pending.shrink_to_fit();
}

void FieldReader::Bind(const TermsMeta& terms) {
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

  const auto separators =
    ViewCast<byte_type>(std::string_view{terms.separators});
  if (blocks != 0) {
    _separators = DecodeSeparators(separators, blocks - 1);
  } else if (!separators.empty()) {
    Corrupted("separators");
  }
  if (const auto& table = terms.fsst_table; !table.empty()) {
    auto decoder = std::make_unique<duckdb_fsst_decoder_t>();
    const auto consumed = duckdb_fsst_import(
      decoder.get(), reinterpret_cast<const unsigned char*>(table.data()),
      table.size());
    if (consumed != table.size()) {
      Corrupted("FSST table");
    }
    _fsst = std::move(decoder);
  }
  if (terms.inline_starts.empty() != (_inline_size == 0)) {
    Corrupted("inline starts");
  }
  if (_inline_size != 0) {
    if (InlineEnd() > _in->Length() || _in->Length() - InlineEnd() < kSlack) {
      Corrupted("inline postings");
    }
    _inline_pages =
      InlinePages{ViewCast<byte_type>(std::string_view{terms.inline_starts}),
                  blocks, _inline_offset, _inline_size};
    _inline_region = _base ? _base + _inline_offset
                           : Copy(_inline_offset, _inline_size + kSlack).data();
  }
}

void FieldReader::prepare(const ReaderState& state) {
  SDB_ASSERT(state.idx);
  auto terms = state.idx->TakeTerms();
  _in = state.idx->File();
  if (!_in) {
    SDB_ENSURE(terms.fields.empty(),
               "term dictionary: fields but no `.idx` stream");
    return;
  }

  _base = _in->Span().data();
  _buffers = &duckdb::BufferManager::GetBufferManager(state.db);
  const auto& heads = terms.fields;
  if (!heads.empty()) {
    Bind(terms);
  }

  IndexFeatures features = IndexFeatures::None;
  for (const auto& head : heads) {
    features |= head.features;
  }
  _pr.prepare(state, features);

  uint64_t single_inline = 0;
  for (const auto& head : heads) {
    single_inline += head.record_inline.size();
  }
  if (single_inline != 0) {
    _single_inline.reserve(single_inline + kSlack);
  }
  _by_id.reserve(heads.size());
  _dicts.reserve(heads.size());
  bstring prefix;
  uint32_t ordinal = 0;
  for (const auto& head : heads) {
    if (head.term_count == 1) {
      _dicts.push_back(&_singles.emplace_back(*this, head));
    } else {
      prefix.clear();
      AppendOrdinal(prefix, ordinal++, terms.ordinal_bytes);
      _dicts.push_back(&_keyed.emplace_back(*this, head, prefix, _separators));
    }
    ++_by_id
        .try_emplace(
          head.id,
          FieldDicts{.first = static_cast<uint32_t>(_dicts.size() - 1)})
        .first->second.count;
    if (_ids.empty() || _ids.back() != head.id) {
      _ids.push_back(head.id);
    }
  }
  if (single_inline != 0) {
    _single_inline.resize(_single_inline.size() + kSlack);
  }
  FillCopies();
  if (!_base && _block_count != 0) {
    _in->Prefetch(PageOffset(0), PageOffset(_block_count) - PageOffset(0));
  }
  const auto decoded = DecodedBytes();
  _buffers->ReserveMemory(decoded);
  _reserved = decoded;
}

const byte_type* FieldReader::KeepInline(bytes_view bytes) {
  SDB_ASSERT(_single_inline.size() + bytes.size() + kSlack <=
             _single_inline.capacity());
  const auto at = _single_inline.size();
  _single_inline.insert(_single_inline.end(), bytes.begin(), bytes.end());
  return _single_inline.data() + at;
}

std::span<const TermReader* const> FieldReader::fields(field_id id) const {
  const auto it = _by_id.find(id);
  if (it == _by_id.end()) {
    return {};
  }
  return std::span{_dicts}.subspan(it->second.first, it->second.count);
}

}  // namespace irs::term_dict
