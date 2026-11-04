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

#include "iresearch/formats/index/block_writer.hpp"

#include <algorithm>
#include <limits>
#include <span>

#include "iresearch/formats/index/term_dict_root.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/string_utils.hpp"

namespace irs::term_dict {
namespace {

constexpr byte_type kZeros[kPage]{};

uint64_t VSize(uint64_t value) noexcept {
  return bytes_io<uint64_t, sizeof(uint64_t)>::vsize(value);
}

uint64_t VarBytes(uint64_t count, uint64_t head_skip, uint32_t max,
                  uint64_t suffixes, uint64_t records) noexcept {
  const uint64_t restarts = (count + kRestartInterval - 1) / kRestartInterval;
  return 2 + VSize(count) + VSize(restarts) + VSize(suffixes) +
         VSize(head_skip) + (kVarRestart + kHeadBytes) * restarts +
         kRecordOffset * RecordGroups(count) +
         count * (2 * uint64_t{ColumnWidth(max)} + 2) + suffixes + records +
         kSlack;
}

uint64_t FixedBytes(uint64_t count, uint64_t records, uint64_t width,
                    uint64_t prefix) noexcept {
  const uint64_t stride = width - prefix;
  const uint64_t groups = RecordGroups(count);
  return 1 + VSize(count) + VSize(groups) + VSize(stride) + VSize(prefix) +
         prefix + (kRecordOffset + kHeadBytes) * groups + count * stride +
         records + kSlack;
}

bytes_view KeyAt(const BlockBuilder& block, size_t i) noexcept {
  const uint32_t begin = i == 0 ? 0 : block.ends[i - 1];
  return {block.keys.data() + begin, block.ends[i] - begin};
}

struct VarEntry {
  bytes_view suffix;
  size_t lcp;
  uint32_t head_skip;
  bool restart;
};

VarEntry VarEntryAt(const BlockBuilder& block, uint32_t index,
                    bytes_view key) noexcept {
  const bool restart = index % kRestartInterval == 0;
  const size_t lcp =
    restart ? 0 : CommonPrefixLength(KeyAt(block, index - 1), key);
  uint32_t head_skip = block.head_skip;
  if (restart) {
    head_skip = static_cast<uint32_t>(
      index == 0 ? key.size() : CommonPrefixLength(KeyAt(block, 0), key));
  }
  return {
    .suffix = key.substr(lcp),
    .lcp = lcp,
    .head_skip = head_skip,
    .restart = restart,
  };
}

void Clear(VarColumns& columns) noexcept {
  columns.lcps.clear();
  columns.lens.clear();
  columns.suffixes.clear();
  columns.restarts.clear();
  columns.max = 0;
}

void Clear(BlockBuilder& block) noexcept {
  block.keys.clear();
  block.ends.clear();
  block.records.clear();
  block.record_starts.clear();
  Clear(block.raw);
  Clear(block.coded);
  block.size = 0;
  block.coded_size = std::numeric_limits<uint64_t>::max();
  block.width = 0;
  block.prefix = 0;
  block.head_skip = 0;
  block.kind = BlockKind::Var;
  block.all_coded = true;
}

uint32_t ColumnMax(const VarColumns& columns, size_t lcp, size_t len) noexcept {
  return std::max(
    {columns.max, static_cast<uint32_t>(lcp), static_cast<uint32_t>(len)});
}

void AppendEntry(VarColumns& columns, bool restart, size_t lcp,
                 bytes_view suffix) {
  if (restart) {
    columns.restarts.push_back(static_cast<uint16_t>(columns.suffixes.size()));
  }
  columns.max = ColumnMax(columns, lcp, suffix.size());
  columns.lcps.push_back(static_cast<uint32_t>(lcp));
  columns.lens.push_back(static_cast<uint32_t>(suffix.size()));
  columns.suffixes.append(suffix);
}

void WriteColumn(DataOutput& out, std::span<const uint32_t> values,
                 uint32_t width) {
  for (const auto value : values) {
    switch (width) {
      case 1:
        out.WriteByte(static_cast<byte_type>(value));
        break;
      case 2:
        out.WriteU16(static_cast<uint16_t>(value));
        break;
      default:
        out.WriteU32(value);
        break;
    }
  }
}

void WriteHeads(DataOutput& out, const BlockBuilder& block, size_t step,
                size_t skip) {
  for (size_t i = 0; i < block.ends.size(); i += step) {
    out.WriteU32(static_cast<uint32_t>(HeadAt(KeyAt(block, i), skip) >> 32));
  }
}

uint64_t SerializedSize(const BlockBuilder& block) noexcept {
  return block.kind == BlockKind::VarFsst ? block.coded_size : block.size;
}

void Serialize(const BlockBuilder& block, DataOutput& out) {
  const auto count = static_cast<uint32_t>(block.ends.size());
  SDB_ASSERT(block.record_starts.size() == RecordGroups(count));
  out.WriteByte(static_cast<byte_type>(block.kind));
  out.WriteV32(count);
  const bool coded = block.kind == BlockKind::VarFsst;
  if (block.kind != BlockKind::FixedStride) {
    const auto& columns = coded ? block.coded : block.raw;
    const uint32_t width = ColumnWidth(columns.max);
    out.WriteV32(static_cast<uint32_t>(columns.restarts.size()));
    out.WriteByte(static_cast<byte_type>(width));
    out.WriteV64(columns.suffixes.size());
    out.WriteV32(block.head_skip);
    for (const auto restart : columns.restarts) {
      out.WriteU16(restart);
    }
    WriteHeads(out, block, kRestartInterval, block.head_skip);
    for (const auto start : block.record_starts) {
      out.WriteU16(start);
    }
    WriteColumn(out, columns.lcps, width);
    WriteColumn(out, columns.lens, width);
    for (uint32_t i = 0; i != count; ++i) {
      const auto key = KeyAt(block, i);
      out.WriteByte(key.empty() ? 0 : key.back());
    }
    for (uint32_t i = 0; i != count; ++i) {
      const auto lcp = columns.lcps[i];
      out.WriteByte(lcp == 0 ? 0 : KeyAt(block, i)[lcp - 1]);
    }
    out.WriteData(columns.suffixes.data(), columns.suffixes.size());
  } else {
    const uint32_t stride = block.width - block.prefix;
    out.WriteV32(static_cast<uint32_t>(block.record_starts.size()));
    out.WriteV32(stride);
    out.WriteV32(block.prefix);
    out.WriteData(block.keys.data(), block.prefix);
    for (const auto start : block.record_starts) {
      out.WriteU16(start);
    }
    WriteHeads(out, block, kRecordInterval, block.prefix);
    for (size_t i = 0; i != count; ++i) {
      out.WriteData(KeyAt(block, i).data() + block.prefix, stride);
    }
  }
  out.WriteData(block.records.data(), block.records.size());
  out.WriteData(kZeros, kSlack);
}

}  // namespace

BlockWriter::BlockWriter(uint64_t inline_base) noexcept
  : _out{&_pages_out}, _inline_base{inline_base} {}

void BlockWriter::Open(DataOutput& out, uint64_t position) noexcept {
  _out = &out;
  _position = position;
}

void BlockWriter::SetFsst(duckdb_fsst_encoder_t* fsst) {
  _fsst.reset(fsst ? duckdb_fsst_duplicate(fsst) : nullptr);
}

void BlockWriter::Recode() {
  auto& block = _block;
  if (!_fsst || block.kind != BlockKind::Var || block.ends.empty()) {
    return;
  }
  _encode = true;
  block.all_coded = true;
  Clear(block.raw);
  Clear(block.coded);
  const auto count = static_cast<uint32_t>(block.ends.size());
  for (uint32_t i = 0; i != count; ++i) {
    AppendVarColumns(i, KeyAt(block, i));
  }
  block.coded_size =
    VarBytes(count, block.head_skip, block.coded.max,
             block.coded.suffixes.size(), block.records.size());
}

void BlockWriter::BeginField(uint32_t width, bool encode,
                             IndexFeatures features) noexcept {
  _width = width;
  _encode = encode;
  _encoder.Reset(features);
  _group_inline = false;
}

bytes_view BlockWriter::EncodeSuffix(bytes_view suffix) {
  if (suffix.empty()) {
    return {};
  }
  size_t length = suffix.size();
  auto* input = const_cast<unsigned char*>(suffix.data());
  utils::StrResizeAmortized(_coded, 7 + 2 * length);
  size_t out_length = 0;
  unsigned char* out = nullptr;
  const size_t done =
    duckdb_fsst_compress(_fsst.get(), 1, &length, &input, _coded.size(),
                         _coded.data(), &out_length, &out);
  SDB_ASSERT(done == 1);
  return {out, out_length};
}

void BlockWriter::Add(bytes_view term, const PostingMeta& meta) {
  if (!Append(term, meta)) {
    Cut();
    Append(term, meta);
  }
  _inline.append(meta.Inline());
}

bool BlockWriter::Append(bytes_view term, const PostingMeta& meta) {
  auto& block = _block;
  const bool record_start = block.ends.size() % kRecordInterval == 0;
  if (record_start) {
    _encoder.BeginBlock();
    _group_inline = false;
  }
  _record.Clear();
  _encoder.Encode(_record, meta);
  if (meta.inline_size != 0 && !_group_inline) {
    _record.WriteV64(InlineEnd());
    _group_inline = true;
  }
  const auto record = _record.View();
  const uint64_t records = block.records.size() + record.size();
  const auto at = block.keys.size();
  block.keys.append(_key_prefix);
  block.keys.append(term);
  const auto key = bytes_view{block.keys}.substr(at);
  const bool fixed =
    _width != 0 &&
    (block.ends.empty() ||
     (block.kind == BlockKind::FixedStride && block.width == key.size()));
  if (!(fixed ? AppendFixedKey(key, records) : AppendVarKey(key, records))) {
    block.keys.resize(at);
    return false;
  }
  if (record_start) {
    block.record_starts.push_back(static_cast<uint16_t>(block.records.size()));
  }
  block.records.append(record);
  block.ends.push_back(static_cast<uint32_t>(block.keys.size()));
  return true;
}

bool BlockWriter::AppendFixedKey(bytes_view key, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  const auto width = static_cast<uint32_t>(key.size());
  const auto prefix =
    count == 0
      ? width
      : static_cast<uint32_t>(CommonPrefixLength(KeyAt(block, 0), key));
  const uint64_t size = FixedBytes(count + 1, records, width, prefix);
  if (count != 0 && size > kPage) {
    return false;
  }
  block.size = size;
  block.width = width;
  block.prefix = prefix;
  block.kind = BlockKind::FixedStride;
  return true;
}

void BlockWriter::AppendVarColumns(uint32_t index, bytes_view key) {
  auto& block = _block;
  const auto entry = VarEntryAt(block, index, key);
  block.head_skip = entry.head_skip;
  AppendEntry(block.raw, entry.restart, entry.lcp, entry.suffix);
  if (_fsst && _encode && block.all_coded) {
    AppendEntry(block.coded, entry.restart, entry.lcp,
                entry.restart ? entry.suffix : EncodeSuffix(entry.suffix));
  }
}

bool BlockWriter::AppendVarKey(bytes_view key, uint64_t records) {
  auto& block = _block;
  const auto count = static_cast<uint32_t>(block.ends.size());
  const bool converts = block.kind == BlockKind::FixedStride;
  if (converts) {
    for (uint32_t i = 0; i != count; ++i) {
      AppendVarColumns(i, KeyAt(block, i));
    }
  }
  const auto entry = VarEntryAt(block, count, key);
  const auto size_with = [&](const VarColumns& columns, size_t suffix_size) {
    return VarBytes(count + 1, entry.head_skip,
                    ColumnMax(columns, entry.lcp, suffix_size),
                    columns.suffixes.size() + suffix_size, records);
  };
  const uint64_t size = size_with(block.raw, entry.suffix.size());
  const bool encode = _fsst && _encode && block.all_coded;
  bytes_view coded;
  uint64_t coded_size = std::numeric_limits<uint64_t>::max();
  if (encode) {
    coded = entry.restart ? entry.suffix : EncodeSuffix(entry.suffix);
    coded_size = size_with(block.coded, coded.size());
  }
  if (count != 0 && std::min(size, coded_size) > kPage) {
    if (converts) {
      Clear(block.raw);
      Clear(block.coded);
      block.head_skip = 0;
    }
    return false;
  }
  block.kind = BlockKind::Var;
  AppendEntry(block.raw, entry.restart, entry.lcp, entry.suffix);
  if (encode) {
    AppendEntry(block.coded, entry.restart, entry.lcp, coded);
    block.coded_size = coded_size;
  } else if (block.all_coded) {
    block.all_coded = false;
    Clear(block.coded);
    block.coded_size = std::numeric_limits<uint64_t>::max();
  }
  block.size = size;
  block.head_skip = entry.head_skip;
  return true;
}

void BlockWriter::Seal() {
  if (_block.kind == BlockKind::Var && _block.coded_size < _block.size) {
    _block.kind = BlockKind::VarFsst;
  }
}

void BlockWriter::Cut() {
  if (_block.ends.empty()) {
    return;
  }
  Seal();
  WriteSeparator(KeyAt(_block, 0));
  WriteBlock();
  StringOutput{_inline_starts}.WriteV64(_inline_start - _inline_last);
  _inline_last = _inline_start;
  _inline_start = _inline.size();
  _last.assign(KeyAt(_block, _block.ends.size() - 1));
  Clear(_block);
}

void BlockWriter::Splice(BlockWriter& part, bytes_view first) {
  const uint64_t base = part._inline_base;
  SDB_ASSERT(_block.ends.empty() && InlineEnd() == base);
  if (part._blocks != 0) {
    WriteSeparator(first);
    PadToPage();
    SDB_ASSERT(part._pages.size() == part._position);
    _out->WriteData(part._pages.data(), part._pages.size());
    _position += part._pages.size();
    for (const auto& span : part._spans) {
      _spans.push_back({.block = _blocks + span.block, .extra = span.extra});
    }
    StringOutput{_inline_starts}.WriteV64(_inline_start - _inline_last);
    SDB_ASSERT(!part._inline_starts.empty() &&
               part._inline_starts.front() == 0);
    _inline_starts.append(part._inline_starts, 1);
    if (part._blocks > 1) {
      AppendSeparators(part._separators);
      _separator = std::move(part._separator);
    }
    _last = std::move(part._last);
    _inline_last = base + part._inline_last;
    _blocks += part._blocks;
  }
  _inline_start = base + part._inline_start;
  _inline.append(part._inline);
  _block = std::move(part._block);
}

void BlockWriter::Finish(TermsMeta& terms) {
  SDB_ASSERT(_block.ends.empty());
  terms.blocks = _blocks;
  terms.first_page = _first_page;
  terms.spans = std::move(_spans);
  terms.separators = std::move(_separators);
  terms.inline_offset = _position;
  terms.inline_size = _inline.size();
  if (!_inline.empty()) {
    terms.inline_starts = std::move(_inline_starts);
  }
  _out->WriteData(_inline.data(), _inline.size());
}

void BlockWriter::Reset() noexcept {
  Clear(_block);
  _fsst.reset();
  _key_prefix.clear();
  _inline.clear();
  _inline_starts.clear();
  _spans.clear();
  _separators.clear();
  _separator.clear();
  _last.clear();
  _pages.clear();
  _out = nullptr;
  _position = 0;
  _first_page = 0;
  _inline_start = 0;
  _inline_last = 0;
  _blocks = 0;
}

bytes_view BlockWriter::LastKey() const noexcept {
  return KeyAt(_block, _block.ends.size() - 1);
}

void BlockWriter::WriteSeparator(bytes_view first) {
  if (_blocks == 0) {
    return;
  }
  EncodeSeparator(
    first.substr(0, CommonPrefixLength(bytes_view{_last}, first) + 1));
}

void BlockWriter::EncodeSeparator(bytes_view separator) {
  const auto shared = CommonPrefixLength(bytes_view{_separator}, separator);
  StringOutput out{_separators};
  out.WriteV32(static_cast<uint32_t>(shared));
  WriteStr(out, separator.substr(shared));
  _separator.assign(separator);
}

void BlockWriter::AppendSeparators(std::string_view separators) {
  const auto encoded = ViewCast<byte_type>(separators);
  const auto* p = encoded.data();
  [[maybe_unused]] const auto shared = vread<uint32_t>(p);
  SDB_ASSERT(shared == 0);
  const auto size = vread<uint32_t>(p);
  EncodeSeparator({p, size});
  p += size;
  _separators.append(separators.substr(p - encoded.data()));
}

void BlockWriter::PadToPage() {
  const uint64_t pad = (kPage - _position % kPage) % kPage;
  _out->WriteData(kZeros, pad);
  _position += pad;
  if (_blocks == 0) {
    _first_page = _position / kPage;
  }
}

void BlockWriter::WriteBlock() {
  PadToPage();
  const uint64_t size = SerializedSize(_block);
  const auto pages = static_cast<uint32_t>((size + kPage - 1) / kPage);
  if (pages != 1) {
    _spans.push_back({.block = _blocks, .extra = pages - 1});
  }
  Serialize(_block, *_out);
  _position += size;
  ++_blocks;
}

}  // namespace irs::term_dict
