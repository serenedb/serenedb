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

#include "iresearch/formats/index/term_dict.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#ifdef __BMI2__
#include <immintrin.h>
#endif

#include <array>
#include <bit>
#include <cstring>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <limits>
#include <string>
#include <string_view>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/index/term_dict_cursor.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::term_dict {
namespace {

constexpr duckdb::field_id_t kTermsRestartInterval = 0;
constexpr duckdb::field_id_t kTermsBlocks = 1;
constexpr duckdb::field_id_t kTermsFirstPage = 2;
constexpr duckdb::field_id_t kTermsSpans = 3;
constexpr duckdb::field_id_t kTermsSeparators = 4;
constexpr duckdb::field_id_t kTermsFsst = 5;
constexpr duckdb::field_id_t kTermsInlineOffset = 6;
constexpr duckdb::field_id_t kTermsInlineSize = 7;
constexpr duckdb::field_id_t kTermsRangesSize = 8;
constexpr duckdb::field_id_t kTermsFields = 9;
constexpr duckdb::field_id_t kTermsInlineStarts = 10;

constexpr duckdb::field_id_t kSpanBlock = 0;
constexpr duckdb::field_id_t kSpanExtraPages = 1;

constexpr duckdb::field_id_t kFieldId = 0;
constexpr duckdb::field_id_t kFieldDict = 1;
constexpr duckdb::field_id_t kFieldOrdinal = 2;
constexpr duckdb::field_id_t kFieldFeatures = 3;
constexpr duckdb::field_id_t kFieldTermCount = 4;
constexpr duckdb::field_id_t kFieldDocCount = 5;
constexpr duckdb::field_id_t kFieldTotalDocFreq = 6;
constexpr duckdb::field_id_t kFieldTotalTermFreq = 7;
constexpr duckdb::field_id_t kFieldScoreBounds = 8;
constexpr duckdb::field_id_t kFieldNorm = 9;
constexpr duckdb::field_id_t kFieldWidth = 10;
constexpr duckdb::field_id_t kFieldMin = 11;
constexpr duckdb::field_id_t kFieldRecord = 12;
constexpr duckdb::field_id_t kFieldRecordInline = 13;
constexpr duckdb::field_id_t kFieldMax = 14;
constexpr duckdb::field_id_t kFieldFirstBlock = 15;
constexpr duckdb::field_id_t kFieldFirstEntry = 16;
constexpr duckdb::field_id_t kFieldFirstRecord = 17;
constexpr duckdb::field_id_t kFieldLastBlock = 18;
constexpr duckdb::field_id_t kFieldLastEntry = 19;
constexpr duckdb::field_id_t kFieldInlineBase = 20;
constexpr duckdb::field_id_t kFieldInlineSize = 21;
constexpr duckdb::field_id_t kFieldRanges = 22;
constexpr duckdb::field_id_t kFieldRangeOffset = 23;
constexpr duckdb::field_id_t kFieldRangeSize = 24;
constexpr duckdb::field_id_t kFieldRangePrefix = 25;
constexpr duckdb::field_id_t kFieldRangeCounts = 26;
constexpr duckdb::field_id_t kFieldPrefixes = 27;

constexpr uint32_t kShortPrefix = 0xF0;
constexpr uint32_t kMediumPrefix = kShortPrefix + (0xFF - kShortPrefix) * 256;
constexpr uint64_t kFsstSampleBytes = 32 << 10;
constexpr size_t kFsstMinSamples = 64;
constexpr uint64_t kFsstMinMeanSuffix = 2;
constexpr uint64_t kFsstMaxMeanSuffix = 16;
constexpr uint64_t kFsstMinRatioPct = 115;
constexpr byte_type kZeros[kPage]{};
constexpr uint32_t kNoBlock = std::numeric_limits<uint32_t>::max();
constexpr size_t kBatchTerms = 64;

[[noreturn]] void Corrupted(std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("term dictionary: bad ", what));
}

[[noreturn]] void Corrupted(std::string_view what, field_id id) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("term dictionary: bad ", what, " of field ", id));
}

[[noreturn]] void Unsupported(std::string_view what, uint64_t value) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("term dictionary: unknown ", what, " ", value,
                          "; it was written by a newer release of SereneDB"));
}

void WriteBytes(duckdb::BinarySerializer& out, duckdb::field_id_t id,
                const char* tag, bytes_view bytes) {
  SDB_ENSURE(bytes.size() <= std::numeric_limits<uint32_t>::max(),
             "term dictionary: ", tag, " of ", bytes.size(), " bytes");
  out.WriteProperty<duckdb::string_t>(
    id, tag,
    duckdb::string_t{reinterpret_cast<const char*>(bytes.data()),
                     static_cast<uint32_t>(bytes.size())});
}

void WriteOptionalBytes(duckdb::BinarySerializer& out, duckdb::field_id_t id,
                        const char* tag, bytes_view bytes) {
  if (!bytes.empty()) {
    WriteBytes(out, id, tag, bytes);
  }
}

bstring ToBytes(std::string_view value) {
  return {reinterpret_cast<const byte_type*>(value.data()), value.size()};
}

bstring ReadBytes(duckdb::BinaryDeserializer& in, duckdb::field_id_t id,
                  const char* tag) {
  return ToBytes(in.ReadProperty<std::string>(id, tag));
}

bstring ReadOptionalBytes(duckdb::BinaryDeserializer& in, duckdb::field_id_t id,
                          const char* tag) {
  return ToBytes(in.ReadPropertyWithDefault<std::string>(id, tag));
}

void WriteField(duckdb::BinarySerializer& out, const FieldHead& head,
                size_t index) {
  out.WriteProperty<uint64_t>(kFieldId, "id", head.key.id);
  out.WritePropertyWithDefault<uint8_t>(kFieldDict, "dict",
                                        static_cast<uint8_t>(head.key.dict), 0);
  out.WritePropertyWithDefault<uint32_t>(kFieldOrdinal, "ordinal", head.ordinal,
                                         static_cast<uint32_t>(index));
  out.WriteProperty<uint32_t>(kFieldFeatures, "features",
                              static_cast<uint32_t>(head.features));
  out.WriteProperty<uint64_t>(kFieldTermCount, "term_count", head.term_count);
  out.WriteProperty<uint64_t>(kFieldDocCount, "doc_count", head.doc_count);
  out.WriteProperty<uint64_t>(kFieldTotalDocFreq, "total_doc_freq",
                              head.total_doc_freq);
  out.WritePropertyWithDefault<uint64_t>(kFieldTotalTermFreq, "total_term_freq",
                                         head.total_term_freq, 0);
  out.WritePropertyWithDefault<bool>(kFieldScoreBounds, "has_score_bounds",
                                     head.has_score_bounds, false);
  out.WritePropertyWithDefault<uint64_t>(kFieldNorm, "norm", head.norm,
                                         field_limits::invalid());
  out.WritePropertyWithDefault<uint32_t>(kFieldWidth, "width", head.width, 0);
  WriteBytes(out, kFieldMin, "min", head.min);
  if (!head.record.empty()) {
    WriteBytes(out, kFieldRecord, "record", head.record);
    WriteOptionalBytes(out, kFieldRecordInline, "record_inline",
                       head.record_inline);
  } else {
    WriteBytes(out, kFieldMax, "max", head.max);
    out.WriteProperty<uint32_t>(kFieldFirstBlock, "first_block",
                                head.first_block);
    out.WritePropertyWithDefault<uint32_t>(kFieldFirstEntry, "first_entry",
                                           head.first_entry, 0);
    out.WritePropertyWithDefault<uint32_t>(kFieldFirstRecord, "first_record",
                                           head.first_record, 0);
    out.WriteProperty<uint32_t>(kFieldLastBlock, "last_block", head.last_block);
    out.WriteProperty<uint32_t>(kFieldLastEntry, "last_entry", head.last_entry);
    out.WritePropertyWithDefault<uint64_t>(kFieldInlineBase, "inline_base",
                                           head.inline_base, 0);
    out.WritePropertyWithDefault<uint64_t>(kFieldInlineSize, "inline_size",
                                           head.inline_size, 0);
  }
  out.WritePropertyWithDefault<bool>(kFieldRanges, "ranges", head.ranges,
                                     false);
  if (!head.range_counts.empty()) {
    out.WriteProperty<uint64_t>(kFieldRangeOffset, "range_offset",
                                head.range_offset);
    out.WriteProperty<uint64_t>(kFieldRangeSize, "range_size", head.range_size);
    out.WritePropertyWithDefault<uint32_t>(kFieldRangePrefix, "range_prefix",
                                           head.range_prefix, 0);
    out.WriteList(kFieldRangeCounts, "range_counts", head.range_counts.size(),
                  [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                    list.WriteElement(head.range_counts[i]);
                  });
  }
  WriteOptionalBytes(out, kFieldPrefixes, "prefixes", head.prefixes);
}

FieldHead ReadField(duckdb::BinaryDeserializer& in, size_t index) {
  FieldHead head;
  head.key.id = in.ReadProperty<uint64_t>(kFieldId, "id");
  const auto dict =
    in.ReadPropertyWithExplicitDefault<uint8_t>(kFieldDict, "dict", 0);
  if (dict >= kTermDicts) {
    Unsupported("term dictionary kind", dict);
  }
  head.key.dict = static_cast<TermDict>(dict);
  head.ordinal = in.ReadPropertyWithExplicitDefault<uint32_t>(
    kFieldOrdinal, "ordinal", static_cast<uint32_t>(index));
  const auto features = in.ReadProperty<uint32_t>(kFieldFeatures, "features");
  if ((features & ~static_cast<uint32_t>(IndexFeatures::Max)) != 0) {
    Unsupported("index features", features);
  }
  head.features = static_cast<IndexFeatures>(features);
  head.term_count = in.ReadProperty<uint64_t>(kFieldTermCount, "term_count");
  head.doc_count = in.ReadProperty<uint64_t>(kFieldDocCount, "doc_count");
  head.total_doc_freq =
    in.ReadProperty<uint64_t>(kFieldTotalDocFreq, "total_doc_freq");
  head.total_term_freq = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kFieldTotalTermFreq, "total_term_freq", 0);
  head.has_score_bounds = in.ReadPropertyWithExplicitDefault<bool>(
    kFieldScoreBounds, "has_score_bounds", false);
  head.norm = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kFieldNorm, "norm", field_limits::invalid());
  head.width =
    in.ReadPropertyWithExplicitDefault<uint32_t>(kFieldWidth, "width", 0);
  head.min = ReadBytes(in, kFieldMin, "min");
  head.record = ReadOptionalBytes(in, kFieldRecord, "record");
  head.record_inline =
    ReadOptionalBytes(in, kFieldRecordInline, "record_inline");
  if (head.record.empty()) {
    head.max = ReadBytes(in, kFieldMax, "max");
    head.first_block =
      in.ReadProperty<uint32_t>(kFieldFirstBlock, "first_block");
    head.first_entry = in.ReadPropertyWithExplicitDefault<uint32_t>(
      kFieldFirstEntry, "first_entry", 0);
    head.first_record = in.ReadPropertyWithExplicitDefault<uint32_t>(
      kFieldFirstRecord, "first_record", 0);
    head.last_block = in.ReadProperty<uint32_t>(kFieldLastBlock, "last_block");
    head.last_entry = in.ReadProperty<uint32_t>(kFieldLastEntry, "last_entry");
    head.inline_base = in.ReadPropertyWithExplicitDefault<uint64_t>(
      kFieldInlineBase, "inline_base", 0);
    head.inline_size = in.ReadPropertyWithExplicitDefault<uint64_t>(
      kFieldInlineSize, "inline_size", 0);
  }
  head.ranges =
    in.ReadPropertyWithExplicitDefault<bool>(kFieldRanges, "ranges", false);
  head.range_offset = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kFieldRangeOffset, "range_offset", 0);
  head.range_size = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kFieldRangeSize, "range_size", 0);
  head.range_prefix = in.ReadPropertyWithExplicitDefault<uint32_t>(
    kFieldRangePrefix, "range_prefix", 0);
  in.ReadOptionalList(
    kFieldRangeCounts, "range_counts",
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      head.range_counts.push_back(list.ReadElement<uint64_t>());
    });
  head.prefixes = ReadOptionalBytes(in, kFieldPrefixes, "prefixes");
  return head;
}

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

uint64_t VSize(uint64_t value) noexcept {
  return bytes_io<uint64_t, sizeof(uint64_t)>::vsize(value);
}

constexpr uint64_t kWordBits = 64;

void SetBitOf(std::vector<uint64_t>& words, uint64_t bit) noexcept {
  SetBit(words[bit / kWordBits], bit % kWordBits);
}

bool BitOf(std::span<const uint64_t> words, uint64_t bit) noexcept {
  return CheckBit(words[bit / kWordBits], bit % kWordBits);
}

uint64_t SelectInWord(uint64_t bits, uint64_t rank) noexcept {
#ifdef __BMI2__
  return static_cast<uint64_t>(
    std::countr_zero(_pdep_u64(uint64_t{1} << rank, bits)));
#else
  for (; rank != 0; --rank) {
    bits &= bits - 1;
  }
  return static_cast<uint64_t>(std::countr_zero(bits));
#endif
}

void AppendFieldPrefix(bstring& out, uint32_t ordinal) {
  if (ordinal < kShortPrefix) {
    out.push_back(static_cast<byte_type>(ordinal));
    return;
  }
  if (ordinal < kMediumPrefix) {
    const uint32_t rest = ordinal - kShortPrefix;
    out.push_back(static_cast<byte_type>(kShortPrefix + rest / 256));
    out.push_back(static_cast<byte_type>(rest % 256));
    return;
  }
  out.push_back(0xFF);
  byte_type rest[sizeof(uint32_t)];
  absl::big_endian::Store32(rest, ordinal - kMediumPrefix);
  out.append(rest, sizeof(rest));
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

std::vector<unsigned char*> SampleStarts(bstring& arena,
                                         std::span<const size_t> lengths) {
  std::vector<unsigned char*> starts(lengths.size());
  auto* start = arena.data();
  for (size_t i = 0; i != lengths.size(); ++i) {
    starts[i] = start;
    start += lengths[i];
  }
  return starts;
}

bool Compresses(duckdb_fsst_encoder_t* encoder, bstring& arena,
                std::vector<size_t>& lengths, uint64_t table_size) {
  auto starts = SampleStarts(arena, lengths);
  const uint64_t raw = arena.size();
  bstring out(7 + 2 * raw, 0);
  std::vector<size_t> out_lengths(lengths.size());
  std::vector<unsigned char*> out_starts(lengths.size());
  const size_t done = duckdb_fsst_compress(
    encoder, lengths.size(), lengths.data(), starts.data(), out.size(),
    out.data(), out_lengths.data(), out_starts.data());
  const uint64_t coded = absl::c_accumulate(out_lengths, uint64_t{0});
  return done == lengths.size() &&
         raw * 100 >= coded * kFsstMinRatioPct + table_size * 100;
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

void WriteColumn(BytesOutput& out, std::span<const uint32_t> values,
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

void WriteHeads(BytesOutput& out, const BlockBuilder& block, size_t step,
                size_t skip) {
  for (size_t i = 0; i < block.ends.size(); i += step) {
    out.WriteU32(static_cast<uint32_t>(HeadAt(KeyAt(block, i), skip) >> 32));
  }
}

void Serialize(const BlockBuilder& block, bstring& page) {
  page.clear();
  BytesOutput out{page};
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
  SDB_ASSERT(page.size() == (coded ? block.coded_size : block.size));
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

void RecordOutput::WriteDirect(const byte_type*, size_t) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("term dictionary: posting record exceeds ",
                          kMaxRecordBytes, " bytes"));
}

void FsstEncoderDelete::operator()(
  duckdb_fsst_encoder_t* encoder) const noexcept {
  duckdb_fsst_destroy(encoder);
}

void FieldWriter::SampleFsst(const BasicTermReader& reader) {
  reader.SampleTerms(kFsstSampleBytes, [&](std::span<const bytes_view> run) {
    for (size_t i = 1; i < run.size(); ++i) {
      const auto suffix = run[i].substr(CommonPrefixLength(run[i - 1], run[i]));
      if (!suffix.empty()) {
        _samples.append(suffix);
        _sample_lengths.push_back(suffix.size());
      }
    }
  });
  const bool enough = _sample_lengths.size() >= kFsstMinSamples;
  if (_fsst) {
    _encode = !enough || Compresses(_fsst.get(), _samples, _sample_lengths, 0);
    _samples.clear();
    _sample_lengths.clear();
    return;
  }
  if (!enough) {
    return;
  }
  TrainFsst();
  _samples.clear();
  _sample_lengths.clear();
  auto& block = _block;
  if (!_fsst || block.kind != BlockKind::Var || block.ends.empty()) {
    return;
  }
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

void FieldWriter::TrainFsst() {
  auto& arena = _samples;
  auto& lengths = _sample_lengths;
  const uint64_t raw = arena.size();
  if (raw < kFsstMinMeanSuffix * lengths.size() ||
      raw > kFsstMaxMeanSuffix * lengths.size()) {
    return;
  }
  auto starts = SampleStarts(arena, lengths);
  FsstEncoder encoder{
    duckdb_fsst_create(lengths.size(), lengths.data(), starts.data(), 0)};
  if (!encoder) {
    return;
  }
  unsigned char table[FSST_MAXHEADER];
  const auto table_size = duckdb_fsst_export(encoder.get(), table);
  if (!Compresses(encoder.get(), arena, lengths, table_size)) {
    return;
  }
  _fsst = std::move(encoder);
  _fsst_table.assign(table, table_size);
}

bytes_view FieldWriter::EncodeSuffix(bytes_view suffix) {
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

void WriteTerms(duckdb::BinarySerializer& out, const TermsMeta& terms) {
  out.WriteProperty<uint32_t>(kTermsRestartInterval, "restart_interval",
                              kRestartInterval);
  out.WritePropertyWithDefault<uint32_t>(kTermsBlocks, "blocks", terms.blocks,
                                         0);
  out.WritePropertyWithDefault<uint64_t>(kTermsFirstPage, "first_page",
                                         terms.first_page, 0);
  if (!terms.spans.empty()) {
    out.WriteList(kTermsSpans, "spans", terms.spans.size(),
                  [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                    list.WriteObject([&](duckdb::BinarySerializer& span) {
                      span.WriteProperty<uint32_t>(kSpanBlock, "block",
                                                   terms.spans[i].block);
                      span.WriteProperty<uint32_t>(
                        kSpanExtraPages, "extra_pages", terms.spans[i].extra);
                    });
                  });
  }
  WriteOptionalBytes(out, kTermsSeparators, "separators", terms.separators);
  WriteOptionalBytes(out, kTermsFsst, "fsst_table", terms.fsst_table);
  out.WritePropertyWithDefault<uint64_t>(kTermsInlineOffset, "inline_offset",
                                         terms.inline_offset, 0);
  out.WritePropertyWithDefault<uint64_t>(kTermsInlineSize, "inline_size",
                                         terms.inline_size, 0);
  out.WritePropertyWithDefault<uint64_t>(kTermsRangesSize, "ranges_size",
                                         terms.ranges_size, 0);
  out.WriteList(kTermsFields, "fields", terms.fields.size(),
                [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                  list.WriteObject([&](duckdb::BinarySerializer& field) {
                    WriteField(field, terms.fields[i], i);
                  });
                });
  WriteOptionalBytes(out, kTermsInlineStarts, "inline_starts",
                     terms.inline_starts);
}

TermsMeta ReadTerms(duckdb::BinaryDeserializer& in, uint64_t data_size) {
  const auto restart =
    in.ReadProperty<uint32_t>(kTermsRestartInterval, "restart_interval");
  if (restart != kRestartInterval) {
    Unsupported("restart interval", restart);
  }
  TermsMeta terms;
  terms.blocks =
    in.ReadPropertyWithExplicitDefault<uint32_t>(kTermsBlocks, "blocks", 0);
  terms.first_page = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kTermsFirstPage, "first_page", 0);
  in.ReadOptionalList(
    kTermsSpans, "spans",
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::BinaryDeserializer& span) {
        terms.spans.push_back({
          .block = span.ReadProperty<uint32_t>(kSpanBlock, "block"),
          .extra = span.ReadProperty<uint32_t>(kSpanExtraPages, "extra_pages"),
        });
      });
    });
  terms.separators = ReadOptionalBytes(in, kTermsSeparators, "separators");
  terms.fsst_table = ReadOptionalBytes(in, kTermsFsst, "fsst_table");
  terms.inline_offset = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kTermsInlineOffset, "inline_offset", 0);
  terms.inline_size = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kTermsInlineSize, "inline_size", 0);
  terms.ranges_size = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kTermsRangesSize, "ranges_size", 0);
  in.ReadList(kTermsFields, "fields",
              [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t i) {
                list.ReadObject([&](duckdb::BinaryDeserializer& field) {
                  terms.fields.push_back(ReadField(field, i));
                });
              });
  terms.inline_starts =
    ReadOptionalBytes(in, kTermsInlineStarts, "inline_starts");
  if (terms.inline_offset > data_size ||
      terms.inline_size > data_size - terms.inline_offset ||
      terms.ranges_size > data_size - terms.inline_offset - terms.inline_size) {
    Corrupted("inline postings");
  }
  return terms;
}

void FieldWriter::prepare(const FlushState& state) {
  SDB_ASSERT(_idx);
  Reset();
  _out = &_idx->BlocksOut();
  _pw.Prepare(state);
}

void FieldWriter::write(const BasicTermReader& reader) {
  const auto props = reader.properties();
  _pw.SetTermPayloadWriter(reader.PayloadWriter());
  _pw.BeginField(props);
  const auto width = reader.KeyWidth();
  _width = width <= kMaxStride ? width : 0;
  if (_width == 0) {
    SampleFsst(reader);
  }
  if (props.ranges && _width != 0) {
    SDB_ASSERT(!IsSubsetOf(IndexFeatures::Pos, props.index_features));
    _ranges.Begin(_pw);
  }

  const uint64_t first_inline = _inline.size();
  uint64_t sum_dfreq = 0;
  uint64_t sum_tfreq = 0;
  const bool freq_exists =
    IndexFeatures::None != (props.index_features & IndexFeatures::Freq);

  const auto consume = [&](bytes_view term, const PostingMeta& meta) {
    if (freq_exists) {
      sum_tfreq += meta.freq;
    }
    if (meta.docs_count == 0) {
      return;
    }
    sum_dfreq += meta.docs_count;
    Add(term, meta);
    if (_ranges.Active()) {
      _ranges.Add(term, meta);
    }
  };

  auto terms = reader.iterator();
  SDB_ASSERT(terms);

  if (_compaction) {
    while (terms->next()) {
      PostingMeta meta;
      auto postings = terms->postings(props.index_features);
      _pw.Write(*postings, meta);
      consume(terms->value(), meta);
    }
  } else {
    constexpr size_t kTermBatch = 128;
    bytes_view term_batch[kTermBatch];
    PostingRows posting_batch[kTermBatch];
    while (const size_t n = terms->NextTermsWithPostings(
             term_batch, posting_batch, props.index_features)) {
      for (size_t i = 0; i != n; ++i) {
        PostingMeta meta;
        _pw.WritePostings(posting_batch[i], meta);
        consume(term_batch[i], meta);
      }
    }
  }

  EndField(reader.key(), props, sum_dfreq, sum_tfreq, first_inline);
  ResetField();
}

void FieldWriter::Add(bytes_view term, const PostingMeta& meta) {
  if (_width == 0) {
    const auto prefix = TermPrefix(term);
    if (_prefixes.empty() || _prefixes.back() != prefix) {
      _prefixes.push_back(prefix);
    }
  }
  if (_terms++ == 0) {
    _min.assign(term);
    _single = meta;
    _key.clear();
    AppendFieldPrefix(_key, _ordinal++);
    _prefix_size = static_cast<uint32_t>(_key.size());
    return;
  }
  if (_terms == 2) {
    Commit(_min, _single);
  }
  Commit(term, meta);
}

void FieldWriter::Commit(bytes_view term, const PostingMeta& meta) {
  _key.resize(_prefix_size);
  _key.append(term);
  const bytes_view key{_key};
  if (!Append(key, meta)) {
    Cut();
    Append(key, meta);
  }
  _inline.append(meta.Inline());
  if (_first_block == kNoBlock) {
    _first_block = _blocks;
    _first_entry = static_cast<uint32_t>(_block.ends.size() - 1);
    _first_record =
      static_cast<uint32_t>(_block.records.size() - _record.View().size());
  }
}

bool FieldWriter::Append(bytes_view key, const PostingMeta& meta) {
  auto& block = _block;
  const bool record_start = block.ends.size() % kRecordInterval == 0;
  if (record_start) {
    _pw.BeginBlock();
  }
  _record.Clear();
  _pw.EncodeDetached(_record, meta, _inline.size());
  const auto record = _record.View();
  const uint64_t records = block.records.size() + record.size();
  const bool fixed =
    _width != 0 &&
    (block.ends.empty() ||
     (block.kind == BlockKind::FixedStride && block.width == key.size()));
  if (!(fixed ? AppendFixedKey(key, records) : AppendVarKey(key, records))) {
    return false;
  }
  if (record_start) {
    block.record_starts.push_back(static_cast<uint16_t>(block.records.size()));
  }
  block.records.append(record);
  block.keys.append(key);
  block.ends.push_back(static_cast<uint32_t>(block.keys.size()));
  return true;
}

bool FieldWriter::AppendFixedKey(bytes_view key, uint64_t records) {
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

void FieldWriter::AppendVarColumns(uint32_t index, bytes_view key) {
  auto& block = _block;
  const auto entry = VarEntryAt(block, index, key);
  block.head_skip = entry.head_skip;
  AppendEntry(block.raw, entry.restart, entry.lcp, entry.suffix);
  if (_fsst && _encode && block.all_coded) {
    AppendEntry(block.coded, entry.restart, entry.lcp,
                entry.restart ? entry.suffix : EncodeSuffix(entry.suffix));
  }
}

bool FieldWriter::AppendVarKey(bytes_view key, uint64_t records) {
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

void FieldWriter::Seal() {
  if (_block.kind == BlockKind::Var && _block.coded_size < _block.size) {
    _block.kind = BlockKind::VarFsst;
  }
  Serialize(_block, _page);
}

void FieldWriter::Cut() {
  if (_block.ends.empty()) {
    return;
  }
  Seal();
  const auto first = KeyAt(_block, 0);
  if (_blocks != 0) {
    const auto separator =
      first.substr(0, CommonPrefixLength(bytes_view{_last}, first) + 1);
    const auto shared = CommonPrefixLength(bytes_view{_separator}, separator);
    BytesOutput out{_separators};
    out.WriteV32(static_cast<uint32_t>(shared));
    WriteStr(out, separator.substr(shared));
    _separator.assign(separator);
  }
  WriteBlock(_page);
  BytesOutput{_inline_starts}.WriteV64(_inline_start - _inline_last);
  _inline_last = _inline_start;
  _inline_start = _inline.size();
  _last.assign(KeyAt(_block, _block.ends.size() - 1));
  Clear(_block);
}

void FieldWriter::WriteBlock(bytes_view block) {
  const uint64_t position = _out->Position();
  const uint64_t pad = (kPage - position % kPage) % kPage;
  _out->WriteData(kZeros, pad);
  if (_blocks == 0) {
    _first_page = (position + pad) / kPage;
  }
  const auto pages = static_cast<uint32_t>((block.size() + kPage - 1) / kPage);
  if (pages != 1) {
    _spans.push_back({.block = _blocks, .extra = pages - 1});
  }
  _out->WriteData(block.data(), block.size());
  ++_blocks;
}

void FieldWriter::EndField(FieldKey key, const FieldProperties& props,
                           uint64_t total_doc_freq, uint64_t total_term_freq,
                           uint64_t first_inline) {
  if (_ranges.Active()) {
    _ranges.Finish(_terms);
  }
  const auto stats = _pw.EndField();
  if (_terms == 0) {
    return;
  }

  auto& head = _heads.emplace_back();
  head.key = key;
  head.features = props.index_features;
  head.term_count = _terms;
  head.doc_count = stats.docs_count;
  head.total_doc_freq = total_doc_freq;
  head.total_term_freq = total_term_freq;
  head.norm = props.norm;
  head.has_score_bounds = stats.has_score_bounds;
  head.ranges = props.ranges;
  head.ordinal = _ordinal - 1;
  head.width = _width;
  head.min = _min;
  if (_terms == 1) {
    _record.Clear();
    _pw.EncodeDetached(_record, _single, 0);
    head.record = _record.View();
    head.record_inline = _single.Inline();
    return;
  }
  head.max = bytes_view{_key}.substr(_prefix_size);
  head.first_block = _first_block;
  head.first_entry = _first_entry;
  head.first_record = _first_record;
  head.last_block = _blocks;
  head.last_entry = static_cast<uint32_t>(_block.ends.size() - 1);
  if (_first_block == _blocks) {
    head.inline_base = first_inline;
    head.inline_size = _inline.size() - first_inline;
  } else {
    auto out = std::back_inserter(head.prefixes);
    uint32_t last = 0;
    for (const auto prefix : _prefixes) {
      WriteVarint(prefix - last, out);
      last = prefix;
    }
  }
  if (props.ranges) {
    bstring region;
    const auto prefix =
      CommonPrefixLength(bytes_view{_min}, bytes_view{head.max});
    _ranges.Write(region, prefix, head.range_counts);
    if (!head.range_counts.empty()) {
      head.range_offset = _range_regions.size();
      head.range_size = region.size();
      head.range_prefix = static_cast<uint32_t>(prefix);
      _range_regions.append(region);
    }
  }
}

void FieldWriter::ResetField() noexcept {
  _ranges.Clear();
  _prefixes.clear();
  _min.clear();
  _terms = 0;
  _first_block = kNoBlock;
  _encode = true;
}

void FieldWriter::Reset() noexcept {
  ResetField();
  Clear(_block);
  _fsst.reset();
  _fsst_table.clear();
  _samples.clear();
  _sample_lengths.clear();
  _inline.clear();
  _spans.clear();
  _separators.clear();
  _separator.clear();
  _last.clear();
  _inline_starts.clear();
  _range_regions.clear();
  _heads.clear();
  _first_page = 0;
  _inline_start = 0;
  _inline_last = 0;
  _blocks = 0;
  _ordinal = 0;
}

void FieldWriter::end() {
  Cut();
  _pw.End();
  if (!_heads.empty()) {
    auto& out = *_out;
    TermsMeta terms{
      .blocks = _blocks,
      .first_page = _first_page,
      .spans = std::move(_spans),
      .separators = std::move(_separators),
      .fsst_table = std::move(_fsst_table),
      .inline_offset = out.Position(),
      .inline_size = _inline.size(),
      .ranges_size = _range_regions.size(),
      .fields = std::move(_heads),
      .inline_starts = _inline.empty() ? bstring{} : std::move(_inline_starts),
    };
    out.WriteData(_inline.data(), _inline.size());
    out.WriteData(_range_regions.data(), _range_regions.size());
    _idx->SetTerms(std::move(terms));
  }
  Reset();
  _idx = nullptr;
  _out = nullptr;
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
  _width = head.width;
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
      _root = Root{MakeSeparators(slice), _width != 0};
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
      _width == 0 || _range_prefix > _width ||
      min().substr(0, _range_prefix) != max().substr(0, _range_prefix)) {
    corrupted();
  }
  const auto* p = owner.Load(in, owner.RangesBegin() + offset, size).data();
  const auto* const end = p + size;
  const uint64_t stride = _width - _range_prefix;
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
         (_width != 0 && term.size() != _width);
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
