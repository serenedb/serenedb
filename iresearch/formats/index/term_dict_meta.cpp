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

#include "iresearch/formats/index/term_dict_meta.hpp"

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <limits>
#include <string>
#include <string_view>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/utils/assert.hpp"
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

}  // namespace

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

}  // namespace irs::term_dict
