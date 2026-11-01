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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <limits>
#include <span>
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
constexpr duckdb::field_id_t kTermsOrdinalBytes = 11;

constexpr duckdb::field_id_t kSpanBlock = 0;
constexpr duckdb::field_id_t kSpanExtraPages = 1;

constexpr duckdb::field_id_t kFieldId = 0;
constexpr duckdb::field_id_t kFieldLayout = 1;
constexpr duckdb::field_id_t kFieldFirstDict = 2;

constexpr std::array<const char*, kValueKinds> kKindTags{
  "string", "false", "true", "numeric", "sql_null", "json_null", "null_element",
};

constexpr duckdb::field_id_t DictProperty(size_t position) noexcept {
  return static_cast<duckdb::field_id_t>(kFieldFirstDict + position);
}

constexpr const char* KindTag(ValueKind kind) noexcept {
  return kKindTags[static_cast<size_t>(kind)];
}

constexpr duckdb::field_id_t kPostingsRecord = 0;
constexpr duckdb::field_id_t kPostingsRecordInline = 1;

constexpr duckdb::field_id_t kDictFeatures = 0;
constexpr duckdb::field_id_t kDictTermCount = 1;
constexpr duckdb::field_id_t kDictDocCount = 2;
constexpr duckdb::field_id_t kDictTotalFreq = 3;
constexpr duckdb::field_id_t kDictScoreBounds = 4;
constexpr duckdb::field_id_t kDictNorm = 5;
constexpr duckdb::field_id_t kDictWidth = 6;
constexpr duckdb::field_id_t kDictMin = 7;
constexpr duckdb::field_id_t kDictRecord = 8;
constexpr duckdb::field_id_t kDictRecordInline = 9;
constexpr duckdb::field_id_t kDictMax = 10;
constexpr duckdb::field_id_t kDictFirstBlock = 11;
constexpr duckdb::field_id_t kDictFirstEntry = 12;
constexpr duckdb::field_id_t kDictFirstRecord = 13;
constexpr duckdb::field_id_t kDictLastBlock = 14;
constexpr duckdb::field_id_t kDictLastEntry = 15;
constexpr duckdb::field_id_t kDictInlineBase = 16;
constexpr duckdb::field_id_t kDictInlineSize = 17;
constexpr duckdb::field_id_t kDictRangeOffset = 18;
constexpr duckdb::field_id_t kDictRangeSize = 19;
constexpr duckdb::field_id_t kDictRangePrefix = 20;
constexpr duckdb::field_id_t kDictRangeCounts = 21;
constexpr duckdb::field_id_t kDictPrefixes = 22;

[[noreturn]] void Unsupported(std::string_view what, uint64_t value) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("term dictionary: unknown ", what, " ", value,
                          "; it was written by a newer release of SereneDB"));
}

void WriteBytes(duckdb::BinarySerializer& out, duckdb::field_id_t id,
                const char* tag, const std::string& bytes) {
  SDB_ENSURE(bytes.size() <= std::numeric_limits<uint32_t>::max(),
             "term dictionary: ", tag, " of ", bytes.size(), " bytes");
  out.WriteProperty(id, tag, bytes);
}

void WriteOptionalBytes(duckdb::BinarySerializer& out, duckdb::field_id_t id,
                        const char* tag, const std::string& bytes) {
  if (!bytes.empty()) {
    WriteBytes(out, id, tag, bytes);
  }
}

void WritePostings(duckdb::BinarySerializer& out, const FieldHead& head) {
  SDB_ASSERT(head.features == IndexFeatures::None && head.term_count == 1 &&
             head.min.empty() && !head.record.empty() &&
             !head.has_score_bounds);
  WriteBytes(out, kPostingsRecord, "record", head.record);
  WriteOptionalBytes(out, kPostingsRecordInline, "record_inline",
                     head.record_inline);
}

void WriteDict(duckdb::BinarySerializer& out, const FieldHead& head) {
  out.WriteProperty<uint32_t>(kDictFeatures, "features",
                              static_cast<uint32_t>(head.features));
  out.WriteProperty<uint64_t>(kDictTermCount, "term_count", head.term_count);
  out.WriteProperty<uint64_t>(kDictDocCount, "doc_count", head.doc_count);
  out.WritePropertyWithDefault<uint64_t>(kDictTotalFreq, "total_freq",
                                         head.total_freq, 0);
  out.WritePropertyWithDefault<bool>(kDictScoreBounds, "has_score_bounds",
                                     head.has_score_bounds, false);
  out.WritePropertyWithDefault<uint64_t>(kDictNorm, "norm", head.norm,
                                         field_limits::invalid());
  out.WritePropertyWithDefault<uint32_t>(kDictWidth, "width", head.width, 0);
  WriteOptionalBytes(out, kDictMin, "min", head.min);
  if (head.term_count == 1) {
    WriteBytes(out, kDictRecord, "record", head.record);
    WriteOptionalBytes(out, kDictRecordInline, "record_inline",
                       head.record_inline);
  } else {
    WriteBytes(out, kDictMax, "max", head.max);
    out.WriteProperty<uint32_t>(kDictFirstBlock, "first_block",
                                head.first_block);
    out.WritePropertyWithDefault<uint32_t>(kDictFirstEntry, "first_entry",
                                           head.first_entry, 0);
    out.WritePropertyWithDefault<uint32_t>(kDictFirstRecord, "first_record",
                                           head.first_record, 0);
    out.WriteProperty<uint32_t>(kDictLastBlock, "last_block", head.last_block);
    out.WriteProperty<uint32_t>(kDictLastEntry, "last_entry", head.last_entry);
    out.WritePropertyWithDefault<uint64_t>(kDictInlineBase, "inline_base",
                                           head.inline_base, 0);
    out.WritePropertyWithDefault<uint64_t>(kDictInlineSize, "inline_size",
                                           head.inline_size, 0);
  }
  if (!head.range_counts.empty()) {
    SDB_ASSERT(head.kind == ValueKind::Numeric && head.width != 0);
    out.WriteProperty<uint64_t>(kDictRangeOffset, "range_offset",
                                head.range_offset);
    out.WriteProperty<uint64_t>(kDictRangeSize, "range_size", head.range_size);
    out.WritePropertyWithDefault<uint32_t>(kDictRangePrefix, "range_prefix",
                                           head.range_prefix, 0);
    out.WriteList(kDictRangeCounts, "range_counts", head.range_counts.size(),
                  [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                    list.WriteElement(head.range_counts[i]);
                  });
  }
  WriteOptionalBytes(out, kDictPrefixes, "prefixes", head.prefixes);
}

void WriteField(duckdb::BinarySerializer& out,
                std::span<const FieldHead> dicts) {
  const auto layout = dicts.front().layout;
  out.WriteProperty<uint64_t>(kFieldId, "id", dicts.front().id);
  out.WritePropertyWithDefault<uint8_t>(
    kFieldLayout, "layout", static_cast<uint8_t>(layout),
    static_cast<uint8_t>(DictLayout::String));
  for (const auto& head : dicts) {
    SDB_ASSERT(head.layout == layout);
    const auto position = PositionOf(layout, head.kind);
    SDB_ASSERT(position != kNoPosition);
    out.WriteObject(DictProperty(position), KindTag(head.kind),
                    [&](duckdb::BinarySerializer& object) {
                      if (IsKeyed(head.kind)) {
                        WriteDict(object, head);
                      } else {
                        WritePostings(object, head);
                      }
                    });
  }
}

void ReadPostings(duckdb::BinaryDeserializer& in, FieldHead& head) {
  head.term_count = 1;
  head.record = in.ReadProperty<std::string>(kPostingsRecord, "record");
  head.record_inline = in.ReadPropertyWithDefault<std::string>(
    kPostingsRecordInline, "record_inline");
  if (head.record.empty()) {
    Corrupted("record", head.id);
  }
}

void ReadDict(duckdb::BinaryDeserializer& in, FieldHead& head) {
  const auto features = in.ReadProperty<uint32_t>(kDictFeatures, "features");
  if ((features & ~static_cast<uint32_t>(IndexFeatures::Max)) != 0) {
    Unsupported("index features", features);
  }
  head.features = static_cast<IndexFeatures>(features);
  head.term_count = in.ReadProperty<uint64_t>(kDictTermCount, "term_count");
  head.doc_count = in.ReadProperty<uint64_t>(kDictDocCount, "doc_count");
  head.total_freq = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kDictTotalFreq, "total_freq", 0);
  head.has_score_bounds = in.ReadPropertyWithExplicitDefault<bool>(
    kDictScoreBounds, "has_score_bounds", false);
  head.norm = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kDictNorm, "norm", field_limits::invalid());
  head.width =
    in.ReadPropertyWithExplicitDefault<uint32_t>(kDictWidth, "width", 0);
  head.min = in.ReadPropertyWithDefault<std::string>(kDictMin, "min");
  if (head.term_count == 0) {
    Corrupted("term count", head.id);
  }
  if (head.term_count == 1) {
    head.record = in.ReadProperty<std::string>(kDictRecord, "record");
    head.record_inline = in.ReadPropertyWithDefault<std::string>(
      kDictRecordInline, "record_inline");
    if (head.record.empty()) {
      Corrupted("record", head.id);
    }
  } else {
    head.max = in.ReadProperty<std::string>(kDictMax, "max");
    head.first_block =
      in.ReadProperty<uint32_t>(kDictFirstBlock, "first_block");
    head.first_entry = in.ReadPropertyWithExplicitDefault<uint32_t>(
      kDictFirstEntry, "first_entry", 0);
    head.first_record = in.ReadPropertyWithExplicitDefault<uint32_t>(
      kDictFirstRecord, "first_record", 0);
    head.last_block = in.ReadProperty<uint32_t>(kDictLastBlock, "last_block");
    head.last_entry = in.ReadProperty<uint32_t>(kDictLastEntry, "last_entry");
    head.inline_base = in.ReadPropertyWithExplicitDefault<uint64_t>(
      kDictInlineBase, "inline_base", 0);
    head.inline_size = in.ReadPropertyWithExplicitDefault<uint64_t>(
      kDictInlineSize, "inline_size", 0);
  }
  head.range_offset = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kDictRangeOffset, "range_offset", 0);
  head.range_size = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kDictRangeSize, "range_size", 0);
  head.range_prefix = in.ReadPropertyWithExplicitDefault<uint32_t>(
    kDictRangePrefix, "range_prefix", 0);
  in.ReadOptionalList(
    kDictRangeCounts, "range_counts",
    [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      head.range_counts.push_back(list.ReadElement<uint64_t>());
    });
  head.prefixes =
    in.ReadPropertyWithDefault<std::string>(kDictPrefixes, "prefixes");
  const bool ranged = head.kind == ValueKind::Numeric && head.width != 0;
  if ((!ranged && (head.range_size != 0 || !head.range_counts.empty())) ||
      (head.kind != ValueKind::String &&
       (!head.prefixes.empty() || head.norm != field_limits::invalid()))) {
    Corrupted("dictionary", head.id);
  }
}

void ReadField(duckdb::BinaryDeserializer& in, std::vector<FieldHead>& fields) {
  const auto id = in.ReadProperty<uint64_t>(kFieldId, "id");
  if (!fields.empty() && fields.back().id >= id) {
    Corrupted("field order", id);
  }
  const auto layout = in.ReadPropertyWithExplicitDefault<uint8_t>(
    kFieldLayout, "layout", static_cast<uint8_t>(DictLayout::String));
  if (layout >= kDictLayouts) {
    Unsupported("dictionary layout", layout);
  }
  const auto kinds = LayoutKinds(static_cast<DictLayout>(layout));
  const auto first = fields.size();
  for (size_t position = 0; position != kinds.size(); ++position) {
    const auto kind = kinds[position];
    in.ReadOptionalObject(DictProperty(position), KindTag(kind),
                          [&](duckdb::BinaryDeserializer& object) {
                            auto& head = fields.emplace_back();
                            head.id = id;
                            head.layout = static_cast<DictLayout>(layout);
                            head.kind = kind;
                            if (IsKeyed(kind)) {
                              ReadDict(object, head);
                            } else {
                              ReadPostings(object, head);
                            }
                          });
  }
  if (fields.size() == first) {
    Corrupted("empty field", id);
  }
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
  const std::span<const FieldHead> heads{terms.fields};
  size_t fields = 0;
  for (size_t i = 0; i != heads.size(); ++i) {
    fields += i == 0 || heads[i - 1].id != heads[i].id;
  }
  out.WriteList(
    kTermsFields, "fields", fields,
    [&, at = size_t{0}](duckdb::BinarySerializer::List& list,
                        duckdb::idx_t) mutable {
      auto end = at + 1;
      while (end != heads.size() && heads[end].id == heads[at].id) {
        SDB_ASSERT(PositionOf(heads[at].layout, heads[end - 1].kind) <
                   PositionOf(heads[at].layout, heads[end].kind));
        ++end;
      }
      list.WriteObject([&](duckdb::BinarySerializer& field) {
        WriteField(field, heads.subspan(at, end - at));
      });
      at = end;
    });
  WriteOptionalBytes(out, kTermsInlineStarts, "inline_starts",
                     terms.inline_starts);
  out.WritePropertyWithDefault<uint32_t>(kTermsOrdinalBytes, "ordinal_bytes",
                                         terms.ordinal_bytes, 1);
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
  terms.separators =
    in.ReadPropertyWithDefault<std::string>(kTermsSeparators, "separators");
  terms.fsst_table =
    in.ReadPropertyWithDefault<std::string>(kTermsFsst, "fsst_table");
  terms.inline_offset = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kTermsInlineOffset, "inline_offset", 0);
  terms.inline_size = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kTermsInlineSize, "inline_size", 0);
  terms.ranges_size = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kTermsRangesSize, "ranges_size", 0);
  in.ReadList(kTermsFields, "fields",
              [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
                list.ReadObject([&](duckdb::BinaryDeserializer& field) {
                  ReadField(field, terms.fields);
                });
              });
  terms.inline_starts = in.ReadPropertyWithDefault<std::string>(
    kTermsInlineStarts, "inline_starts");
  terms.ordinal_bytes = in.ReadPropertyWithExplicitDefault<uint32_t>(
    kTermsOrdinalBytes, "ordinal_bytes", 1);
  if (terms.ordinal_bytes == 0 || terms.ordinal_bytes > kMaxOrdinalBytes) {
    Unsupported("ordinal width", terms.ordinal_bytes);
  }
  const auto keyed = absl::c_count_if(
    terms.fields, [](const FieldHead& head) { return IsKeyed(head.kind); });
  if (static_cast<uint64_t>(keyed) > OrdinalLimit(terms.ordinal_bytes)) {
    Corrupted("ordinal width");
  }
  if (terms.inline_offset > data_size ||
      terms.inline_size > data_size - terms.inline_offset ||
      terms.ranges_size > data_size - terms.inline_offset - terms.inline_size) {
    Corrupted("inline postings");
  }
  return terms;
}

}  // namespace irs::term_dict
