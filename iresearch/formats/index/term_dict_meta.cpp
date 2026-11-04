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

constexpr duckdb::field_id_t DictProperty(DictMember member) noexcept {
  return static_cast<duckdb::field_id_t>(kFieldFirstDict +
                                         static_cast<size_t>(member));
}

constexpr std::string_view kMemberTags[kDictMembers]{
  "string", "numeric", "false", "true", "sql_null", "json_null", "null_element",
};

constexpr const char* MemberTag(DictMember member) noexcept {
  return kMemberTags[static_cast<size_t>(member)].data();
}

constexpr duckdb::field_id_t kSlotRecord = 0;
constexpr duckdb::field_id_t kSlotRecordInline = 1;

constexpr duckdb::field_id_t kDictFeatures = 0;
constexpr duckdb::field_id_t kDictTermCount = 1;
constexpr duckdb::field_id_t kDictDocCount = 2;
constexpr duckdb::field_id_t kDictWidth = 3;
constexpr duckdb::field_id_t kDictMin = 4;
constexpr duckdb::field_id_t kDictRecord = 5;
constexpr duckdb::field_id_t kDictRecordInline = 6;
constexpr duckdb::field_id_t kDictMax = 7;
constexpr duckdb::field_id_t kDictFirstBlock = 8;
constexpr duckdb::field_id_t kDictFirstEntry = 9;
constexpr duckdb::field_id_t kDictFirstRecord = 10;
constexpr duckdb::field_id_t kDictLastBlock = 11;
constexpr duckdb::field_id_t kDictLastEntry = 12;
constexpr duckdb::field_id_t kDictInlineBase = 13;
constexpr duckdb::field_id_t kDictInlineSize = 14;
constexpr duckdb::field_id_t kDictText = 15;
constexpr duckdb::field_id_t kDictRanges = 16;
constexpr duckdb::field_id_t kDictSingleValued = 17;

constexpr duckdb::field_id_t kTextTotalFreq = 0;
constexpr duckdb::field_id_t kTextScoreBounds = 1;
constexpr duckdb::field_id_t kTextNorm = 2;
constexpr duckdb::field_id_t kTextPrefixes = 3;

constexpr duckdb::field_id_t kRangesOffset = 0;
constexpr duckdb::field_id_t kRangesSize = 1;
constexpr duckdb::field_id_t kRangesPrefix = 2;
constexpr duckdb::field_id_t kRangesCounts = 3;

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

bool Empty(const TextHead& text) noexcept {
  return text.total_freq == 0 && !text.has_score_bounds &&
         !field_limits::valid(text.norm) && text.prefixes.empty();
}

void WriteSlot(duckdb::BinarySerializer& out, const FieldHead& head) {
  SDB_ASSERT(head.term_count == 1 && head.features == IndexFeatures::None &&
             head.min.empty() && Empty(head.text) &&
             head.ranges.counts.empty());
  WriteBytes(out, kSlotRecord, "record", head.record);
  WriteOptionalBytes(out, kSlotRecordInline, "record_inline",
                     head.record_inline);
}

void WriteText(duckdb::BinarySerializer& out, const TextHead& text) {
  out.WritePropertyWithDefault<uint64_t>(kTextTotalFreq, "total_freq",
                                         text.total_freq, 0);
  out.WritePropertyWithDefault<bool>(kTextScoreBounds, "has_score_bounds",
                                     text.has_score_bounds, false);
  out.WritePropertyWithDefault<uint64_t>(kTextNorm, "norm", text.norm,
                                         field_limits::invalid());
  WriteOptionalBytes(out, kTextPrefixes, "prefixes", text.prefixes);
}

void WriteRanges(duckdb::BinarySerializer& out, const RangesHead& ranges) {
  out.WriteProperty<uint64_t>(kRangesOffset, "offset", ranges.offset);
  out.WriteProperty<uint64_t>(kRangesSize, "size", ranges.size);
  out.WritePropertyWithDefault<uint32_t>(kRangesPrefix, "prefix", ranges.prefix,
                                         0);
  out.WriteList(kRangesCounts, "counts", ranges.counts.size(),
                [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                  list.WriteElement(ranges.counts[i]);
                });
}

void WriteKeyed(duckdb::BinarySerializer& out, const FieldHead& head) {
  SDB_ASSERT(head.member == DictMember::Strings || Empty(head.text));
  SDB_ASSERT(head.member == DictMember::Numbers || head.ranges.counts.empty());
  out.WritePropertyWithDefault<uint32_t>(
    kDictFeatures, "features", static_cast<uint32_t>(head.features), 0);
  out.WritePropertyWithDefault<uint64_t>(kDictTermCount, "term_count",
                                         head.term_count, 1);
  if (head.term_count != 1) {
    out.WriteProperty<uint64_t>(kDictDocCount, "doc_count", head.doc_count);
  }
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
  if (!Empty(head.text)) {
    out.WriteObject(kDictText, "text", [&](duckdb::BinarySerializer& object) {
      WriteText(object, head.text);
    });
  }
  if (!head.ranges.counts.empty()) {
    SDB_ASSERT(IsSubsetOf(IndexFeatures::Ranges, head.features) &&
               head.width != 0);
    out.WriteObject(kDictRanges, "ranges",
                    [&](duckdb::BinarySerializer& object) {
                      WriteRanges(object, head.ranges);
                    });
  }
  if (head.term_count != 1) {
    out.WritePropertyWithDefault<bool>(kDictSingleValued, "single_valued",
                                       head.single_valued, false);
  }
}

void WriteField(duckdb::BinarySerializer& out,
                std::span<const FieldHead> dicts) {
  const auto layout = dicts.front().layout;
  out.WriteProperty<uint64_t>(kFieldId, "id", dicts.front().id);
  out.WritePropertyWithDefault<uint8_t>(
    kFieldLayout, "layout", static_cast<uint8_t>(layout),
    static_cast<uint8_t>(DictLayout::String));
  for (const auto& head : dicts) {
    SDB_ASSERT(head.layout == layout &&
               Contains({.layout = layout, .member = head.member}));
    out.WriteObject(DictProperty(head.member), MemberTag(head.member),
                    [&](duckdb::BinarySerializer& object) {
                      if (IsTermMember(head.member)) {
                        WriteKeyed(object, head);
                      } else {
                        WriteSlot(object, head);
                      }
                    });
  }
}

void ReadSlot(duckdb::BinaryDeserializer& in, FieldHead& head) {
  head.term_count = 1;
  head.record = in.ReadProperty<std::string>(kSlotRecord, "record");
  head.record_inline =
    in.ReadPropertyWithDefault<std::string>(kSlotRecordInline, "record_inline");
  if (head.record.empty()) {
    Corrupted("record", head.id);
  }
}

void ReadText(duckdb::BinaryDeserializer& in, TextHead& text) {
  text.total_freq = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kTextTotalFreq, "total_freq", 0);
  text.has_score_bounds = in.ReadPropertyWithExplicitDefault<bool>(
    kTextScoreBounds, "has_score_bounds", false);
  text.norm = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kTextNorm, "norm", field_limits::invalid());
  text.prefixes =
    in.ReadPropertyWithDefault<std::string>(kTextPrefixes, "prefixes");
}

void ReadRanges(duckdb::BinaryDeserializer& in, RangesHead& ranges) {
  ranges.offset = in.ReadProperty<uint64_t>(kRangesOffset, "offset");
  ranges.size = in.ReadProperty<uint64_t>(kRangesSize, "size");
  ranges.prefix =
    in.ReadPropertyWithExplicitDefault<uint32_t>(kRangesPrefix, "prefix", 0);
  in.ReadList(kRangesCounts, "counts",
              [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
                ranges.counts.push_back(list.ReadElement<uint64_t>());
              });
}

void ReadKeyed(duckdb::BinaryDeserializer& in, FieldHead& head) {
  const auto features =
    in.ReadPropertyWithExplicitDefault<uint32_t>(kDictFeatures, "features", 0);
  if ((features & ~static_cast<uint32_t>(IndexFeatures::Max)) != 0) {
    Unsupported("index features", features);
  }
  head.features = static_cast<IndexFeatures>(features);
  head.term_count = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kDictTermCount, "term_count", 1);
  if (head.term_count == 0) {
    Corrupted("term count", head.id);
  }
  if (head.term_count != 1) {
    head.doc_count = in.ReadProperty<uint64_t>(kDictDocCount, "doc_count");
  }
  head.width =
    in.ReadPropertyWithExplicitDefault<uint32_t>(kDictWidth, "width", 0);
  head.min = in.ReadPropertyWithDefault<std::string>(kDictMin, "min");
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
  if (head.member == DictMember::Strings) {
    in.ReadOptionalObject(
      kDictText, "text",
      [&](duckdb::BinaryDeserializer& object) { ReadText(object, head.text); });
  } else {
    in.ReadOptionalObject(kDictRanges, "ranges",
                          [&](duckdb::BinaryDeserializer& object) {
                            ReadRanges(object, head.ranges);
                          });
  }
  head.single_valued =
    head.term_count == 1 || in.ReadPropertyWithExplicitDefault<bool>(
                              kDictSingleValued, "single_valued", false);
  const bool ranged =
    IsSubsetOf(IndexFeatures::Ranges, head.features) && head.width != 0;
  const bool has_ranges = !head.ranges.counts.empty() || head.ranges.size != 0;
  if ((has_ranges && (!ranged || head.term_count == 1)) ||
      (head.term_count == 1 && !head.text.prefixes.empty()) ||
      (field_limits::valid(head.text.norm) &&
       !IsSubsetOf(IndexFeatures::Norm, head.features))) {
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
  const auto first = fields.size();
  for (const auto member : LayoutMembers(static_cast<DictLayout>(layout))) {
    in.ReadOptionalObject(DictProperty(member), MemberTag(member),
                          [&](duckdb::BinaryDeserializer& object) {
                            auto& head = fields.emplace_back();
                            head.id = id;
                            head.layout = static_cast<DictLayout>(layout);
                            head.member = member;
                            if (IsTermMember(member)) {
                              ReadKeyed(object, head);
                            } else {
                              ReadSlot(object, head);
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
  out.WriteList(kTermsFields, "fields", fields,
                [&, at = size_t{0}](duckdb::BinarySerializer::List& list,
                                    duckdb::idx_t) mutable {
                  auto end = at + 1;
                  while (end != heads.size() && heads[end].id == heads[at].id) {
                    SDB_ASSERT(heads[end - 1].member < heads[end].member);
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
    terms.fields, [](const FieldHead& head) { return head.term_count > 1; });
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
