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

#include "index/merge/support/segment_dump.hpp"

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <duckdb/common/types/hyperloglog.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/norm_column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/formats/term_reader.hpp>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <map>
#include <numeric>
#include <set>

#include "index/merge/support/segment_gen.hpp"
#include "postings_cursor.hpp"

namespace tests::merge {
namespace {

using irs::IndexFeatures;

std::vector<irs::doc_id_t> Renumber(const irs::SubReader& segment) {
  const auto end =
    static_cast<irs::doc_id_t>(segment.docs_count() + irs::doc_limits::min());
  std::vector<irs::doc_id_t> out(end, irs::doc_limits::invalid());
  auto mask = segment.MaskedDocs();
  auto next = irs::doc_limits::min();
  for (auto doc = irs::doc_limits::min(); doc < end; ++doc) {
    if (!mask.Contains(doc)) {
      out[doc] = next++;
    }
  }
  return out;
}

std::vector<uint32_t> DumpNorms(const irs::SubReader& segment,
                                const irs::FieldMeta& meta,
                                std::span<const irs::doc_id_t> renumber) {
  std::vector<uint32_t> out;
  if (!irs::IsSubsetOf(IndexFeatures::Norm, meta.index_features) ||
      !irs::field_limits::valid(meta.norm)) {
    return out;
  }
  const auto* cs = segment.GetColReader();
  const auto* norms = cs != nullptr ? cs->NormColumn(meta.norm) : nullptr;
  if (norms == nullptr) {
    return out;
  }
  std::vector<uint32_t> values;
  for (size_t r = 0; r != norms->RegionCount(); ++r) {
    const auto& region = norms->Region(r);
    values.resize(region.end_doc - region.first_doc);
    norms->Decode(region.first_doc, values.size(), values.data());
    for (size_t i = 0; i != values.size(); ++i) {
      const auto doc = static_cast<irs::doc_id_t>(region.first_doc + i);
      if (doc < renumber.size() && irs::doc_limits::valid(renumber[doc])) {
        out.push_back(values[i]);
      }
    }
  }
  return out;
}

DumpField DumpOneField(const irs::SubReader& segment,
                       const irs::TermReader& reader,
                       std::span<const irs::doc_id_t> renumber) {
  const auto& meta = reader.meta();
  DumpField out{.id = meta.id, .features = meta.index_features};
  std::set<irs::doc_id_t> docs;
  for (auto it = reader.iterator(); it->next();) {
    DumpTerm term{.value = irs::bstring{it->value()}};
    PostingsCursor cursor{it->postings(meta.index_features),
                          meta.index_features, segment.MaskedDocs()};
    for (auto doc = cursor.Next(); !irs::doc_limits::eof(doc);
         doc = cursor.Next()) {
      DumpPosting posting{.doc = renumber[doc], .freq = cursor.GetFreq()};
      if (auto* pos = cursor.Positions(); pos != nullptr) {
        const auto* offs = irs::get<irs::OffsAttr>(*pos);
        while (pos->next()) {
          posting.positions.push_back(pos->value());
          if (offs != nullptr) {
            posting.offsets.emplace_back(offs->start, offs->end);
          }
        }
      }
      docs.insert(posting.doc);
      term.postings.push_back(std::move(posting));
    }
    if (!term.postings.empty()) {
      out.terms.push_back(std::move(term));
    }
  }
  out.docs = docs.size();
  out.norms = DumpNorms(segment, meta, renumber);
  return out;
}

std::string ValueString(const duckdb::Value& value) {
  return value.IsNull() ? std::string{"NULL"} : value.ToString();
}

DumpColumn DumpOneColumn(const irs::SubReader& segment,
                         const irs::ColReader& cs, const irs::ColumnReader& col,
                         std::span<const irs::doc_id_t> renumber) {
  DumpColumn out{.id = col.Id(), .type = col.Type().ToString()};
  irs::ReadContext ctx{cs};
  auto state = col.InitScan(ctx);
  const auto* stored = col.HyperLogLog();
  duckdb::HyperLogLog rebuilt;
  duckdb::Vector hashes{duckdb::LogicalType::HASH, STANDARD_VECTOR_SIZE};
  for (uint64_t row = 0; row < col.RowCount();) {
    const auto take =
      std::min<uint64_t>(col.RowCount() - row, STANDARD_VECTOR_SIZE);
    duckdb::Vector vec{col.Type(), STANDARD_VECTOR_SIZE};
    col.Scan(state, vec, take);
    if (stored != nullptr) {
      duckdb::VectorOperations::Hash(vec, hashes, take);
      rebuilt.Update(vec, hashes);
    }
    for (uint64_t k = 0; k != take; ++k) {
      const auto doc = row + k + irs::doc_limits::min();
      if (doc < renumber.size() && irs::doc_limits::valid(renumber[doc])) {
        out.values.push_back(ValueString(vec.GetValue(k)));
      }
    }
    row += take;
  }
  for (auto doc = col.RowCount() + irs::doc_limits::min();
       doc < renumber.size(); ++doc) {
    if (irs::doc_limits::valid(renumber[doc])) {
      out.values.emplace_back("NULL");
    }
  }
  if (stored != nullptr) {
    out.sketch_matches_values = stored->Count() == rebuilt.Count();
  }
  return out;
}

void AppendPostings(const DumpTerm& term, IndexFeatures features,
                    irs::doc_id_t base, std::vector<DumpPosting>& out) {
  const bool freq = irs::IsSubsetOf(IndexFeatures::Freq, features);
  const bool pos =
    irs::IsSubsetOf(IndexFeatures::Freq | IndexFeatures::Pos, features);
  const bool offs = pos && irs::IsSubsetOf(IndexFeatures::Offs, features);
  for (auto posting : term.postings) {
    posting.doc += base;
    if (!freq) {
      posting.freq = 0;
    }
    if (!pos) {
      posting.positions.clear();
    }
    if (!offs) {
      posting.offsets.clear();
    }
    out.push_back(std::move(posting));
  }
}

SegmentDump Permute(SegmentDump dump, std::span<const size_t> order) {
  std::vector<irs::doc_id_t> renumbered(order.size());
  for (size_t row = 0; row != order.size(); ++row) {
    renumbered[order[row]] =
      static_cast<irs::doc_id_t>(row + irs::doc_limits::min());
  }
  for (auto& field : dump.fields) {
    for (auto& term : field.terms) {
      for (auto& posting : term.postings) {
        posting.doc = renumbered[posting.doc - irs::doc_limits::min()];
      }
      std::ranges::sort(term.postings, {}, &DumpPosting::doc);
    }
    if (!field.norms.empty()) {
      const auto norms = field.norms;
      for (size_t row = 0; row != order.size(); ++row) {
        field.norms[row] = norms[order[row]];
      }
    }
  }
  for (auto& column : dump.columns) {
    const auto values = column.values;
    for (size_t row = 0; row != order.size(); ++row) {
      column.values[row] = values[order[row]];
    }
  }
  return dump;
}

std::string Escape(irs::bytes_view value) {
  return absl::CHexEscape(irs::ViewCast<char>(value));
}

std::string Describe(const DumpPosting& posting) {
  std::string out = absl::StrCat(posting.doc, " f", posting.freq);
  if (!posting.positions.empty()) {
    absl::StrAppend(&out, " p[", absl::StrJoin(posting.positions, ","), "]");
  }
  if (!posting.offsets.empty()) {
    absl::StrAppend(&out, " o[",
                    absl::StrJoin(posting.offsets, ",",
                                  [](std::string* s, const auto& o) {
                                    absl::StrAppend(s, o.first, "-", o.second);
                                  }),
                    "]");
  }
  return out;
}

template<typename T, typename Fn>
std::string FirstOf(std::string_view what, const std::vector<T>& expected,
                    const std::vector<T>& actual, Fn&& describe) {
  const auto n = std::min(expected.size(), actual.size());
  for (size_t i = 0; i != n; ++i) {
    if (!(expected[i] == actual[i])) {
      return absl::StrCat(what, " #", i, ": expected ", describe(expected[i]),
                          ", actual ", describe(actual[i]));
    }
  }
  if (expected.size() != actual.size()) {
    return absl::StrCat(what, ": expected ", expected.size(),
                        " entries, actual ", actual.size());
  }
  return {};
}

}  // namespace

std::vector<duckdb::Value> LiveValues(const irs::SubReader& segment,
                                      irs::field_id id) {
  const auto renumber = Renumber(segment);
  std::vector<duckdb::Value> out;
  const auto* cs = segment.GetColReader();
  const auto* col = cs != nullptr ? cs->Column(id) : nullptr;
  uint64_t rows = 0;
  if (col != nullptr) {
    irs::ReadContext ctx{*cs};
    auto state = col->InitScan(ctx);
    rows = col->RowCount();
    for (uint64_t row = 0; row < rows;) {
      const auto take = std::min<uint64_t>(rows - row, STANDARD_VECTOR_SIZE);
      duckdb::Vector vec{col->Type(), STANDARD_VECTOR_SIZE};
      col->Scan(state, vec, take);
      for (uint64_t k = 0; k != take; ++k) {
        const auto doc = row + k + irs::doc_limits::min();
        if (doc < renumber.size() && irs::doc_limits::valid(renumber[doc])) {
          out.push_back(vec.GetValue(k));
        }
      }
      row += take;
    }
  }
  for (auto doc = rows + irs::doc_limits::min(); doc < renumber.size(); ++doc) {
    if (irs::doc_limits::valid(renumber[doc])) {
      out.emplace_back();
    }
  }
  return out;
}

SegmentDump Dump(const irs::SubReader& segment) {
  const auto renumber = Renumber(segment);
  SegmentDump out{.docs = segment.live_docs_count()};
  for (const auto id : segment.field_ids()) {
    if (const auto* reader = segment.field(id); reader != nullptr) {
      auto field = DumpOneField(segment, *reader, renumber);
      if (!field.terms.empty()) {
        out.fields.push_back(std::move(field));
      }
    }
  }
  std::ranges::sort(out.fields, {}, &DumpField::id);
  if (const auto* cs = segment.GetColReader(); cs != nullptr) {
    for (const auto& col : cs->Columns()) {
      out.columns.push_back(DumpOneColumn(segment, *cs, *col, renumber));
    }
  }
  std::ranges::sort(out.columns, {}, &DumpColumn::id);
  return out;
}

SegmentDump Concat(std::span<const SegmentDump> inputs) {
  std::map<irs::field_id, IndexFeatures> features;
  std::map<irs::field_id, std::string> types;
  for (const auto& in : inputs) {
    for (const auto& field : in.fields) {
      const auto [it, fresh] = features.emplace(field.id, field.features);
      if (!fresh) {
        it->second &= field.features;
      }
    }
    for (const auto& column : in.columns) {
      types.emplace(column.id, column.type);
    }
  }

  std::map<irs::field_id, std::map<irs::bstring, std::vector<DumpPosting>>>
    terms;
  std::map<irs::field_id, std::vector<uint32_t>> norms;
  std::set<irs::field_id> with_norms;
  std::map<irs::field_id, std::vector<std::string>> values;
  irs::doc_id_t base = 0;
  for (const auto& in : inputs) {
    for (const auto& [id, merged] : features) {
      const auto it = std::ranges::find(in.fields, id, &DumpField::id);
      const DumpField* field = it != in.fields.end() ? &*it : nullptr;
      if (field != nullptr) {
        for (const auto& term : field->terms) {
          AppendPostings(term, merged, base, terms[id][term.value]);
        }
      }
      if (!irs::IsSubsetOf(IndexFeatures::Norm, merged)) {
        continue;
      }
      auto& out = norms[id];
      if (field != nullptr && !field->norms.empty()) {
        with_norms.insert(id);
        out.insert(out.end(), field->norms.begin(), field->norms.end());
      } else {
        out.insert(out.end(), in.docs, 0);
      }
    }
    for (const auto& [id, type] : types) {
      auto& out = values[id];
      const auto it = std::ranges::find(in.columns, id, &DumpColumn::id);
      if (it != in.columns.end()) {
        out.insert(out.end(), it->values.begin(), it->values.end());
      } else {
        out.insert(out.end(), in.docs, "NULL");
      }
    }
    base += static_cast<irs::doc_id_t>(in.docs);
  }

  SegmentDump out{.docs = base};
  for (const auto& [id, merged] : features) {
    DumpField field{.id = id, .features = merged};
    std::set<irs::doc_id_t> docs;
    for (auto& [value, postings] : terms[id]) {
      for (const auto& posting : postings) {
        docs.insert(posting.doc);
      }
      field.terms.push_back(DumpTerm{.value = value, .postings = postings});
    }
    field.docs = docs.size();
    if (with_norms.contains(id)) {
      field.norms = std::move(norms[id]);
    }
    if (!field.terms.empty()) {
      out.fields.push_back(std::move(field));
    }
  }
  for (const auto& [id, type] : types) {
    out.columns.push_back(
      DumpColumn{.id = id, .type = type, .values = std::move(values[id])});
  }
  return out;
}

SegmentDump ExpectedMerge(std::span<const irs::SubReader* const> inputs) {
  std::vector<SegmentDump> dumps;
  dumps.reserve(inputs.size());
  for (const auto* input : inputs) {
    dumps.push_back(Dump(*input));
  }
  return Concat(dumps);
}

SegmentDump ExpectedSortedMerge(std::span<const irs::SubReader* const> inputs,
                                const irs::SortSpec& spec) {
  auto expected = ExpectedMerge(inputs);
  std::vector<std::vector<duckdb::Value>> keys(spec.keys.size());
  for (const auto* input : inputs) {
    for (size_t k = 0; k != spec.keys.size(); ++k) {
      auto values = LiveValues(*input, spec.keys[k].column);
      keys[k].insert(keys[k].end(), std::make_move_iterator(values.begin()),
                     std::make_move_iterator(values.end()));
    }
  }
  std::vector<size_t> order(expected.docs);
  std::iota(order.begin(), order.end(), size_t{0});
  std::ranges::stable_sort(order, [&](size_t lhs, size_t rhs) {
    for (size_t k = 0; k != spec.keys.size(); ++k) {
      const auto cmp =
        CompareKeyValues(keys[k][lhs], keys[k][rhs], spec.keys[k]);
      if (cmp != 0) {
        return cmp < 0;
      }
    }
    return false;
  });
  return Permute(std::move(expected), order);
}

std::string Describe(const SegmentDump& dump) {
  std::string out = absl::StrCat("docs ", dump.docs, "\n");
  for (const auto& field : dump.fields) {
    absl::StrAppend(&out, "field ", field.id, " features ",
                    static_cast<uint32_t>(field.features), " docs ", field.docs,
                    " terms ", field.terms.size(), "\n");
    for (const auto& term : field.terms) {
      absl::StrAppend(&out, "  term \"", Escape(term.value), "\"\n");
      for (const auto& posting : term.postings) {
        absl::StrAppend(&out, "    ", Describe(posting), "\n");
      }
    }
    if (!field.norms.empty()) {
      absl::StrAppend(&out, "  norms ", absl::StrJoin(field.norms, " "), "\n");
    }
  }
  for (const auto& column : dump.columns) {
    absl::StrAppend(&out, "column ", column.id, " ", column.type, "\n");
    for (const auto& value : column.values) {
      absl::StrAppend(&out, "  ", absl::CHexEscape(value), "\n");
    }
  }
  return out;
}

std::string FirstDifference(const SegmentDump& expected,
                            const SegmentDump& actual) {
  if (expected.docs != actual.docs) {
    return absl::StrCat("docs: expected ", expected.docs, ", actual ",
                        actual.docs);
  }
  auto ids = [](const auto& items) {
    std::vector<irs::field_id> out;
    for (const auto& item : items) {
      out.push_back(item.id);
    }
    return out;
  };
  if (auto diff = FirstOf("field id", ids(expected.fields), ids(actual.fields),
                          [](irs::field_id id) { return absl::StrCat(id); });
      !diff.empty()) {
    return diff;
  }
  for (size_t f = 0; f != expected.fields.size(); ++f) {
    const auto& e = expected.fields[f];
    const auto& a = actual.fields[f];
    const auto where = absl::StrCat("field ", e.id);
    if (e.features != a.features) {
      return absl::StrCat(where, " features: expected ",
                          static_cast<uint32_t>(e.features), ", actual ",
                          static_cast<uint32_t>(a.features));
    }
    if (e.docs != a.docs) {
      return absl::StrCat(where, " docs: expected ", e.docs, ", actual ",
                          a.docs);
    }
    std::vector<irs::bstring> e_terms;
    std::vector<irs::bstring> a_terms;
    for (const auto& t : e.terms) {
      e_terms.push_back(t.value);
    }
    for (const auto& t : a.terms) {
      a_terms.push_back(t.value);
    }
    if (auto diff = FirstOf(absl::StrCat(where, " term"), e_terms, a_terms,
                            [](const irs::bstring& v) {
                              return absl::StrCat("\"", Escape(v), "\"");
                            });
        !diff.empty()) {
      return diff;
    }
    for (size_t t = 0; t != e.terms.size(); ++t) {
      if (auto diff =
            FirstOf(absl::StrCat(where, " term \"", Escape(e.terms[t].value),
                                 "\" posting"),
                    e.terms[t].postings, a.terms[t].postings,
                    [](const DumpPosting& p) { return Describe(p); });
          !diff.empty()) {
        return diff;
      }
    }
    if (auto diff =
          FirstOf(absl::StrCat(where, " norm of doc"), e.norms, a.norms,
                  [](uint32_t v) { return absl::StrCat(v); });
        !diff.empty()) {
      return diff;
    }
  }
  if (auto diff =
        FirstOf("column id", ids(expected.columns), ids(actual.columns),
                [](irs::field_id id) { return absl::StrCat(id); });
      !diff.empty()) {
    return diff;
  }
  for (size_t c = 0; c != expected.columns.size(); ++c) {
    const auto& e = expected.columns[c];
    const auto& a = actual.columns[c];
    if (e.type != a.type) {
      return absl::StrCat("column ", e.id, " type: expected ", e.type,
                          ", actual ", a.type);
    }
    if (e.sketch_matches_values != a.sketch_matches_values) {
      return absl::StrCat("column ", e.id,
                          ": stored distinct-count sketch does not match the "
                          "sketch of its values");
    }
    if (auto diff = FirstOf(absl::StrCat("column ", e.id, " row"), e.values,
                            a.values, [](const std::string& v) { return v; });
        !diff.empty()) {
      return diff;
    }
  }
  return {};
}

}  // namespace tests::merge
