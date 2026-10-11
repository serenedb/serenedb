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

#include "index/merge/support/segment_gen.hpp"

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <memory>

#include "index/doc_generator.hpp"
#include "insert_field.hpp"

namespace tests::merge {
namespace {

using irs::IndexFeatures;

constexpr std::string_view kWords[] = {
  "alpha", "bravo", "charlie", "delta", "echo",   "foxtrot", "golf",
  "hotel", "india", "juliet",  "kilo",  "lima",   "mike",    "november",
  "oscar", "papa",  "quebec",  "romeo", "sierra", "tango",   "uniform"};

constexpr IndexFeatures kMixedFeatures[] = {
  IndexFeatures::None,
  IndexFeatures::Freq,
  IndexFeatures::Freq | IndexFeatures::Pos,
  IndexFeatures::Freq | IndexFeatures::Pos | IndexFeatures::Offs,
  IndexFeatures::Freq | IndexFeatures::Norm,
};

}  // namespace

std::span<const std::string_view> Words() { return kWords; }

std::vector<duckdb::LogicalType> ColumnTypes() {
  return {duckdb::LogicalType::BIGINT,
          duckdb::LogicalType::DOUBLE,
          duckdb::LogicalType::VARCHAR,
          duckdb::LogicalType::BOOLEAN,
          duckdb::LogicalType::BLOB,
          duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER),
          duckdb::LogicalType::STRUCT({{"a", duckdb::LogicalType::INTEGER},
                                       {"b", duckdb::LogicalType::VARCHAR}})};
}

size_t Generator::Uniform(size_t lo, size_t hi) {
  return std::uniform_int_distribution<size_t>{lo, hi}(_rng);
}

bool Generator::Coin(double p) { return std::bernoulli_distribution{p}(_rng); }

std::string Generator::Text(size_t max_words) {
  std::string out;
  const auto n = Uniform(1, max_words);
  for (size_t i = 0; i != n; ++i) {
    if (i != 0) {
      out.push_back(' ');
    }
    out += kWords[Uniform(0, std::size(kWords) - 1)];
  }
  return out;
}

duckdb::Value Generator::Value(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::BIGINT:
      return duckdb::Value::BIGINT(static_cast<int64_t>(Uniform(0, 1 << 20)) -
                                   (1 << 19));
    case duckdb::LogicalTypeId::DOUBLE:
      return duckdb::Value::DOUBLE(static_cast<double>(Uniform(0, 1000000)) /
                                   997.0);
    case duckdb::LogicalTypeId::VARCHAR:
      return duckdb::Value{Coin(0.05) ? std::string(4100 + Uniform(0, 64), 'x')
                                      : Text(6)};
    case duckdb::LogicalTypeId::BOOLEAN:
      return duckdb::Value::BOOLEAN(Coin(0.5));
    case duckdb::LogicalTypeId::BLOB: {
      std::string bytes(Uniform(0, 24), '\0');
      for (auto& c : bytes) {
        c = static_cast<char>(Uniform(0, 255));
      }
      return duckdb::Value::BLOB_RAW(bytes);
    }
    case duckdb::LogicalTypeId::LIST: {
      duckdb::vector<duckdb::Value> items;
      const auto n = Uniform(0, 4);
      for (size_t i = 0; i != n; ++i) {
        items.push_back(
          duckdb::Value::INTEGER(static_cast<int32_t>(Uniform(0, 100))));
      }
      return duckdb::Value::LIST(duckdb::LogicalType::INTEGER,
                                 std::move(items));
    }
    case duckdb::LogicalTypeId::STRUCT:
      return duckdb::Value::STRUCT(
        {{"a", duckdb::Value::INTEGER(static_cast<int32_t>(Uniform(0, 9)))},
         {"b", duckdb::Value{Text(2)}}});
    default:
      return duckdb::Value{type};
  }
}

void WriteValue(irs::IndexWriter::Document& doc, irs::field_id id,
                const duckdb::LogicalType& type, const duckdb::Value& value) {
  auto& cw = doc.GetColWriter()->OpenColumn(id, type);
  duckdb::Vector v{type, 1};
  v.SetValue(0, value);
  duckdb::FlatVector::SetSize(v, 1);
  cw.Append(static_cast<uint64_t>(doc.DocId()) - irs::doc_limits::min(), v, 1);
}

void RemoveTerms(irs::IndexWriter& writer, irs::field_id field,
                 std::span<const std::string> terms) {
  if (terms.empty()) {
    return;
  }
  auto trx = writer.GetBatch();
  for (const auto& term : terms) {
    auto filter = std::make_unique<irs::ByTerm>();
    *filter->mutable_field_id() = field;
    filter->mutable_options()->term =
      irs::bstring{irs::ViewCast<irs::byte_type>(std::string_view{term})};
    trx.Remove(irs::Filter::ptr{std::move(filter)});
  }
  trx.Commit();
  ASSERT_TRUE(writer.RefreshCommit());
}

void WriteSegment(irs::IndexWriter& writer, Generator& gen,
                  const SegmentShape& shape, size_t& next_id) {
  const auto types = ColumnTypes();
  std::vector<std::string> keys;
  {
    auto trx = writer.GetBatch();
    for (size_t i = 0; i != shape.docs; ++i) {
      auto doc = trx.Insert();
      const auto& key = keys.emplace_back(absl::StrCat("doc-", next_id++));
      tests::StringField id_field{"id", key};
      id_field.id = kIdField;
      tests::InsertField(doc, id_field);
      tests::TextField<std::string> text{"text", gen.Text(12)};
      text.id = kTextField;
      tests::InsertField(doc, text);
      if (shape.has_tag && gen.Coin(0.7)) {
        tests::StringField tag{"tag", absl::StrCat("tag", gen.Uniform(0, 5))};
        tag.id = kTagField;
        tests::InsertField(doc, tag);
      }
      if (shape.has_norm_field) {
        tests::TextField<std::string> normed{"normed", gen.Text(30), false,
                                             IndexFeatures::Norm};
        normed.id = kNormField;
        tests::InsertField(doc, normed);
      }
      tests::StringField mixed{"mixed", kWords[gen.Uniform(0, 4)]};
      mixed.id = kMixedField;
      mixed.index_features = shape.mixed;
      tests::InsertField(doc, mixed);
      for (const auto c : shape.columns) {
        if (gen.Coin(shape.null_ratio)) {
          continue;
        }
        WriteValue(doc, static_cast<irs::field_id>(kFirstColumn + c), types[c],
                   gen.Value(types[c]));
      }
    }
    trx.Commit();
  }
  ASSERT_TRUE(writer.RefreshCommit());
  std::vector<std::string> removed;
  for (const auto& key : keys) {
    if (gen.Coin(shape.delete_ratio)) {
      removed.push_back(key);
    }
  }
  RemoveTerms(writer, kIdField, removed);
}

SegmentShape RandomShape(Generator& gen) {
  SegmentShape shape;
  shape.docs = gen.Uniform(1, 400);
  shape.delete_ratio = gen.Coin(0.5) ? 0.0 : gen.Uniform(1, 9) / 10.0;
  shape.null_ratio = gen.Uniform(0, 5) / 10.0;
  for (size_t c = 0; c != ColumnTypes().size(); ++c) {
    if (gen.Coin(0.75)) {
      shape.columns.push_back(c);
    }
  }
  shape.mixed = kMixedFeatures[gen.Uniform(0, std::size(kMixedFeatures) - 1)];
  shape.has_tag = gen.Coin(0.8);
  shape.has_norm_field = gen.Coin(0.8);
  return shape;
}

irs::ColumnOptions SketchedColumns(irs::field_id id) {
  return irs::ColumnOptions{.hyperloglog =
                              id == kFirstColumn || id == kFirstColumn + 2};
}

std::vector<const irs::SubReader*> Segments(
  const irs::DirectoryReader& reader) {
  std::vector<const irs::SubReader*> out;
  for (const auto& segment : reader) {
    out.push_back(&segment);
  }
  return out;
}

}  // namespace tests::merge
