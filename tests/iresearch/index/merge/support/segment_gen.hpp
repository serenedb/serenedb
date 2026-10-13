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

#pragma once

#include <cstddef>
#include <cstdint>
#include <duckdb/common/types/value.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tests::merge {

inline constexpr irs::field_id kIdField = 1;
inline constexpr irs::field_id kTextField = 2;
inline constexpr irs::field_id kTagField = 3;
inline constexpr irs::field_id kNormField = 4;
inline constexpr irs::field_id kMixedField = 5;
inline constexpr irs::field_id kFirstColumn = 100;

std::span<const std::string_view> Words();

std::vector<duckdb::LogicalType> ColumnTypes();

class Generator {
 public:
  explicit Generator(uint64_t seed) : _rng{seed} {}

  size_t Uniform(size_t lo, size_t hi);

  bool Coin(double p);

  std::string Text(size_t max_words);

  duckdb::Value Value(const duckdb::LogicalType& type);

 private:
  std::mt19937_64 _rng;
};

struct SegmentShape {
  size_t docs = 0;
  double delete_ratio = 0;
  double null_ratio = 0;
  std::vector<size_t> columns;
  irs::IndexFeatures mixed = irs::IndexFeatures::None;
  bool has_tag = true;
  bool has_norm_field = true;
};

void WriteValue(irs::IndexWriter::Document& doc, irs::field_id id,
                const duckdb::LogicalType& type, const duckdb::Value& value);

void RemoveTerms(irs::IndexWriter& writer, irs::field_id field,
                 std::span<const std::string> terms);

void WriteSegment(irs::IndexWriter& writer, Generator& gen,
                  const SegmentShape& shape, size_t& next_id);

SegmentShape RandomShape(Generator& gen);

irs::ColumnOptions SketchedColumns(irs::field_id id);

std::vector<const irs::SubReader*> Segments(const irs::DirectoryReader& reader);

class WithoutColumnStore final : public irs::SubReader {
 public:
  explicit WithoutColumnStore(const irs::SubReader& segment) noexcept
    : _segment{segment} {}

  uint64_t CountMappedMemory() const final { return 0; }

  const irs::SegmentInfo& Meta() const final { return _segment.Meta(); }

  const irs::DocumentMask* docs_mask() const final {
    return _segment.docs_mask();
  }

  irs::lead::Node::ptr docs_iterator() const final {
    return _segment.docs_iterator();
  }

  std::span<const irs::field_id> field_ids() const final {
    return _segment.field_ids();
  }

  const irs::TermReader* field(irs::field_id id) const final {
    return _segment.field(id);
  }

  irs::NormReader::ptr norms(irs::field_id) const final { return {}; }

 private:
  const irs::SubReader& _segment;
};

}  // namespace tests::merge
