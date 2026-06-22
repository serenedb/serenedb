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

#include <duckdb/common/enums/compression_type.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "catalog/index.h"
#include "catalog/persistence/inverted_index.h"
#include "catalog/scorer_options.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/tokenizer.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search
namespace sdb::catalog {
namespace term_dict {

inline constexpr irs::field_id kPKFieldId =
  static_cast<irs::field_id>(Column::kGeneratedPKId.id());

enum class Kind : uint8_t {
  Unsupported,
  Null,
  String,
  Bool,
  NumericI32,
  NumericI64,
  NumericF32,
  NumericF64,
};

constexpr Kind Classify(duckdb::LogicalTypeId id) noexcept {
  using enum duckdb::LogicalTypeId;
  using enum Kind;
  switch (id) {
    case SQLNULL:
      return Null;
    case VARCHAR:
    case CHAR:
    case BLOB:
    case GEOMETRY:
      return String;
    case BOOLEAN:
      return Bool;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case UTINYINT:
    case USMALLINT:
    case DATE:
      return NumericI32;
    case BIGINT:
    case UINTEGER:
    case TIME:
    case TIME_NS:
    case TIME_TZ:
    case TIMESTAMP_SEC:
    case TIMESTAMP_MS:
    case TIMESTAMP:
    case TIMESTAMP_NS:
    case TIMESTAMP_TZ:
    case TIMESTAMP_TZ_NS:
      return NumericI64;
    case FLOAT:
      return NumericF32;
    case DOUBLE:
      return NumericF64;
    default:
      return Unsupported;
  }
}

constexpr bool IsNumeric(Kind k) noexcept { return k >= Kind::NumericI32; }

constexpr bool IsSupported(Kind k) noexcept { return k != Kind::Unsupported; }

Result Validate(std::string_view label, const duckdb::LogicalType& type,
                std::string_view opclass);

}  // namespace term_dict
namespace included {

Result Validate(std::string_view label, const duckdb::LogicalType& type);

}  // namespace included
namespace hnsw {

uint32_t Dimension(const duckdb::LogicalType& type) noexcept;

Result Validate(std::string_view label, const duckdb::LogicalType& type);

}  // namespace hnsw

using persistence::ExpressionKey;
using persistence::HNSWColumnConfig;

struct InvertedIndexEntryInfo {
  ObjectId text_dictionary = ObjectId::none();
  search::Features features;
  irs::field_id synthetic_column = irs::field_limits::invalid();
  uint32_t norm_row_group_size = 0;
  bool store_values = false;
  bool indexed_term_dict = false;
  bool hyperloglog = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  std::optional<HNSWColumnConfig> hnsw_config;
  uint32_t row_group_size = 0;

  irs::field_id null_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id numeric_field_id = irs::field_limits::invalid();

  bool IsHNSW() const noexcept { return hnsw_config.has_value(); }
  bool HasTextDictionary() const noexcept { return text_dictionary.isSet(); }
  bool HasJsonLeafFields() const noexcept {
    return irs::field_limits::valid(numeric_field_id) &&
           irs::field_limits::valid(bool_field_id);
  }
  bool IsTermDict() const noexcept {
    return !IsHNSW() && (indexed_term_dict || HasTextDictionary());
  }
  bool IsStored() const noexcept { return store_values || IsHNSW(); }
};

struct ColumnTokenizer {
  Tokenizer::TokenizerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
  irs::field_id tokenizer_column = irs::field_limits::invalid();
};

class InvertedIndex final : public Index {
 public:
  using Entries =
    containers::NodeHashMap<irs::field_id, InvertedIndexEntryInfo>;

  // `columns` are the de-duped plain-column keys (each key's field_id is its
  // column id). `expression_keys` carry each expression's payload + its
  // allocated field_id as one unit. `entries` is the per-field config keyed by
  // field_id.
  InvertedIndex(ObjectId database_id, ObjectId schema_id, ObjectId id,
                ObjectId relation_id, std::string name,
                std::vector<Column::Id> columns,
                std::vector<ExpressionKey> expression_keys, Entries entries,
                InvertedIndexOptions options)
    : Index{database_id,
            schema_id,
            id,
            relation_id,
            std::move(name),
            DeriveFromKeys(columns, expression_keys),
            ObjectType::InvertedIndex},
      _entries{std::move(entries)},
      _expression_keys{std::move(expression_keys)},
      _options{std::move(options)} {
    BuildExprByFieldIdIndex();
    BuildSerializedExprIndex();
    BuildFieldLookupIndex();
    BumpTickServerForEntryIds();
  }

  static std::shared_ptr<InvertedIndex> Deserialize(duckdb::Deserializer& src,
                                                    ReadContext ctx);
  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  const InvertedIndexEntryInfo* FindEntry(irs::field_id id) const noexcept;
  // Convenience: returns the entry only if it is a plain column (not an
  // indexed expression). Use when the caller knows column id semantics.
  const InvertedIndexEntryInfo* FindColumnInfo(
    catalog::Column::Id column_id) const noexcept;

  // The expression key owning `field_id`, or nullptr if `field_id` is a plain
  // column key (or unknown). Pointer is stable for the index's lifetime (into
  // the immutable _expression_keys vector).
  const ExpressionData* ExpressionByFieldId(irs::field_id id) const noexcept;

  // The expression keys (payload + allocated field_id), one self-contained unit
  // each.
  const std::vector<ExpressionKey>& ExpressionKeys() const noexcept {
    return _expression_keys;
  }

  struct FieldLookup {
    const InvertedIndexEntryInfo* entry = nullptr;
    irs::field_id entry_field_id = irs::field_limits::invalid();
  };
  FieldLookup LookupField(irs::field_id id) const noexcept;
  static void AppendKindSuffix(std::string& out,
                               const duckdb::LogicalType& type);

  ColumnTokenizer GetTokenizer(const std::shared_ptr<const Snapshot>& snapshot,
                               irs::field_id field_id) const;

  irs::field_id FindFieldIdBySerialized(
    std::string_view serialized_expr) const noexcept;

  std::optional<irs::HNSWInfo> GetHNSWInfo(irs::field_id field_id) const;

  const InvertedIndexOptions& GetOptions() const noexcept { return _options; }

  const std::optional<ScorerOptions>& GetTopKScorer() const noexcept {
    return _options.topk_scorer;
  }

  containers::FlatHashSet<ObjectId> GetTokenizers() const final;

  // Mutable iresearch runtime storage (writer/reader/refresh) for this index,
  // held behind a shared_ptr on the otherwise-immutable metadata object: a COW
  // snapshot clone shares the same storage, a row feed never clones the
  // catalog, and a metadata mutation (Clone) carries it forward. nullptr until
  // the index is bound to its storage (CREATE INDEX / boot recovery).
  const std::shared_ptr<search::InvertedIndexStorage>& GetData()
    const noexcept {
    return _data;
  }
  void SetData(
    std::shared_ptr<search::InvertedIndexStorage> data) const noexcept {
    _data = std::move(data);
  }

 private:
  // One pass: de-dup `columns` and append each expression key's dependent
  // columns -> the base query surface.
  static DerivedColumnIds DeriveFromKeys(
    std::span<const Column::Id> columns,
    std::span<const ExpressionKey> expression_keys);

  void BuildExprByFieldIdIndex();
  void BuildSerializedExprIndex();
  void BuildFieldLookupIndex();
  void BumpTickServerForEntryIds();

  Entries _entries;
  std::vector<ExpressionKey> _expression_keys;
  // Bridge: field_id -> the owning expression key's payload (nullptr-absent for
  // column keys). Pointers are stable (into the immutable _expression_keys).
  containers::FlatHashMap<irs::field_id, const ExpressionData*>
    _expr_by_field_id;
  // Reverse map: serialized expression -> field_id. Views point into the
  // durable storage in _expression_keys.
  containers::FlatHashMap<std::string_view, irs::field_id> _expr_to_field;
  containers::FlatHashMap<irs::field_id, FieldLookup> _field_lookup;
  InvertedIndexOptions _options;
  mutable std::shared_ptr<search::InvertedIndexStorage> _data;
};

}  // namespace sdb::catalog
