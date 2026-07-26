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
#include <ranges>
#include <span>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "basics/down_cast.h"
#include "catalog/index.h"
#include "catalog/persistence/inverted_index.h"
#include "catalog/scorer_options.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/tokenizer.h"

namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search
namespace duckdb {

class AttachedDatabase;

}  // namespace duckdb
namespace sdb::catalog {
namespace term_dict {

inline constexpr irs::field_id kPKFieldId =
  static_cast<irs::field_id>(kGeneratedPKId.id());

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

void Validate(std::string_view label, const duckdb::LogicalType& type,
              std::string_view opclass);

}  // namespace term_dict
namespace included {

void Validate(std::string_view label, const duckdb::LogicalType& type);

}  // namespace included
namespace ivf {

uint32_t Dimension(const duckdb::LogicalType& type) noexcept;

void Validate(std::string_view label, const duckdb::LogicalType& type);

}  // namespace ivf

using persistence::ExpressionKey;
using persistence::IVFColumnConfig;

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
  std::optional<IVFColumnConfig> ivf_config;
  uint32_t row_group_size = 0;

  irs::field_id null_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id numeric_field_id = irs::field_limits::invalid();

  bool IsIVF() const noexcept { return ivf_config.has_value(); }
  bool HasTextDictionary() const noexcept { return text_dictionary.isSet(); }
  bool HasJsonLeafFields() const noexcept {
    return irs::field_limits::valid(numeric_field_id) &&
           irs::field_limits::valid(bool_field_id);
  }
  bool IsTermDict() const noexcept {
    return !IsIVF() && (indexed_term_dict || HasTextDictionary());
  }
  bool IsStored() const noexcept { return store_values || IsIVF(); }
};

// The text-search dictionaries an index's entries name, resolved once. The
// definitions are catalog entries now, so a per-field lookup on a flush path
// would be a catalog read per column per chunk; the tokenize paths take this
// instead, built where a transaction is still in scope.
using TokenizerMap = containers::FlatHashMap<ObjectId, TokenizerRef>;

// Read through `context`'s own transaction, out of the catalog of the database
// it is connected to -- the one holding both the index and them. Never through
// the shared by-id cache: that holds what is committed, and a reader has to
// keep seeing the dictionary its own index version names after another session
// has dropped both.
TokenizerMap ResolveTokenizers(duckdb::ClientContext& context,
                               const CreateIndexInfoBase& index);
// The same for the feed, which reaches here from WAL replay and from the tail
// of a commit: there the attachment is at hand and a transaction may not be,
// and the attachment may still be opening.
TokenizerMap ResolveTokenizers(duckdb::ClientContext* context,
                               duckdb::AttachedDatabase& db,
                               const CreateIndexInfoBase& index);

struct ColumnTokenizer {
  CreateTokenizerInfo::TokenizerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
  irs::field_id tokenizer_column = irs::field_limits::invalid();
};

// One inverted index, in the form a catalog entry is built from -- and also an
// irs::IndexFieldOptions: the definition IS the per-column physical-encoding
// config the iresearch writer consults at flush/merge. A write or compaction
// hands the writer the info from its own DDL view, so the long-lived writer
// never reaches into the live catalog.
//
// The three derived maps below alias this object's own storage, which is why
// there is no copy constructor: the info is const and shared, and Copy()
// rebuilds a fresh one from the payload rather than aliasing the source.
class CreateInvertedIndexInfo final : public CreateIndexInfoBase,
                                      public irs::IndexFieldOptions {
 public:
  using Entries =
    containers::NodeHashMap<irs::field_id, InvertedIndexEntryInfo>;

  // `columns` are the de-duped plain-column keys (each key's field_id is its
  // column id). `expression_keys` carry each expression's payload + its
  // allocated field_id as one unit. `entries` is the per-field config keyed by
  // field_id. `predicate` is the partial-index predicate (empty
  // serialized_expr = full index); its dependent columns join the referenced
  // set so the store mirror declares them and duckdb populates their chunk
  // vectors on DML.
  CreateInvertedIndexInfo(ObjectId schema_id, ObjectId id, ObjectId relation_id,
                          std::string_view name, std::string comment,
                          std::vector<ColumnId> columns,
                          std::vector<ExpressionKey> expression_keys,
                          Entries entries, InvertedIndexOptions options,
                          ExpressionData predicate)
    : CreateIndexInfoBase{schema_id,
                          id,
                          relation_id,
                          name,
                          std::move(comment),
                          DeriveIds(columns,
                                    std::views::transform(
                                      expression_keys,
                                      [](const auto& key) -> const auto& {
                                        return key.data;
                                      }),
                                    predicate.dependent_columns),
                          /*inverted=*/true},
      _entries{std::move(entries)},
      _expression_keys{std::move(expression_keys)},
      _options{std::move(options)},
      _predicate{std::move(predicate)} {
    BuildDerivedIndexes();
    RestoreEntryIds();
  }

  CreateInvertedIndexInfo(const CreateInvertedIndexInfo&) = delete;
  CreateInvertedIndexInfo& operator=(const CreateInvertedIndexInfo&) = delete;

  persistence::InvertedIndexData ToData() const;
  void Serialize(duckdb::Serializer& sink) const final;
  void WriteJson(basics::JsonSink& sink) const final;
  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;

  static std::shared_ptr<CreateInvertedIndexInfo> Deserialize(
    duckdb::Deserializer& src, ObjectId schema_id, ObjectId id,
    ObjectId relation_id);
  // From the persisted payload, which stores the per-field config in its packed
  // form -- boot replay and Copy() both come through here.
  static std::shared_ptr<CreateInvertedIndexInfo> FromData(
    ObjectId schema_id, ObjectId id, ObjectId relation_id,
    persistence::InvertedIndexData data);

  const InvertedIndexEntryInfo* FindEntry(irs::field_id id) const noexcept;
  // Convenience: returns the entry only if it is a plain column (not an
  // indexed expression). Use when the caller knows column id semantics.
  const InvertedIndexEntryInfo* FindColumnInfo(
    catalog::ColumnId column_id) const noexcept;

  // The expression key owning `field_id`, or nullptr if `field_id` is a plain
  // column key (or unknown). Pointer is stable for the index's lifetime (into
  // the immutable _expression_keys vector).
  const ExpressionData* ExpressionByFieldId(irs::field_id id) const noexcept;

  // The expression keys (payload + allocated field_id), one self-contained unit
  // each.
  const std::vector<ExpressionKey>& ExpressionKeys() const noexcept {
    return _expression_keys;
  }

  // Whether `field_id`'s expression feeds a synthetic geo column, where JSON
  // object/array leaves are geometry rather than the error they are anywhere
  // else. Both the DML feed and the CREATE INDEX build gate their leaf
  // rejection on this, so it lives with the keys instead of beside each.
  bool IsGeoJsonKey(const ExpressionKey& key) const noexcept;

  struct FieldLookup {
    const InvertedIndexEntryInfo* entry = nullptr;
    irs::field_id entry_field_id = irs::field_limits::invalid();
  };
  FieldLookup LookupField(irs::field_id id) const noexcept;
  static void AppendKindSuffix(std::string& out,
                               const duckdb::LogicalType& type);

  ColumnTokenizer GetTokenizer(const TokenizerMap& dicts,
                               irs::field_id field_id) const;

  bool IsKeywordField(duckdb::ClientContext& context,
                      irs::field_id field_id) const noexcept;

  irs::field_id FindFieldIdBySerialized(
    std::string_view serialized_expr) const noexcept;

  std::optional<irs::IvfInfo> GetIvfInfo(irs::field_id field_id) const;

  const InvertedIndexOptions& GetOptions() const noexcept { return _options; }

  const ExpressionData* Predicate() const noexcept {
    return _predicate.serialized_expr.empty() ? nullptr : &_predicate;
  }

  // irs::IndexFieldOptions: the per-field encoding config the writer asks for
  // at flush/merge, resolved against this index's own entries (no catalog
  // lookup).
  irs::ColumnOptions GetColumnOptions(irs::field_id id) const final;
  irs::NormColumnOptions GetNormColumnOptions(irs::field_id id) const final;

  // Segment-reuse homogeneity gate: any two incarnations of an inverted index
  // produce identical column encodings, so a write may always resume a segment
  // opened by another incarnation. A DROP COLUMN that truly changes the
  // physical layout recreates the storage (new index id -> new writer), so a
  // single writer's pooled segments only ever see encoding-equivalent
  // incarnations; RENAME and SET COMMENT -- the mutations that rewrite the info
  // without touching a key -- leave column options untouched. A serenedb
  // writer's gate only ever compares two of these (the other concrete
  // IndexFieldOptions, FunctionFieldOptions, is iresearch-test-only and never
  // mixed onto the same writer), so equality reduces to "same type" -- which is
  // an invariant here, asserted rather than branched on. It is deliberately NOT
  // the inherited pointer identity: a rewritten info is a new object encoding
  // the same columns, and pointer identity would end every open segment.
  bool EqualOptions(const irs::IndexFieldOptions& other) const noexcept final {
    SDB_ASSERT(dynamic_cast<const CreateInvertedIndexInfo*>(&other) != nullptr,
               "EqualOptions across IndexFieldOptions types");
    return true;
  }

  const std::optional<ScorerOptions>& GetTopKScorer() const noexcept {
    return _options.topk_scorer;
  }

  containers::FlatHashSet<ObjectId> GetTokenizers() const final;

 private:
  void BuildDerivedIndexes();
  void BuildExprByFieldIdIndex();
  void BuildSerializedExprIndex();
  void BuildFieldLookupIndex();
  void RestoreEntryIds();

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
  ExpressionData _predicate;
};

using InvertedIndexInfoRef = std::shared_ptr<const CreateInvertedIndexInfo>;

// The inverted info behind an index, for the readers whose facts are this
// kind's only.
inline const CreateInvertedIndexInfo& InvertedInfo(
  const CreateIndexInfoBase& index) noexcept {
  return basics::downCast<const CreateInvertedIndexInfo>(index);
}

// The same as its own ref, for the readers that keep it alive past the
// definition they took it from.
inline InvertedIndexInfoRef InvertedInfoRef(const IndexInfoRef& index) {
  SDB_ASSERT(index->IsInverted());
  return std::static_pointer_cast<const CreateInvertedIndexInfo>(index);
}

}  // namespace sdb::catalog
