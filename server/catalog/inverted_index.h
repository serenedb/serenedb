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
#include "catalog/tokenizer.h"
#include "search/search_analyzer_impl.h"

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

using persistence::AnnColumnConfig;
using persistence::ExpressionKey;

struct InvertedIndexEntryInfo {
  ObjectId text_dictionary = ObjectId::none();
  search::Features features;
  irs::field_id synthetic_column = irs::field_limits::invalid();
  bool store_values = false;
  bool indexed_term_dict = false;
  bool hyperloglog = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  std::optional<AnnColumnConfig> ann_config;

  irs::field_id null_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id numeric_field_id = irs::field_limits::invalid();

  bool IsAnn() const noexcept { return ann_config.has_value(); }
  bool IsIVF() const noexcept {
    return ann_config && ann_config->kind == irs::AnnKind::Ivf;
  }
  bool IsHNSW() const noexcept {
    return ann_config && ann_config->kind == irs::AnnKind::Hnsw;
  }
  bool HasTextDictionary() const noexcept { return text_dictionary.isSet(); }
  bool HasJsonLeafFields() const noexcept {
    return irs::field_limits::valid(numeric_field_id) &&
           irs::field_limits::valid(bool_field_id);
  }
  bool IsTermDict() const noexcept {
    return !IsAnn() && (indexed_term_dict || HasTextDictionary());
  }
  bool IsStored() const noexcept { return store_values || IsAnn(); }
};

// The ANN descriptor for an entry with an ann_config (nullopt otherwise), keyed
// off `field_id` (its centroids/postings ids).
std::optional<irs::AnnInfo> AnnInfoForEntry(
  irs::field_id field_id, const InvertedIndexEntryInfo& entry);

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
                               const Index& index);
// The same for the feed, which reaches here from WAL replay and from the tail
// of a commit: there the attachment is at hand and a transaction may not be,
// and the attachment may still be opening.
TokenizerMap ResolveTokenizers(duckdb::ClientContext* context,
                               duckdb::AttachedDatabase& db,
                               const Index& index);

struct ColumnTokenizer {
  Tokenizer::TokenizerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
  irs::field_id tokenizer_column = irs::field_limits::invalid();
};

// The analyzer + features for one entry: its text dictionary (the default
// string tokenizer when absent) plus its synthetic tokenizer column.
// Entry-level rather than index-level, for a config merged across several
// indexes.
ColumnTokenizer TokenizerForEntry(const TokenizerMap& dicts,
                                  const InvertedIndexEntryInfo& entry);

// One inverted index, in the form a catalog entry is built from -- and also an
// irs::IndexFieldOptions: the definition IS the per-column physical-encoding
// config the iresearch writer consults at flush/merge, handed over from the
// caller's own DDL view so the long-lived writer never reaches into the live
// catalog. The derived maps below alias this object's own storage, hence no
// copy constructor: Copy() rebuilds a fresh one from the payload.
class InvertedIndex final : public Index, public irs::IndexFieldOptions {
 public:
  using Entries =
    containers::NodeHashMap<irs::field_id, InvertedIndexEntryInfo>;

  // `columns` are the de-duped plain-column keys; `expression_keys` carry each
  // expression's payload + allocated field_id; `entries` is the per-field
  // config keyed by field_id. `predicate` is the partial-index predicate (empty
  // serialized_expr = full index); its dependent columns join the referenced
  // set so the store mirror declares them for DML. `col_to_term_field` is empty
  // for a transactional index (field_id == column id) and populated for a
  // Search-table one, so several indexes on one column get distinct term fields
  // in the shared store; it is restored from the table's tag, not this index's
  // payload.
  InvertedIndex(
    ObjectId schema_id, ObjectId id, ObjectId relation_id,
    std::string_view name, std::string comment, std::vector<ColumnId> columns,
    std::vector<ExpressionKey> expression_keys, Entries entries,
    InvertedIndexOptions options, ExpressionData predicate,
    containers::FlatHashMap<ColumnId, irs::field_id> col_to_term_field = {})
    : Index{schema_id,
            id,
            relation_id,
            name,
            std::move(comment),
            DeriveIds(columns,
                      std::views::transform(expression_keys,
                                            [](const auto& key) -> const auto& {
                                              return key.data;
                                            }),
                      predicate.dependent_columns)},
      _entries{std::move(entries)},
      _expression_keys{std::move(expression_keys)},
      _col_to_term_field{std::move(col_to_term_field)},
      _options{std::move(options)},
      _predicate{std::move(predicate)} {
    row_group_size = _options.row_group_size;
    BuildDerivedIndexes();
    RestoreEntryIds();
  }

  InvertedIndex(const InvertedIndex&) = delete;
  InvertedIndex& operator=(const InvertedIndex&) = delete;

  // Whether any column carries a term field_id of its own, i.e. whether the
  // payload needs the wider layout. Empty means every column's field_id is its
  // column id, which the narrow layout already says.
  bool HasAllocatedTermFields() const noexcept {
    return !_col_to_term_field.empty();
  }

  // The allocations themselves, for a rebuild that must carry them across (see
  // RebuiltWith): the payload's narrow layout has no room for them.
  const containers::FlatHashMap<ColumnId, irs::field_id>& TermFieldsByColumn()
    const noexcept {
    return _col_to_term_field;
  }

  persistence::InvertedIndexData ToData() const;
  // The wider layout: every column paired with its allocated term field_id.
  persistence::SearchInvertedIndexData ToSearchData() const;
  void SerializePayload(duckdb::Serializer& sink) const final;
  void WriteJson(basics::JsonSink& sink) const final;

  // `column_term_fields` says which layout the payload holds -- it cannot be
  // derived here, since the map it would come from is what is being read. It
  // rides the record as its own property, so a narrow payload is unchanged.
  static duckdb::unique_ptr<InvertedIndex> Deserialize(
    duckdb::Deserializer& src, ObjectId schema_id, ObjectId id,
    ObjectId relation_id, bool column_term_fields = false);
  // From the persisted payload, which stores the per-field config in its packed
  // form -- boot replay and Copy() both come through here. The term-field map
  // is passed through rather than taken from `data`, so a rebuild (ALTER) does
  // not drop a Search-table index's allocations.
  static duckdb::unique_ptr<InvertedIndex> FromData(
    ObjectId schema_id, ObjectId id, ObjectId relation_id,
    persistence::InvertedIndexData data,
    containers::FlatHashMap<ColumnId, irs::field_id> col_to_term_field = {});

  // The allocated term field_id for a plain column.
  irs::field_id TermFieldForColumn(ColumnId column) const noexcept {
    if (auto it = _col_to_term_field.find(column);
        it != _col_to_term_field.end()) {
      return it->second;
    }
    return static_cast<irs::field_id>(column);
  }

  // The plain column a Search-table allocated term field belongs to; call only
  // with such a field (asserted).
  ColumnId ColumnForTermField(irs::field_id field_id) const noexcept {
    for (const auto& [column, term_field] : _col_to_term_field) {
      if (term_field == field_id) {
        SDB_ASSERT(ReferencesColumn(column),
                   "ColumnForTermField: term field maps to a non-indexed "
                   "column ",
                   column);
        return column;
      }
    }
    SDB_ASSERT(false,
               "ColumnForTermField: not an allocated term field: ", field_id);
    return kInvalidColumnId;
  }

  const InvertedIndexEntryInfo* FindEntry(irs::field_id id) const noexcept;
  // Convenience: returns the entry only if it is a plain column (not an
  // indexed expression). Use when the caller knows column id semantics.
  const InvertedIndexEntryInfo* FindColumnInfo(
    catalog::ColumnId column_id) const noexcept;

  // The expression key owning `field_id`, or nullptr if `field_id` is a plain
  // column key (or unknown). Pointer is stable for the index's lifetime (into
  // the immutable _expression_keys vector).
  const ExpressionData* ExpressionByFieldId(irs::field_id id) const noexcept;

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

  std::optional<irs::AnnInfo> GetAnnInfo(irs::field_id field_id) const;

  const InvertedIndexOptions& GetOptions() const noexcept { return _options; }

  const ExpressionData* Predicate() const noexcept {
    return _predicate.serialized_expr.empty() ? nullptr : &_predicate;
  }

  // irs::IndexFieldOptions: the per-field encoding config the writer asks for
  // at flush/merge, resolved against this index's own entries (no catalog
  // lookup).
  irs::ColumnOptions GetColumnOptions(irs::field_id id) const final;
  irs::field_id GetNormColumnId(irs::field_id id) const final;

  // Segment-reuse gate: any two incarnations of an inverted index encode
  // columns identically (a layout-changing DROP COLUMN recreates the storage ->
  // new writer; RENAME / SET COMMENT leave column options untouched), and only
  // this concrete type ever reaches a serenedb writer, so equality reduces to
  // "same type" -- asserted, not branched on. Deliberately NOT the inherited
  // pointer identity: a rewritten info is a new object encoding the same
  // columns, and pointer identity would end every open segment.
  bool EqualOptions(const irs::IndexFieldOptions& other) const noexcept final {
    SDB_ASSERT(dynamic_cast<const InvertedIndex*>(&other) != nullptr,
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
  // Per-column allocated term field_id (Search-table indexes only).
  containers::FlatHashMap<ColumnId, irs::field_id> _col_to_term_field;
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

// The inverted info behind an index, for the readers whose facts are this
// kind's only.
inline const InvertedIndex& InvertedInfo(const Index& index) noexcept {
  return basics::downCast<const InvertedIndex>(index);
}

}  // namespace sdb::catalog
