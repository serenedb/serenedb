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

#include <cstdint>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/search/scorer_options.hpp>
#include <iresearch/types.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"
#include "catalog1/entry/tokenizer.h"
#include "search/search_analyzer_impl.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb

namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search

namespace sdb::catalog {

using ScorerOptions = irs::ScorerOptions;

using TokenizerMap =
  duckdb::identifier_map_t<duckdb::optional_ptr<const TokenizerCatalogEntry>>;

struct ColumnTokenizer {
  TokenizerCatalogEntry::TokenizerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
  irs::field_id tokenizer_column = irs::field_limits::invalid();
};

// The `ivf` opclass, resolved at CREATE INDEX. `d` comes from the key's
// ARRAY(FLOAT, N); the rest from the opclass options and the sdb_ivf_*
// session settings.
struct InvertedIndexFieldIVF {
  int d = 0;
  irs::VectorMetric metric = irs::VectorMetric::L2Sqr;
  irs::VectorQuantization quant = irs::VectorQuantization::None;
  uint32_t pq_m = 0;
  uint32_t rabitq_bits = 0;
  float sample_factor = 0;
  uint32_t posting_size = 0;
  bool compression = true;
};

struct InvertedIndexField {
  // The per-kind JSON leaves. Allocated only for a JSON key whose analyzer
  // reads leaves rather than the whole value -- a geo analyzer consumes the
  // GeoJSON object itself, so it gets none and HasJsonLeafFields() keeps it
  // out of the leaf splitter.
  irs::field_id numeric_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id null_field_id = irs::field_limits::invalid();
  // Carries either the tokenizer's own per-row payload or the field's norms --
  // never both, and nothing at all unless the analyzer asks for one.
  irs::field_id synthetic_column = irs::field_limits::invalid();
  // The text search dictionary this field tokenizes through; empty when it
  // tokenizes verbatim. Named rather than referenced by oid because duckdb
  // reassigns oids on every load.
  duckdb::Identifier text_dictionary;
  search::Features features;
  bool store_values = false;
  bool indexed_term_dict = false;
  bool hyperloglog = false;
  // The analyzer reads the whole value rather than descending into it -- a geo
  // analyzer parses the GeoJSON object itself. Distinct from having no JSON
  // leaves, which is also true of a JSON key that names no opclass at all and
  // must still be rejected for holding an object.
  bool whole_value = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  // Whether the named dictionary tokenizes verbatim (template='keyword').
  // Decided at CREATE INDEX, where the analyzer is resolved, because the
  // decoded config has no tokenizer map to resolve one with.
  bool is_keyword = false;
  std::optional<InvertedIndexFieldIVF> ivf_config;

  bool IsIVF() const noexcept { return ivf_config.has_value(); }
  bool HasTextDictionary() const noexcept {
    return !text_dictionary.empty();
  }
  bool HasJsonLeafFields() const noexcept {
    return irs::field_limits::valid(numeric_field_id) &&
           irs::field_limits::valid(bool_field_id);
  }
  bool IsTermDict() const noexcept {
    return !IsIVF() && (indexed_term_dict || HasTextDictionary());
  }
  bool IsStored() const noexcept { return store_values || IsIVF(); }
};

struct InvertedIndexFieldLookup {
  irs::field_id entry_field_id = irs::field_limits::invalid();
  const InvertedIndexField* entry = nullptr;
};

// The index type name duckdb stores in IndexCatalogEntry::index_type, and the
// name the index type is registered under.
inline constexpr const char* kInvertedIndexTypeName = "inverted";

// The two built-in opclasses. An opclass written bare resolves against the
// schema's text search dictionaries first, so a dictionary of either name
// shadows the built-in; the parenthesised form always selects the built-in.
inline constexpr std::string_view kIncludedKind = "included";
inline constexpr std::string_view kIVFKind = "ivf";

struct InvertedIndexOptions {
  uint32_t row_group_size{122880};
  uint32_t refresh_interval_ms{1000};
  uint32_t reindex_interval_ms{0};
  uint32_t compaction_interval_ms{1000};
  uint32_t cleanup_interval_step{1};
  uint64_t segment_memory_max{268435456};
  uint32_t segment_docs_max{0};
  uint32_t compaction_max_segments{10};
  uint64_t compaction_max_segments_bytes{5368709120};
  uint64_t compaction_floor_segment_bytes{2097152};

  bool operator==(const InvertedIndexOptions& rhs) const = default;
};

// duckdb persists CreateIndexInfo::options for us, so the WITH map is the
// stored form and these are the only two conversions the catalog owns.
InvertedIndexOptions DecodeInvertedIndexOptions(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

void EncodeInvertedIndexOptions(
  const InvertedIndexOptions& options,
  duckdb::case_insensitive_map_t<duckdb::Value>& into);

enum class PkColumnKind : uint8_t {
  None = 0,
  Has = 1,
  Unable = 2,
};

struct PkPolicy {
  bool index_term = true;
  PkColumnKind column = PkColumnKind::Has;
};

enum class OpclassKind : uint8_t {
  None = 0,
  Dictionary = 1,
  Included = 2,
  Ivf = 3,
};

struct InvertedIndexKey {
  irs::field_id field_id = irs::field_limits::invalid();
  irs::field_id block = irs::field_limits::invalid();
  OpclassKind kind = OpclassKind::None;
  std::string dictionary;
  duckdb::idx_t dictionary_oid = 0;
  irs::IndexFeatures features = irs::IndexFeatures::None;
  std::string return_type;
  uint8_t return_type_id = 0;
  bool feeds = true;
  std::string serialized;
};

using InvertedIndexFields =
  containers::NodeHashMap<irs::field_id, InvertedIndexField>;

// The resolved row-key policy travels in the index's options, decided at CREATE
// from the key shape the index turned out to have.
inline constexpr std::string_view kPkTermOption = "sdb_pk_term";
inline constexpr std::string_view kPkColumnOption = "sdb_pk_column";

void WritePkPolicy(PkPolicy policy,
                   duckdb::case_insensitive_map_t<duckdb::Value>& into);
PkPolicy ReadPkPolicy(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

// The view fast path's key columns, stored as one comma-joined value.
std::vector<std::string> KeyColumnsFromOptions(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

std::vector<InvertedIndexKey> DecodeInvertedKeys(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

void EncodeInvertedKeys(const std::vector<InvertedIndexKey>& keys,
                        duckdb::case_insensitive_map_t<duckdb::Value>& into);

InvertedIndexFields DecodeInvertedEntries(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

void EncodeInvertedEntries(const InvertedIndexFields& fields,
                           duckdb::case_insensitive_map_t<duckdb::Value>& into);

struct InvertedIndexFieldOptions final : irs::IndexFieldOptions {
  irs::ColumnOptions GetColumnOptions(irs::field_id id) const final;
  irs::field_id GetNormColumnId(irs::field_id id) const final;

  // The IVF build parameters of a vector key, or nullopt for any other field.
  std::optional<irs::IvfInfo> GetIvfInfo(irs::field_id field_id) const;

  // The field lookups, here rather than on the bound index: they read nothing
  // but this object, so a caller holding the entry answers them without
  // reaching for the index at all.
  const InvertedIndexField* FindEntry(irs::field_id field_id) const noexcept;
  // A stored column id, which for a JSON expression is one of the per-kind
  // sub-ids rather than the field's own.
  const InvertedIndexField* FindColumnInfo(
    irs::field_id column_id) const noexcept;
  InvertedIndexFieldLookup LookupField(irs::field_id field_id) const noexcept;
  // True when the field indexes verbatim -- no text search dictionary, so a
  // term equals the whole value.
  bool IsKeywordField(irs::field_id field_id) const noexcept;

  static constexpr size_t kNoSlot = static_cast<size_t>(-1);
  size_t KeySlot(irs::field_id field_id) const noexcept;

  bool KeyIsBareColumn(size_t slot) const noexcept;

  // The type the key feeding `field_id` evaluates to. INVALID / empty when the
  // key is a bare column or is not this index's -- which is how a caller tells
  // an expression key from a column one.
  duckdb::LogicalTypeId ExpressionTypeId(irs::field_id field_id) const noexcept;
  std::string_view ExpressionTypeName(irs::field_id field_id) const noexcept;
  duckdb::LogicalType ExpressionType(duckdb::ClientContext& context,
                                     irs::field_id field_id) const;

  // The analyzer for `field_id`, drawn from a caller-resolved dictionary map:
  // the config names dictionaries by oid, the map holds the live entries.
  ColumnTokenizer GetTokenizer(const TokenizerMap& dicts,
                               irs::field_id field_id) const;

  // The key whose expression serializes to `serialized`, or an invalid id.
  // Answered off the persisted keys, so no bound index is needed.
  irs::field_id FindFieldIdBySerialized(
    std::string_view serialized) const noexcept;

  PkPolicy pk;
  std::vector<std::string> key_columns;
  // One entry per declared key, in the order duckdb lists them: slot i here is
  // slot i of the entry's parsed_expressions and of the bound index's
  // unbound_expressions. Those two live on other objects and stay aligned by
  // convention -- this vector is what makes the alignment visible.
  std::vector<InvertedIndexKey> keys;

  std::vector<irs::field_id> indexed_columns;
  // Node-based: the entry is well past the flat map's size budget, and
  // FindEntry hands out pointers into it that outlive later inserts.
  containers::NodeHashMap<irs::field_id, InvertedIndexField> fields;
  containers::FlatHashMap<irs::field_id, irs::field_id> owner_of;
  duckdb::unique_ptr<duckdb::ParsedExpression> predicate;
};

class InvertedIndexEntry final : public duckdb::DuckIndexEntry {
 public:
  InvertedIndexEntry(duckdb::Catalog& catalog,
                     duckdb::SchemaCatalogEntry& schema,
                     duckdb::CreateIndexInfo& info,
                     duckdb::TableCatalogEntry& table);
  InvertedIndexEntry(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    duckdb::CreateIndexInfo& info,
    duckdb::shared_ptr<duckdb::IndexDataTableInfo> storage_info);

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const override;

  // A view-backed index has no DataTableInfo to read the relation's name off,
  // and the base would dereference it. The name it was created against is the
  // answer, and it is the only one available.
  duckdb::Identifier GetTableName() const override;

  duckdb::Identifier GetSchemaName() const override;

  void Rollback(duckdb::CatalogEntry& prev_entry) override;

  // Handed over once the storage is opened, which happens after the entry is
  // created. Every later version of the entry inherits it through Copy.
  void AdoptStorage(std::shared_ptr<search::InvertedIndexStorage> storage) {
    _storage = std::move(storage);
  }

  const auto& Storage() const noexcept { return _storage; }
  const auto& Config() const noexcept { return _config; }

  // The storage parameters, decoded from the persisted WITH map on every call
  // rather than cached: they are the one part of the definition ALTER INDEX
  // rewrites, and the config they would otherwise sit on is deliberately
  // shared across entry versions.
  InvertedIndexOptions Options() const {
    return DecodeInvertedIndexOptions(options);
  }
  std::string ExpressionText(irs::field_id field_id) const;

 private:
  std::shared_ptr<search::InvertedIndexStorage> _storage;
  std::shared_ptr<const InvertedIndexFieldOptions> _config;
  duckdb::Identifier _relation_name;
};

}  // namespace sdb::catalog
