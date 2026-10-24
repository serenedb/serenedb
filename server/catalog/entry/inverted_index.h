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

#include <array>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/search/scorers/scorer_options.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/entry/tokenizer.h"
#include "catalog/persistence/inverted_index.h"
#include "query/config_variable_names.h"
#include "search/search_analyzer_impl.h"

namespace duckdb {

class ClientContext;
struct CreateIndexInfo;

}  // namespace duckdb
namespace sdb::search {

class InvertedIndexStorage;
class SearchTable;

}  // namespace sdb::search
namespace sdb::catalog {

using ScorerOptions = irs::ScorerOptions;

using persistence::InvertedIndexSettings;
using persistence::PkColumnKind;
using persistence::PkPolicy;

struct ColumnTokenizer {
  Tokenizer::TokenizerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
  irs::field_id tokenizer_column = irs::field_limits::invalid();
};

struct InvertedIndexField : persistence::FieldRecord {
  bool HasTextDictionary() const noexcept { return text_dictionary != 0; }
  bool HasJsonLeafFields() const noexcept {
    return irs::field_limits::valid(numeric_field_id) &&
           irs::field_limits::valid(bool_field_id);
  }
  bool IsTermDict() const noexcept {
    return !column_options.ann_info && indexed_term_dict;
  }
  bool IsStored() const noexcept { return store_values; }
  bool IsTokenized() const noexcept {
    return !whole_value && (!IsStored() || IsTermDict());
  }
};

struct InvertedIndexFieldLookup {
  irs::field_id entry_field_id = irs::field_limits::invalid();
  const InvertedIndexField* entry = nullptr;
};

// The index type name duckdb stores in IndexCatalogEntry::index_type, and the
// name the index type is registered under.
inline constexpr const char* kInvertedIndexTypeName = "inverted";

inline constexpr std::string_view kIncludedKind = "included";
inline constexpr std::string_view kIVFKind = "ivf";
inline constexpr std::string_view kHNSWKind = "hnsw";

struct InvertedIndexKey : persistence::KeyRecord {
  std::string expression_text;
};

struct ExpressionKeys {
  const InvertedIndexKey* term = nullptr;
  const InvertedIndexKey* stored = nullptr;
};

using InvertedIndexFields =
  irs::containers::NodeHashMap<irs::field_id, InvertedIndexField>;

inline constexpr auto kInvertedIndexSettings = std::to_array({
  kRowGroupSizeSetting,
  kRefreshIntervalSetting,
  kReindexIntervalSetting,
  kCompactionIntervalSetting,
  kCleanupIntervalStepSetting,
  kSegmentMemoryMaxSetting,
  kSegmentDocsMaxSetting,
  kCompactionMaxSegmentsSetting,
  kCompactionMaxSegmentsBytesSetting,
  kCompactionFloorSegmentBytesSetting,
});

std::vector<std::string> ParseKeyColumns(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

bool IsKnownInvertedIndexOption(std::string_view name);

void BindInvertedIndexOptions(
  duckdb::ClientContext& context,
  duckdb::case_insensitive_map_t<duckdb::Value>& options, bool view_backed,
  bool search_table);

InvertedIndexSettings ResolveSettings(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

using CompressionByColumn =
  irs::containers::FlatHashMap<irs::field_id, duckdb::CompressionType>;

class InvertedIndexConfig final : public irs::IndexFieldOptions {
 public:
  InvertedIndexConfig(std::vector<InvertedIndexKey> index_keys,
                      InvertedIndexFields index_fields);

  irs::ColumnOptions GetColumnOptions(irs::field_id id) const final;
  irs::field_id GetNormColumnId(irs::field_id id) const final;
  bool TrimsCurveTerms(irs::field_id id) const final;

  const InvertedIndexField* FindEntry(irs::field_id field_id) const noexcept;
  irs::field_id TermField(irs::field_id column_id) const noexcept;
  irs::field_id ColumnOf(irs::field_id field_id) const noexcept;
  const InvertedIndexField* FindColumnInfo(
    irs::field_id column_id) const noexcept {
    return LookupField(TermField(column_id)).entry;
  }
  InvertedIndexFieldLookup LookupField(irs::field_id field_id) const noexcept;
  bool IsKeywordField(irs::field_id field_id) const noexcept;

  duckdb::LogicalType ExpressionType(irs::field_id field_id) const noexcept;
  std::string ExpressionText(irs::field_id field_id) const;

  irs::field_id FindFieldIdByExpression(
    std::string_view normalized) const noexcept;
  ExpressionKeys FindExpressionKeys(std::string_view normalized) const noexcept;
  const InvertedIndexKey* FindKey(irs::field_id field_id) const noexcept;
  bool WritesField(const InvertedIndexKey& key) const noexcept {
    return FindKey(key.field_id) == &key;
  }
  bool StoresExpression(irs::field_id field_id) const noexcept;
  bool StoresExpressions() const noexcept { return _stores_expressions; }
  bool HasExpressionKeys() const noexcept { return !_expressions.empty(); }

  const std::vector<InvertedIndexKey> keys;
  const InvertedIndexFields fields;
  PkPolicy pk;
  std::optional<ScorerOptions> top_k_scorer;
  CompressionByColumn declared_compression;

 private:
  static constexpr uint32_t kNoKey = std::numeric_limits<uint32_t>::max();

  struct ExpressionSlots {
    uint32_t first = kNoKey;
    uint32_t term = kNoKey;
    uint32_t stored = kNoKey;
  };

  const InvertedIndexKey* KeyAt(uint32_t key) const noexcept {
    return key == kNoKey ? nullptr : &keys[key];
  }
  ExpressionSlots SlotsOf(std::string_view normalized) const noexcept;

  irs::containers::FlatHashMap<irs::field_id, uint32_t> _field_keys;
  irs::containers::FlatHashMap<irs::field_id, irs::field_id> _column_fields;
  irs::containers::FlatHashMap<std::string, ExpressionSlots> _expressions;
  irs::containers::FlatHashMap<irs::field_id, irs::field_id> _owners;
  bool _stores_expressions = false;
};

inline const duckdb::Value* FindOption(
  const duckdb::case_insensitive_map_t<duckdb::Value>& with,
  std::string_view name) {
  const auto it = with.find(name);
  return it != with.end() ? &it->second : nullptr;
}

std::optional<ScorerOptions> TopKScorer(
  duckdb::ClientContext& context,
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

class IndexTokenizers {
 public:
  using Bound = irs::containers::FlatHashMap<irs::field_id, ColumnTokenizer>;

  IndexTokenizers() = default;
  IndexTokenizers(duckdb::ClientContext& context, duckdb::Catalog& catalog,
                  const InvertedIndexConfig& config);

  ColumnTokenizer Acquire(irs::field_id field_id) const;
  ColumnTokenizer Acquire(irs::field_id field_id,
                          duckdb::ClientContext& context) const;
  Bound AcquireAll(duckdb::ClientContext& context) const;

 private:
  struct Field {
    TokenizerRef tokenizer;
    irs::IndexFeatures features = irs::IndexFeatures::None;
    irs::field_id tokenizer_column = irs::field_limits::invalid();
    std::optional<irs::curve::Options> curve;
  };

  irs::containers::FlatHashMap<irs::field_id, Field> _fields;
  duckdb::ClientContext* _context = nullptr;
};

class InvertedIndexEntry final : public duckdb::DuckIndexEntry {
 public:
  InvertedIndexEntry(duckdb::Catalog& catalog,
                     duckdb::SchemaCatalogEntry& schema,
                     duckdb::CreateIndexInfo& info,
                     duckdb::optional_ptr<duckdb::TableCatalogEntry> table);

  void SetConfig(std::shared_ptr<const InvertedIndexConfig> config) {
    _config = std::move(config);
  }

  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo(
    duckdb::CatalogTransaction transaction) const final;
  std::string ToSQL() const final {
    return duckdb::IndexCatalogEntry::GetInfo()->ToString();
  }

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;

  using duckdb::DuckIndexEntry::AlterEntry;
  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::CatalogTransaction transaction, duckdb::AlterInfo& info) final;

  duckdb::Identifier GetTableName() const final;

  duckdb::Identifier GetSchemaName() const final { return ParentSchemaName(); }

  void Rollback(duckdb::CatalogEntry& prev_entry) final;

  void OnDrop() final;

  void CreateReindexJob(duckdb::CatalogTransaction transaction,
                        uint32_t interval_ms);

  bool ScanColumnSegmentInfo(
    const duckdb::QueryContext& context,
    duckdb::ColumnSegmentInfoScanState& state,
    duckdb::vector<duckdb::ColumnSegmentInfo>& result) const final;

  // Handed over once the storage is opened, which happens after the entry is
  // created. Every later version of the entry inherits it through Copy.
  void AdoptStorage(std::shared_ptr<search::InvertedIndexStorage> storage) {
    _storage = std::move(storage);
  }

  const auto& Storage() const noexcept { return _storage; }
  const auto& SearchStore() const noexcept { return _search_table; }
  const auto& Config() const noexcept { return _config; }
  IndexTokenizers ResolveTokenizers(duckdb::ClientContext& context) const {
    return {context, catalog, *_config};
  }
  std::string ExpressionText(irs::field_id field_id) const {
    return _config->ExpressionText(field_id);
  }

 private:
  persistence::InvertedIndexData ToPersisted() const;

  bool ViewBacked() const noexcept { return !info && !_search_table; }

  duckdb::optional_ptr<duckdb::CatalogEntry> ReindexJob(
    duckdb::CatalogTransaction transaction) const;

  void AlterReindexJob(duckdb::CatalogTransaction transaction,
                       uint32_t interval_ms);

  std::shared_ptr<search::InvertedIndexStorage> _storage;
  std::shared_ptr<search::SearchTable> _search_table;
  std::shared_ptr<const InvertedIndexConfig> _config;
  duckdb::Identifier _relation_name;
};

std::vector<duckdb::reference<InvertedIndexEntry>> InvertedIndexEntries(
  duckdb::DuckCatalog& catalog);

}  // namespace sdb::catalog
