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

#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/value.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/search/scorer_options.hpp>
#include <iresearch/types.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "catalog1/entry/tokenizer.h"
#include "catalog1/persistence/inverted_index.h"
#include "search/search_analyzer_impl.h"

namespace duckdb {

class ClientContext;
struct CreateIndexInfo;

}  // namespace duckdb

namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search

namespace sdb::catalog {

using ScorerOptions = irs::ScorerOptions;

using persistence::InvertedIndexSettings;
using persistence::PkColumnKind;
using persistence::PkPolicy;

using TokenizerMap =
  duckdb::identifier_map_t<duckdb::optional_ptr<const TokenizerCatalogEntry>>;

struct ColumnTokenizer {
  TokenizerCatalogEntry::TokenizerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
  irs::field_id tokenizer_column = irs::field_limits::invalid();
};

struct InvertedIndexField {
  irs::field_id numeric_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id null_field_id = irs::field_limits::invalid();
  irs::field_id synthetic_column = irs::field_limits::invalid();
  search::Features features;
  bool store_values = false;
  bool indexed_term_dict = false;
  bool whole_value = false;
  bool is_keyword = false;
  irs::ColumnOptions column_options;
  // The text search dictionary this field tokenizes through; empty when it
  // tokenizes verbatim. Named rather than referenced by oid because duckdb
  // reassigns oids on every load.
  duckdb::Identifier text_dictionary;

  bool IsIVF() const noexcept { return column_options.ivf_info.has_value(); }
  bool HasTextDictionary() const noexcept { return !text_dictionary.empty(); }
  bool HasJsonLeafFields() const noexcept {
    return irs::field_limits::valid(numeric_field_id) &&
           irs::field_limits::valid(bool_field_id);
  }
  bool IsTermDict() const noexcept { return !IsIVF() && indexed_term_dict; }
  bool IsStored() const noexcept { return store_values; }
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

struct InvertedIndexKey {
  irs::field_id field_id = irs::field_limits::invalid();
  // INVALID for a bare column, which indexes under the column's own type.
  duckdb::LogicalType type;
  std::string normalized_expression;
};

using InvertedIndexFields =
  containers::NodeHashMap<irs::field_id, InvertedIndexField>;

std::vector<std::string> ParseKeyColumns(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options);

duckdb::optional_ptr<TokenizerCatalogEntry> ResolveOpclassDict(
  duckdb::CatalogTransaction transaction, duckdb::SchemaCatalogEntry& schema,
  std::string_view opclass);

struct InvertedIndexConfig final : irs::IndexFieldOptions {
  irs::ColumnOptions GetColumnOptions(irs::field_id id) const final;
  irs::field_id GetNormColumnId(irs::field_id id) const final;

  const InvertedIndexField* FindEntry(irs::field_id field_id) const noexcept;
  const InvertedIndexField* FindColumnInfo(
    irs::field_id column_id) const noexcept;
  InvertedIndexFieldLookup LookupField(irs::field_id field_id) const noexcept;
  bool IsKeywordField(irs::field_id field_id) const noexcept;

  const InvertedIndexKey* FindKey(irs::field_id field_id) const noexcept;
  duckdb::LogicalType ExpressionType(irs::field_id field_id) const noexcept;

  ColumnTokenizer GetTokenizer(const TokenizerMap& dicts,
                               irs::field_id field_id) const;

  irs::field_id FindFieldIdByExpression(
    std::string_view normalized) const noexcept;

  InvertedIndexSettings settings;
  PkPolicy pk;
  std::vector<InvertedIndexKey> keys;
  InvertedIndexFields fields;
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

  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const override;
  std::string ToSQL() const override;

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
  std::optional<ScorerOptions> TopKScorer(duckdb::ClientContext& context) const;
  std::string ExpressionText(irs::field_id field_id) const;

 private:
  persistence::InvertedIndexData ToPersisted() const;

  std::shared_ptr<search::InvertedIndexStorage> _storage;
  std::shared_ptr<const InvertedIndexConfig> _config;
  duckdb::Identifier _relation_name;
};

}  // namespace sdb::catalog
