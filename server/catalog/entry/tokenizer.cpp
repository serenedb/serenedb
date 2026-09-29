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

#include "catalog/entry/tokenizer.h"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <string_view>
#include <utility>

#include "catalog/persistence/blob.h"

namespace sdb::catalog {

std::string PackTokenizerConfig(const irs::analysis::TokenizerConfig& config) {
  return persistence::Pack(config);
}

irs::analysis::TokenizerConfig UnpackTokenizerConfig(std::string_view name,
                                                     std::string_view bytes) {
  return persistence::Unpack<irs::analysis::TokenizerConfig>(
    "text search dictionary", name, bytes);
}

namespace {

irs::analysis::TokenizerConfig WithoutLastOption(
  const irs::analysis::TokenizerConfig& config) {
  auto older = irs::analysis::Clone(config);
  std::visit(
    [](auto& options) {
      auto fields = irs::utils::FieldsOf(options);
      if constexpr (constexpr auto kCount = std::tuple_size_v<decltype(fields)>;
                    kCount != 0) {
        auto& last = std::get<kCount - 1>(fields);
        last = std::remove_cvref_t<decltype(last)>{};
      }
    },
    older.config);
  return older;
}

}  // namespace

TokenizerCatalogEntry::TokenizerCatalogEntry(duckdb::Catalog& catalog,
                                             duckdb::SchemaCatalogEntry& schema,
                                             duckdb::CreateTokenizerInfo& info)
  : duckdb::StandardEntry{duckdb::CatalogType::TOKENIZER_ENTRY, schema, catalog,
                          info.GetQualifiedName().Name(), info.oid},
    _tokenizer{std::make_shared<Tokenizer>(
      search::Features{static_cast<irs::IndexFeatures>(info.features)},
      UnpackTokenizerConfig(info.GetQualifiedName().Name().GetIdentifierName(),
                            info.config))} {
  comment = info.comment;
  tags = info.tags;
  dependencies = info.dependencies;
  permissions = info.permissions;
}

Tokenizer::TokenizerWrapper Tokenizer::Acquire(
  duckdb::ClientContext& context) const {
  irs::analysis::Tokenizer::ptr analyzer;
  {
    const absl::MutexLock lock{&_mutex};
    if (!_pool.empty()) {
      analyzer = std::move(_pool.back());
      _pool.pop_back();
    }
  }
  if (!analyzer) {
    analyzer = irs::analysis::CreateTokenizer(
      irs::analysis::Clone(_config),
      irs::DuckDBEngine::Instance().instance().GetSharedObjectCache());
  }
  TokenizerWrapper wrapper{analyzer.release(), Deleter{shared_from_this()}};
  wrapper->Bind(context);
  return wrapper;
}

void Tokenizer::Release(irs::analysis::Tokenizer::ptr analyzer) const noexcept {
  analyzer->Unbind();
  const absl::MutexLock lock{&_mutex};
  _pool.emplace_back(std::move(analyzer));
}

duckdb::unique_ptr<duckdb::CreateInfo> TokenizerCatalogEntry::GetInfo() const {
  auto info = duckdb::make_uniq<duckdb::CreateTokenizerInfo>();
  info->SetName(name);
  info->SetQualification(catalog.GetName(), ParentSchemaName());
  info->features = std::to_underlying(GetFeatures().GetIndexFeatures());
  info->config = PackTokenizerConfig(Config());
  SDB_IF_FAILURE("tokenizer_config_without_last_option") {
    info->config = PackTokenizerConfig(WithoutLastOption(Config()));
  }
  info->comment = comment;
  info->tags = tags;
  info->dependencies = dependencies;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> TokenizerCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<TokenizerCatalogEntry>(
    catalog, ParentSchema(context), info->Cast<duckdb::CreateTokenizerInfo>());
}

}  // namespace sdb::catalog
