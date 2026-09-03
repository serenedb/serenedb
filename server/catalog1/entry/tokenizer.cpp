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

#include "catalog1/entry/tokenizer.h"

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <utility>

#include "basics/assert.h"

namespace sdb::catalog {

CreateTokenizerInfo::CreateTokenizerInfo(duckdb::Identifier name,
                                         search::Features features,
                                         irs::analysis::TokenizerConfig config)
  : duckdb::CreateInfo{duckdb::CatalogType::TOKENIZER_ENTRY},
    _config{std::move(config)},
    _features{features} {
  SetName(std::move(name));
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateTokenizerInfo::Copy() const {
  auto result = duckdb::make_uniq<CreateTokenizerInfo>(
    qualified_name.Name(), _features, irs::analysis::Clone(_config));
  CopyProperties(*result);
  return std::move(result);
}

std::string CreateTokenizerInfo::ToString() const {
  return absl::StrCat(
    "CREATE TEXT SEARCH DICTIONARY ",
    duckdb::KeywordHelper::WriteOptionallyQuoted(QualifiedNameToString()), ";");
}

TokenizerCatalogEntry::TokenizerCatalogEntry(duckdb::Catalog& catalog,
                                             duckdb::SchemaCatalogEntry& schema,
                                             CreateTokenizerInfo& info)
  : duckdb::StandardEntry{duckdb::CatalogType::TOKENIZER_ENTRY, schema, catalog,
                          info.GetQualifiedName().Name()},
    _config{irs::analysis::Clone(info.Config())},
    _features{info.GetFeatures()},
    _pool{std::make_shared<AnalyzerPool>(irs::analysis::Clone(info.Config()))} {
  comment = info.comment;
  tags = info.tags;
  dependencies = info.dependencies;
}

irs::analysis::Analyzer::ptr AnalyzerPool::Acquire() {
  const absl::MutexLock lock{&_mutex};
  if (_pool.empty()) {
    return irs::analysis::CreateAnalyzer(irs::analysis::Clone(_config));
  }
  auto analyzer = std::move(_pool.back());
  SDB_ASSERT(analyzer);
  _pool.pop_back();
  return analyzer;
}

void AnalyzerPool::Release(irs::analysis::Analyzer::ptr analyzer) noexcept {
  SDB_ASSERT(analyzer);
  const absl::MutexLock lock{&_mutex};
  _pool.push_back(std::move(analyzer));
}

TokenizerCatalogEntry::TokenizerWrapper TokenizerCatalogEntry::Acquire() const {
  return TokenizerWrapper{_pool->Acquire().release(), Deleter{_pool}};
}

duckdb::unique_ptr<duckdb::CreateInfo> TokenizerCatalogEntry::GetInfo() const {
  auto info = duckdb::make_uniq<CreateTokenizerInfo>(
    name, _features, irs::analysis::Clone(_config));
  info->SetQualification(catalog.GetName(), schema.name);
  info->comment = comment;
  info->tags = tags;
  info->dependencies = dependencies;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> TokenizerCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<TokenizerCatalogEntry>(
    catalog, schema, info->Cast<CreateTokenizerInfo>());
}

std::string TokenizerCatalogEntry::ToSQL() const {
  return GetInfo()->ToString();
}

}  // namespace sdb::catalog
