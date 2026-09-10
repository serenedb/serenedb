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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/serializer.h"

namespace sdb::catalog {

std::string PackTokenizerConfig(const irs::analysis::TokenizerConfig& config) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  basics::WriteTuple(serializer, config);
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

irs::analysis::TokenizerConfig UnpackTokenizerConfig(const std::string& bytes) {
  duckdb::MemoryStream stream{
    const_cast<duckdb::data_ptr_t>(
      reinterpret_cast<duckdb::const_data_ptr_t>(bytes.data())),
    bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  irs::analysis::TokenizerConfig config;
  basics::ReadTuple(deserializer, config);
  return config;
}

TokenizerCatalogEntry::TokenizerCatalogEntry(duckdb::Catalog& catalog,
                                             duckdb::SchemaCatalogEntry& schema,
                                             duckdb::CreateTokenizerInfo& info)
  : duckdb::StandardEntry{duckdb::CatalogType::TOKENIZER_ENTRY, schema, catalog,
                          info.GetQualifiedName().Name(), info.oid},
    _tokenizer{std::make_shared<Tokenizer>(
      search::Features{static_cast<irs::IndexFeatures>(info.features)},
      UnpackTokenizerConfig(info.config))} {
  comment = info.comment;
  tags = info.tags;
  dependencies = info.dependencies;
  permissions = info.permissions;
}

Tokenizer::TokenizerWrapper Tokenizer::Acquire() const {
  irs::analysis::Analyzer::ptr analyzer;
  {
    const absl::MutexLock lock{&_mutex};
    if (!_pool.empty()) {
      analyzer = std::move(_pool.back());
      _pool.pop_back();
    }
  }
  if (!analyzer) {
    analyzer = irs::analysis::CreateAnalyzer(irs::analysis::Clone(_config));
  }
  return TokenizerWrapper{analyzer.release(), Deleter{shared_from_this()}};
}

void Tokenizer::Release(irs::analysis::Analyzer::ptr analyzer) const noexcept {
  SDB_ASSERT(analyzer);
  const absl::MutexLock lock{&_mutex};
  _pool.push_back(std::move(analyzer));
}

duckdb::unique_ptr<duckdb::CreateInfo> TokenizerCatalogEntry::GetInfo() const {
  auto info = duckdb::make_uniq<duckdb::CreateTokenizerInfo>();
  info->SetName(name);
  info->SetQualification(catalog.GetName(), schema.name);
  info->features = std::to_underlying(GetFeatures().GetIndexFeatures());
  info->config = PackTokenizerConfig(Config());
  info->comment = comment;
  info->tags = tags;
  info->dependencies = dependencies;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> TokenizerCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<TokenizerCatalogEntry>(
    catalog, schema, info->Cast<duckdb::CreateTokenizerInfo>());
}

std::string TokenizerCatalogEntry::ToSQL() const {
  return GetInfo()->ToString();
}

}  // namespace sdb::catalog
