////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "catalog/tokenizer.h"

#include <cstdint>
#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/serializer.h"
#include "basics/simdjson_sink.h"
#include "catalog/persistence/tokenizer.h"

namespace sdb::catalog {
namespace {

using persistence::TokenizerData;

}  // namespace

CreateTokenizerInfo::CreateTokenizerInfo(ObjectId id, ObjectId schema_id,
                                         std::string_view name,
                                         search::Features features,
                                         uint32_t norm_row_group_size,
                                         irs::analysis::TokenizerConfig config)
  : duckdb::CreateInfo{duckdb::CatalogType::TOKENIZER_ENTRY},
    _config{std::move(config)},
    _features{features},
    _norm_row_group_size{norm_row_group_size} {
  SetId(id);
  SetSchemaId(schema_id);
  SetTokenizerName(name);
}

CreateTokenizerInfo::TokenizerWrapper CreateTokenizerInfo::GetTokenizer()
  const {
  absl::MutexLock lock{&_m};
  if (_pool.empty()) {
    auto analyzer = CreateAnalyzer();
    return TokenizerWrapper{analyzer.release(), Deleter{this}};
  }
  auto analyzer = std::move(_pool.back());
  SDB_ASSERT(analyzer);
  _pool.pop_back();
  return TokenizerWrapper{analyzer.release(), Deleter{this}};
}

void CreateTokenizerInfo::PushTokenizer(
  irs::analysis::Analyzer::ptr analyzer) const noexcept {
  SDB_ASSERT(analyzer);
  absl::MutexLock lock{&_m};
  _pool.push_back(std::move(analyzer));
}

irs::analysis::Analyzer::ptr CreateTokenizerInfo::CreateAnalyzer() const {
  return irs::analysis::CreateAnalyzer(irs::analysis::Clone(_config));
}

std::shared_ptr<CreateTokenizerInfo> CreateTokenizerInfo::Deserialize(
  duckdb::Deserializer& src, ObjectId id, ObjectId schema_id) {
  TokenizerData data;
  basics::ReadTuple(src, data);
  return std::make_shared<CreateTokenizerInfo>(
    id, schema_id, data.name, data.features, data.norm_row_group_size,
    std::move(data.config));
}

persistence::TokenizerData CreateTokenizerInfo::ToData() const {
  return TokenizerData{
    .name = std::string{GetName()},
    .config = irs::analysis::Clone(_config),
    .features = _features,
    .norm_row_group_size = _norm_row_group_size,
  };
}

void CreateTokenizerInfo::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, ToData());
}

void CreateTokenizerInfo::WriteJson(basics::JsonSink& sink) const {
  basics::WriteObject(sink, ToData());
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateTokenizerInfo::Copy() const {
  return duckdb::make_uniq<CreateTokenizerInfo>(
    GetId(), GetSchemaId(), GetName(), _features, _norm_row_group_size,
    irs::analysis::Clone(_config));
}

}  // namespace sdb::catalog
