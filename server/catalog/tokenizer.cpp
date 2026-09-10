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

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/duckdb_engine.h"
#include "basics/serializer.h"
#include "basics/simdjson_sink.h"

namespace sdb::catalog {

namespace {}  // namespace

CreateTokenizerInfo::CreateTokenizerInfo(ObjectId id, ObjectId schema_id,
                                         std::string_view name,
                                         search::Features features,
                                         irs::analysis::TokenizerConfig config)
  : duckdb::CreateInfo{duckdb::CatalogType::TOKENIZER_ENTRY},
    _config{std::move(config)},
    _features{features} {
  SetId(id);
  SetSchemaId(schema_id);
  SetName(duckdb::Identifier{std::string{name}});
}

Tokenizer::Tokenizer(ObjectId id, search::Features features,
                     irs::analysis::TokenizerConfig config)
  : _id{id},
    _config{std::move(config)},
    _features{features},
    _pool{irs::analysis::TokenizerPool::Get(DuckDBEngine::Instance().instance(),
                                            absl::StrCat(id.id()))} {}

Tokenizer::TokenizerWrapper Tokenizer::GetTokenizer(
  duckdb::ClientContext& ctx) const {
  auto analyzer = _pool->Acquire();
  if (!analyzer) {
    analyzer = irs::analysis::CreateTokenizer(
      irs::analysis::Clone(_config),
      DuckDBEngine::Instance().instance().GetSharedObjectCache());
  }
  TokenizerWrapper wrapper{analyzer.release(), Deleter{_pool}};
  wrapper->Bind(ctx);
  return wrapper;
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateTokenizerInfo::Deserialize(
  duckdb::Deserializer& src) {
  auto result = duckdb::make_uniq<CreateTokenizerInfo>();
  result->SetName(src.ReadPropertyWithDefault<duckdb::Identifier>(200, "name"));
  // Analyzer config and feature set are iresearch types: the basics framework
  // is the only serializer they have, so they ride inside one property.
  src.OnPropertyBegin(202, "analyzer");
  auto refs = std::tie(result->_config, result->_features);
  basics::ReadTuple(src, refs);
  src.OnPropertyEnd();
  return std::move(result);
}

void CreateTokenizerInfo::WriteJson(basics::JsonSink& sink) const {
  sink.OnObjectBegin();
  sink.OnPropertyBegin("config");
  basics::WriteObject(sink, _config);
  sink.OnSeparator();
  sink.OnPropertyBegin("features");
  basics::WriteObject(sink, _features);
  sink.OnObjectEnd();
}

void CreateTokenizerInfo::Serialize(duckdb::Serializer& sink) const {
  duckdb::CreateInfo::Serialize(sink);
  sink.WritePropertyWithDefault<duckdb::Identifier>(200, "name",
                                                    qualified_name.Name());
  sink.OnPropertyBegin(202, "analyzer");
  basics::WriteTuple(sink, std::tie(_config, _features));
  sink.OnPropertyEnd();
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateTokenizerInfo::Copy() const {
  return duckdb::make_uniq<CreateTokenizerInfo>(GetId(), GetSchemaId(),
                                                GetName(), _features,
                                                irs::analysis::Clone(_config));
}

}  // namespace sdb::catalog
