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
#include <duckdb/parser/keyword_helper.hpp>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <utility>

#include "basics/assert.h"
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

Tokenizer::TokenizerWrapper Tokenizer::GetTokenizer() const {
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

void Tokenizer::PushTokenizer(
  irs::analysis::Analyzer::ptr analyzer) const noexcept {
  SDB_ASSERT(analyzer);
  absl::MutexLock lock{&_m};
  _pool.push_back(std::move(analyzer));
}

irs::analysis::Analyzer::ptr Tokenizer::CreateAnalyzer() const {
  return irs::analysis::CreateAnalyzer(irs::analysis::Clone(_config));
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
