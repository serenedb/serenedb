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
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <string>
#include <utility>

#include "basics/assert.h"
#include "basics/serializer.h"
#include "catalog/persistence/tokenizer.h"
#include "catalog/search_analyzer_impl.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

using persistence::TokenizerData;

}  // namespace

Tokenizer::TokenizerWrapper Tokenizer::GetTokenizer() {
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

void Tokenizer::PushTokenizer(irs::analysis::Analyzer::ptr analyzer) noexcept {
  SDB_ASSERT(analyzer);
  absl::MutexLock lock{&_m};
  _pool.push_back(std::move(analyzer));
}

irs::analysis::Analyzer::ptr Tokenizer::CreateAnalyzer() const {
  return irs::analysis::CreateAnalyzer(irs::analysis::Clone(_config));
}

Tokenizer::Tokenizer(Permissions perm, ObjectId schema_id, ObjectId id,
                     std::string_view name, search::Features features,
                     uint32_t norm_row_group_size,
                     irs::analysis::TokenizerConfig config)
  : Object{std::move(perm), schema_id, id, name, ObjectType::Tokenizer},
    _config{std::move(config)},
    _features{features},
    _norm_row_group_size{norm_row_group_size} {}

std::shared_ptr<Tokenizer> Tokenizer::Deserialize(duckdb::Deserializer& src,
                                                  ReadContext ctx) {
  TokenizerData data;
  basics::ReadTuple(src, data);

  return std::make_shared<Tokenizer>(
    std::move(data.perm), ctx.schema_id, ctx.id, data.name, data.features,
    data.norm_row_group_size, std::move(data.config));
}

void Tokenizer::Serialize(duckdb::Serializer& sink) const {
  TokenizerData data{
    .name = std::string{GetName()},
    .config = irs::analysis::Clone(_config),
    .features = _features,
    .norm_row_group_size = _norm_row_group_size,
    .perm = GetPermissions(),
  };
  basics::WriteTuple(sink, data);
}

std::shared_ptr<Object> Tokenizer::Clone() const {
  duckdb::MemoryStream stream;
  return DeserializeObject<Tokenizer>(
    SerializeObject(*this, stream),
    {.id = GetId(), .schema_id = GetParentId()});
}

}  // namespace sdb::catalog
