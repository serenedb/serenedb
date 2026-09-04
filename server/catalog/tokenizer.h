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

#pragma once

#include <absl/synchronization/mutex.h>

#include <duckdb/parser/parsed_data/create_info.hpp>
#include <expected>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/analysis/tokenizer_pool.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/entry.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/object_id.h"
#include "search/search_analyzer_impl.h"

namespace duckdb {

class ClientContext;
class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::basics {

class JsonSink;

}  // namespace sdb::basics
namespace sdb::catalog {

// One tokenizer as everything but the catalog log sees it: the analyzer config
// and the pool of built analyzers over it. Not the durable shape -- that is
// CreateTokenizerInfo below (a CreateInfo of our own under TOKENIZER_ENTRY,
// which the log writes and boot reads back; owner and ACL live on the entry) --
// and not versioned: a reader that resolved this one keeps tokenizing with it
// while an ALTER writes the next.
class Tokenizer {
 public:
  // Returns a built analyzer to the pool it came from, so a per-row tokenize
  // does not rebuild one. A null owner means the analyzer was not pooled (the
  // built-in string tokenizer, which has no catalog object).
  struct Deleter {
    duckdb::shared_ptr<irs::analysis::TokenizerPool> pool{};

    void operator()(irs::analysis::Tokenizer* analyzer) {
      if (pool) {
        pool->Release(irs::analysis::Tokenizer::ptr{analyzer});
      } else {
        delete analyzer;
      }
    }
  };

  using TokenizerWrapper = std::unique_ptr<irs::analysis::Tokenizer, Deleter>;

  Tokenizer(ObjectId id, search::Features features,
            irs::analysis::TokenizerConfig config);

  ObjectId GetId() const noexcept { return _id; }

  TokenizerWrapper GetTokenizer(duckdb::ClientContext& ctx) const;

  const auto& Config() const noexcept { return _config; }

  const search::Features& GetFeatures() const noexcept { return _features; }

 private:
  ObjectId _id;
  irs::analysis::TokenizerConfig _config;
  search::Features _features;
  duckdb::shared_ptr<irs::analysis::TokenizerPool> _pool;
};

class CreateTokenizerInfo final : public duckdb::CreateInfo {
 public:
  CreateTokenizerInfo()
    : duckdb::CreateInfo{duckdb::CatalogType::TOKENIZER_ENTRY} {}

  CreateTokenizerInfo(ObjectId id, ObjectId schema_id, std::string_view name,
                      search::Features features,
                      irs::analysis::TokenizerConfig config);

  const auto& Config() const noexcept { return _config; }

  const search::Features& GetFeatures() const noexcept { return _features; }

  void Serialize(duckdb::Serializer& sink) const final;
  // The analyzer config is an option structure, not a statement.
  void WriteJson(basics::JsonSink& sink) const;
  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;

  static duckdb::unique_ptr<duckdb::CreateInfo> Deserialize(
    duckdb::Deserializer& src);

  ObjectId GetId() const noexcept { return ObjectId{oid}; }
  void SetId(ObjectId id) noexcept { oid = id.id(); }

  ObjectId GetSchemaId() const noexcept { return ObjectId{parent_oid}; }
  void SetSchemaId(ObjectId id) noexcept { parent_oid = id.id(); }
  ObjectId GetParentId() const noexcept { return GetSchemaId(); }

  std::string_view GetName() const noexcept {
    return GetQualifiedName().Name().GetIdentifierName();
  }

 private:
  irs::analysis::TokenizerConfig _config;
  search::Features _features;
};

// The owner and the ACL are on the entry, their one home.
using TokenizerRef = std::shared_ptr<const Tokenizer>;

}  // namespace sdb::catalog
