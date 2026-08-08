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
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
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

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::basics {

class JsonSink;

}  // namespace sdb::basics
namespace sdb::catalog {

// One text-search dictionary, in the form a catalog entry is built from. duckdb
// has no counterpart, so CatalogType gained TOKENIZER_ENTRY and this is a
// CreateInfo of our own under it: what a mutator fills in, what the catalog log
// records, and what SereneDBTokenizerEntry holds.
//
// Owner and ACL are not here: they travel beside the info and live on the
// entry, because duckdb's CreateInfo has nowhere to put them.
class CreateTokenizerInfo final : public duckdb::CreateInfo {
 public:
  CreateTokenizerInfo()
    : duckdb::CreateInfo{duckdb::CatalogType::TOKENIZER_ENTRY} {}
  // Returns a built analyzer to the pool of the definition it came from, so a
  // per-row tokenize does not rebuild one. A null owner means the analyzer was
  // not pooled (the built-in string tokenizer, which has no definition).
  struct Deleter {
    const CreateTokenizerInfo* tokenizer{nullptr};

    void operator()(irs::analysis::Analyzer* analyzer) {
      // TODO(mbkkt) Revert this when global identity will be available
      if (tokenizer != nullptr) {
        tokenizer->PushTokenizer(irs::analysis::Analyzer::ptr{analyzer});
      } else {
        delete analyzer;
      }
    }
  };

  using TokenizerWrapper = std::unique_ptr<irs::analysis::Analyzer, Deleter>;

  CreateTokenizerInfo(ObjectId id, ObjectId schema_id, std::string_view name,
                      search::Features features, uint32_t norm_row_group_size,
                      irs::analysis::TokenizerConfig config);

  TokenizerWrapper GetTokenizer() const;

  void PushTokenizer(irs::analysis::Analyzer::ptr analyzer) const noexcept;

  const auto& Config() const noexcept { return _config; }

  const search::Features& GetFeatures() const noexcept { return _features; }

  uint32_t GetNormRowGroupSize() const noexcept { return _norm_row_group_size; }

  void Serialize(duckdb::Serializer& sink) const final;
  void WriteJson(basics::JsonSink& sink) const;
  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;

  static duckdb::unique_ptr<duckdb::CreateInfo> Deserialize(
    duckdb::Deserializer& src);

  ObjectId GetId() const noexcept { return ObjectId{oid}; }
  void SetId(ObjectId id) noexcept { oid = id.id(); }

  ObjectId GetSchemaId() const noexcept { return ObjectId{parent_oid}; }
  void SetSchemaId(ObjectId id) noexcept { parent_oid = id.id(); }
  // A dictionary is a schema child, so the schema is its parent.
  ObjectId GetParentId() const noexcept { return GetSchemaId(); }

  std::string_view GetName() const noexcept {
    return GetQualifiedName().Name().GetIdentifierName();
  }
  void SetTokenizerName(std::string_view name) {
    SetName(duckdb::Identifier{std::string{name}});
  }

 private:
  irs::analysis::Analyzer::ptr CreateAnalyzer() const;

  // A cache of built analyzers, not part of the definition: every reader of one
  // version shares it, and a clone starts empty.
  mutable absl::Mutex _m;
  mutable std::vector<irs::analysis::Analyzer::ptr> _pool ABSL_GUARDED_BY(_m);
  irs::analysis::TokenizerConfig _config;
  search::Features _features;
  uint32_t _norm_row_group_size;
};

// The owner and the ACL are on the entry, their one home; a reader wanting
// both -- pg_ts_dict -- takes a HeldTokenizer.
using TokenizerRef = std::shared_ptr<const CreateTokenizerInfo>;
using HeldTokenizer = std::pair<TokenizerRef, Permissions>;

}  // namespace sdb::catalog
