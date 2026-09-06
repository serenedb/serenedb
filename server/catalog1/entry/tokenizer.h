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

#include <absl/synchronization/mutex.h>

#include <duckdb/catalog/standard_entry.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <memory>
#include <string>
#include <vector>

#include "search/search_analyzer_impl.h"

namespace sdb::catalog {

class CreateTokenizerInfo final : public duckdb::CreateInfo {
 public:
  CreateTokenizerInfo()
    : duckdb::CreateInfo{duckdb::CatalogType::TOKENIZER_ENTRY} {}

  CreateTokenizerInfo(duckdb::Identifier name, search::Features features,
                      irs::analysis::TokenizerConfig config);

  const irs::analysis::TokenizerConfig& Config() const noexcept {
    return _config;
  }

  search::Features GetFeatures() const noexcept { return _features; }

  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;
  std::string ToString() const final;

 private:
  irs::analysis::TokenizerConfig _config;
  search::Features _features;
};

class Tokenizer final : public std::enable_shared_from_this<Tokenizer> {
 public:
  struct Deleter {
    std::shared_ptr<const Tokenizer> tokenizer;

    void operator()(irs::analysis::Analyzer* analyzer) const {
      if (tokenizer) {
        tokenizer->Release(irs::analysis::Analyzer::ptr{analyzer});
      } else {
        delete analyzer;
      }
    }
  };

  using TokenizerWrapper = std::unique_ptr<irs::analysis::Analyzer, Deleter>;

  Tokenizer(search::Features features, irs::analysis::TokenizerConfig config)
    : _config{std::move(config)}, _features{features} {}

  TokenizerWrapper Acquire() const;

  const irs::analysis::TokenizerConfig& Config() const noexcept {
    return _config;
  }

  search::Features GetFeatures() const noexcept { return _features; }

 private:
  void Release(irs::analysis::Analyzer::ptr analyzer) const noexcept;

  irs::analysis::TokenizerConfig _config;
  search::Features _features;
  mutable absl::Mutex _mutex;
  mutable std::vector<irs::analysis::Analyzer::ptr> _pool
    ABSL_GUARDED_BY(_mutex);
};

using TokenizerRef = std::shared_ptr<const Tokenizer>;

class TokenizerCatalogEntry final : public duckdb::StandardEntry {
 public:
  static constexpr duckdb::CatalogType Type =
    duckdb::CatalogType::TOKENIZER_ENTRY;
  static constexpr const char* Name = "tokenizer";

  TokenizerCatalogEntry(duckdb::Catalog& catalog,
                        duckdb::SchemaCatalogEntry& schema,
                        CreateTokenizerInfo& info);

  const TokenizerRef& GetTokenizer() const noexcept { return _tokenizer; }

  const irs::analysis::TokenizerConfig& Config() const noexcept {
    return _tokenizer->Config();
  }

  search::Features GetFeatures() const noexcept {
    return _tokenizer->GetFeatures();
  }

  Tokenizer::TokenizerWrapper Acquire() const { return _tokenizer->Acquire(); }

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const override;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const override;
  std::string ToSQL() const override;

 private:
  TokenizerRef _tokenizer;
};

}  // namespace sdb::catalog
