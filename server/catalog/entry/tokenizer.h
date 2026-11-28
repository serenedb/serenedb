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
#include <duckdb/parser/parsed_data/create_tokenizer_info.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "search/search_analyzer_impl.h"

namespace sdb::catalog {

std::string PackTokenizerConfig(const irs::analysis::TokenizerConfig& config);

irs::analysis::TokenizerConfig UnpackTokenizerConfig(std::string_view name,
                                                     std::string_view bytes);

class TokenizerBinding final
  : public std::enable_shared_from_this<TokenizerBinding> {
 public:
  struct Deleter {
    std::shared_ptr<const TokenizerBinding> binding;

    void operator()(irs::analysis::Tokenizer* analyzer) const {
      if (binding) {
        binding->Release(irs::analysis::Tokenizer::ptr{analyzer});
      } else {
        delete analyzer;
      }
    }
  };

  using TokenizerWrapper = std::unique_ptr<irs::analysis::Tokenizer, Deleter>;

  explicit TokenizerBinding(irs::analysis::TokenizerConfig config)
    : _config{std::move(config)} {}

  TokenizerWrapper Acquire(duckdb::ClientContext& context) const;

  const irs::analysis::TokenizerConfig& Config() const noexcept {
    return _config;
  }
  duckdb::LogicalTypeId Input() const noexcept { return _config.input; }
  duckdb::LogicalTypeId Terms() const noexcept { return _config.terms; }

 private:
  void Release(irs::analysis::Tokenizer::ptr analyzer) const noexcept;

  irs::analysis::TokenizerConfig _config;
  mutable absl::Mutex _mutex;
  mutable std::vector<irs::analysis::Tokenizer::ptr> _pool
    ABSL_GUARDED_BY(_mutex);
};

using TokenizerBindingRef = std::shared_ptr<const TokenizerBinding>;

class Tokenizer final {
 public:
  using Deleter = TokenizerBinding::Deleter;
  using TokenizerWrapper = TokenizerBinding::TokenizerWrapper;

  Tokenizer(search::Features features, irs::analysis::TokenizerConfig config)
    : _config{std::move(config)}, _features{features} {}

  TokenizerBindingRef Bind(duckdb::ClientContext& context,
                           const duckdb::LogicalType& input) const;

  TokenizerBindingRef Intern(std::string_view name,
                             std::string_view bound) const;

  const irs::analysis::TokenizerConfig& Config() const noexcept {
    return _config;
  }

  search::Features GetFeatures() const noexcept { return _features; }

 private:
  TokenizerBindingRef Intern(std::string packed,
                             irs::analysis::TokenizerConfig bound) const;

  irs::analysis::TokenizerConfig _config;
  search::Features _features;
  mutable absl::Mutex _mutex;
  mutable irs::containers::FlatHashMap<std::string, TokenizerBindingRef>
    _bindings ABSL_GUARDED_BY(_mutex);
  mutable irs::containers::FlatHashMap<duckdb::LogicalTypeId,
                                       TokenizerBindingRef>
    _by_input ABSL_GUARDED_BY(_mutex);
};

using TokenizerRef = std::shared_ptr<const Tokenizer>;

class TokenizerCatalogEntry final : public duckdb::StandardEntry {
 public:
  static constexpr duckdb::CatalogType Type =
    duckdb::CatalogType::TOKENIZER_ENTRY;
  static constexpr const char* Name = "tokenizer";

  TokenizerCatalogEntry(duckdb::Catalog& catalog,
                        duckdb::SchemaCatalogEntry& schema,
                        duckdb::CreateTokenizerInfo& info);

  const TokenizerRef& GetTokenizer() const noexcept { return _tokenizer; }

  const irs::analysis::TokenizerConfig& Config() const noexcept {
    return _tokenizer->Config();
  }

  const std::string& Definition() const noexcept { return _definition; }

  search::Features GetFeatures() const noexcept {
    return _tokenizer->GetFeatures();
  }

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;
  std::string ToSQL() const final { return GetInfo()->ToString(); }

 private:
  TokenizerRef _tokenizer;
  std::string _definition;
};

}  // namespace sdb::catalog
