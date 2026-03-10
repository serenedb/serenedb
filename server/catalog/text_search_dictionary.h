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

#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <tuple>
#include <vector>

#include "catalog/object.h"
#include "vpack/builder.h"

namespace sdb::catalog {

enum class TokenizerType {
  Unknown = 0,
  Text,
  NGram,
  Stem,
  Norm,
};

template<typename Analyzer, typename... Args>
class AnalyzerFactory {
 public:
  explicit AnalyzerFactory(Args... args) : _args{std::move(args)...} {}

  irs::analysis::Analyzer::ptr Create() const {
    return std::apply(
      [](const Args&... args) { return std::make_unique<Analyzer>(args...); },
      _args);
  }

 private:
  std::tuple<Args...> _args;
};

class AnalyzersPoolBase {
 public:
  irs::analysis::Analyzer::ptr GetAnalyzer() {
    absl::MutexLock lock{&_m};
    if (_pool.empty()) {
      return CreateAnalyzer();
    }
    auto analyzer = std::move(_pool.back());
    _pool.pop_back();
    return analyzer;
  }
  void PushAnalyzer(irs::analysis::Analyzer::ptr analyzer) noexcept {
    absl::MutexLock lock{&_m};
    _pool.push_back(std::move(analyzer));
  }

  virtual ~AnalyzersPoolBase() = default;

 protected:
  virtual irs::analysis::Analyzer::ptr CreateAnalyzer() const = 0;

 private:
  absl::Mutex _m;
  std::vector<irs::analysis::Analyzer::ptr> _pool;
};

template<typename Analyzer, typename... Args>
class AnalyzersPool final : public AnalyzersPoolBase {
 public:
  explicit AnalyzersPool(AnalyzerFactory<Analyzer, Args...>&& factory)
    : _factory{std::move(factory)} {}

  irs::analysis::Analyzer::ptr CreateAnalyzer() const final {
    return _factory.Create();
  }

 private:
  AnalyzerFactory<Analyzer, Args...> _factory;
};

class TSDictionary : public SchemaObject {
 public:
  template<typename Tokenizer, typename... Args>
  static std::shared_ptr<TSDictionary> CreateTokenizer(ObjectId id,
                                                       std::string_view name,
                                                       Args... args) {
    AnalyzerFactory<Tokenizer, Args...> factory{std::move(args)...};
    auto pool =
      std::make_unique<AnalyzersPool<Tokenizer, Args...>>(std::move(factory));
    return std::make_shared<TSDictionary>(id, name, std::move(pool));
  }

  irs::analysis::Analyzer::ptr GetTokenizer() const {
    return _pool->GetAnalyzer();
  }
  void PushTokenizer(irs::analysis::Analyzer::ptr analyzer) noexcept {
    return _pool->PushAnalyzer(std::move(analyzer));
  }

  void WriteInternal(vpack::Builder& b) const final;

  TSDictionary(ObjectId id, std::string_view name,
               std::unique_ptr<AnalyzersPoolBase> pool)
    : SchemaObject{{}, {}, {}, id, name, ObjectType::TSDictionary},
      _pool{std::move(pool)} {}

 private:
  std::unique_ptr<AnalyzersPoolBase> _pool;
};

}  // namespace sdb::catalog
