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

#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>

#include "basics/assert.h"
#include "catalog/search_analyzer_impl.h"
#include "vpack/builder.h"
#include "vpack/slice.h"

namespace sdb::catalog {

irs::analysis::Analyzer::ptr AnalyzersPool::GetAnalyzer() {
  absl::MutexLock lock{&_m};
  if (_pool.empty()) {
    return CreateAnalyzer();
  }
  auto analyzer = std::move(_pool.back());
  _pool.pop_back();
  return analyzer;
}

void AnalyzersPool::PushAnalyzer(
  irs::analysis::Analyzer::ptr analyzer) noexcept {
  absl::MutexLock lock{&_m};
  _pool.push_back(std::move(analyzer));
}

irs::analysis::Analyzer::ptr AnalyzersPool::CreateAnalyzer() const {
  vpack::Slice slice{reinterpret_cast<const uint8_t*>(_data.data())};
  irs::analysis::Analyzer::ptr output;
  irs::analysis::analyzers::MakeAnalyzer(slice, output);
  return output;
}

Tokenizer::Tokenizer(ObjectId id, std::string_view name,
                     search::Features features, std::string data)
  : SchemaObject{{}, {}, {}, id, name, ObjectType::Tokenizer},
    _pool{std::make_unique<AnalyzersPool>(std::move(data))},
    _features{features} {}

void Tokenizer::WriteInternal(vpack::Builder& b) const {
  b.add("name", GetName());
  b.add("analyzer", _pool->GetAnalyzerOptions().get("analyzer"));
  b.add("features", vpack::Value{vpack::ValueType::Array});
  _features.ToVPack(b);
  b.close();
}

}  // namespace sdb::catalog
