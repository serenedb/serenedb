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
#include <vpack/builder.h>
#include <vpack/slice.h>

#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <memory>
#include <optional>
#include <string_view>
#include <tuple>
#include <vector>

#include "catalog/object.h"
#include "catalog/search_analyzer_impl.h"

namespace sdb::catalog {

// Discriminator for opclass kinds. Persisted under the "type" key in the
// stored vpack body. Adding new kinds is append-only; existing values must
// never be renumbered.
enum class OpClassKind : uint8_t {
  Text = 0,
};

std::string_view ToString(OpClassKind kind) noexcept;
std::optional<OpClassKind> ParseOpClassKind(std::string_view name) noexcept;

class OpClass : public SchemaObject {
 public:
  struct Deleter {
    OpClass* opclass{nullptr};

    void operator()(irs::analysis::Analyzer* analyzer) {
      // TODO(mbkkt) Revert this when global identity will be available
      if (opclass) {
        opclass->PushTokenizer(irs::analysis::Analyzer::ptr{analyzer});
      } else {
        delete analyzer;
      }
    }
  };

  using TokenizerWrapper = std::unique_ptr<irs::analysis::Analyzer, Deleter>;

  static std::shared_ptr<OpClass> ReadInternal(vpack::Slice slice,
                                               ReadContext ctx);

  ResultOr<TokenizerWrapper> GetTokenizer();

  void PushTokenizer(irs::analysis::Analyzer::ptr analyzer) noexcept;

  vpack::Slice Slice() const noexcept;

  void WriteInternal(vpack::Builder&) const final;
  std::shared_ptr<Object> Clone() const final;

  OpClass(ObjectId id, std::string_view name, OpClassKind kind,
          search::Features features, std::string data);

  OpClassKind GetKind() const noexcept { return _kind; }
  const search::Features& GetFeatures() const noexcept { return _features; }

 private:
  irs::analysis::Analyzer::ptr CreateAnalyzer() const;

  mutable absl::Mutex _m;
  std::vector<irs::analysis::Analyzer::ptr> _pool;
  std::string _data;
  OpClassKind _kind;
  search::Features _features;
};

}  // namespace sdb::catalog
