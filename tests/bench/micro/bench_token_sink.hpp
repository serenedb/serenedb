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

#include <benchmark/benchmark.h>

#include <iresearch/analysis/batch/token_batch.hpp>
#include <iresearch/analysis/batch/token_sinks.hpp>

namespace bench {

// Fill destination that touches every produced term (so the tokenizer's
// work is not optimized away) and counts tokens.
class DrainSink final : public irs::TokenConsumer {
 public:
  explicit DrainSink(irs::TokenLayout layout = irs::TokenLayout::TermsPos)
    : writer{*this, layout} {}

  void Consume(irs::TokenBatch& batch, std::span<const irs::DocRun>) final {
    for (uint32_t i = 0; i < batch.count; ++i) {
      benchmark::DoNotOptimize(batch.terms[i].GetData());
    }
    count += batch.count;
  }

  size_t Consume() {
    writer.Finish();
    return count;
  }

  irs::TokenWriter writer;
  size_t count = 0;
};

}  // namespace bench
