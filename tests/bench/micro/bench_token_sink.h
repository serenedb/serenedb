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

#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/analysis/token_sinks.hpp>

namespace bench {

class DrainSink final : public irs::TokenConsumer, public irs::StoreSink {
 public:
  explicit DrainSink(irs::TokenLayout layout = irs::TokenLayout::TermsPos)
    : layout{layout} {
    writer.Bind(*this, this);
  }

  void OnStore(irs::doc_id_t, irs::bytes_view) final {}

  void Consume(irs::TokenBatch& batch, irs::DocRuns) final {
    for (uint32_t i = 0; i < batch.count; ++i) {
      benchmark::DoNotOptimize(batch.terms[i].GetData());
    }
    count += batch.count;
  }

  size_t Consume() {
    writer.Finish();
    return count;
  }

  irs::TokenSink writer;
  irs::TokenLayout layout;
  size_t count = 0;
};

}  // namespace bench
