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

#include "iresearch/formats/column/codecs/blob_compression_state.tpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <limits>
#include <memory>

#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/string_compression_function.hpp"
#include "iresearch/formats/column/codecs/string_compression_loops.tpp"
#include "iresearch/formats/column/codecs/zstd.hpp"
#include "iresearch/formats/column/codecs/zxc.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {
namespace {

constexpr double kLevelTolerance = 0.02;
constexpr double kWideFramesGain = 0.03;
constexpr size_t kMaxLevels = 4;

struct BlobAnalyzeState final : StringAnalyzeState {
  uint64_t score = std::numeric_limits<uint64_t>::max();
};

template<ByteCodec kLeaf>
std::unique_ptr<StringAnalyzeState> BlobInitAnalyze(const AnalyzeInput& input) {
  auto state = std::make_unique<BlobAnalyzeState>();
  state->config = {.choice = {input.shape, kLeaf}};
  return state;
}

void BlobAnalyze(StringAnalyzeState& base, const AnalyzeInput& input) {
  auto& state = base.Cast<BlobAnalyzeState>();
  auto& window = input.window;
  const auto& method = input.method;
  const auto levels = method.levels;
  SDB_ASSERT(!levels.empty() && levels.size() <= kMaxLevels);
  const auto leaf = static_cast<size_t>(state.config.choice.leaf);
  auto& tuned = input.tuned;
  auto config = state.config;
  if (!input.tune) {
    config.level = absl::c_linear_search(levels, tuned.level[leaf])
                     ? tuned.level[leaf]
                     : levels.front();
    config.layout = tuned.layout[leaf];
    state.config = config;
    state.score = window.Analyze(config);
    return;
  }
  uint64_t scores[kMaxLevels];
  uint64_t best_score = std::numeric_limits<uint64_t>::max();
  for (size_t i = 0; i < levels.size(); ++i) {
    config.level = levels[i];
    scores[i] = window.Analyze(config);
    best_score = std::min(best_score, scores[i]);
  }
  size_t level = 0;
  while (static_cast<double>(scores[level]) >
         static_cast<double>(best_score) * (1.0 + kLevelTolerance)) {
    ++level;
  }
  config.level = levels[level];
  state.config = config;
  state.score = scores[level];
  const auto analyze_layout = [&](FrameLayout layout, double min_gain) {
    auto candidate = state.config;
    candidate.layout = layout;
    const auto candidate_score = window.Analyze(candidate);
    if (static_cast<double>(candidate_score) <
        static_cast<double>(state.score) * (1.0 - min_gain)) {
      state.config = candidate;
      state.score = candidate_score;
    }
  };
  if (method.local) {
    analyze_layout(FrameLayout::Local, 0.0);
  }
  analyze_layout(FrameLayout::Wide, kWideFramesGain);
  tuned.level[leaf] = state.config.level;
  tuned.layout[leaf] = state.config.layout;
}

uint64_t BlobFinalAnalyze(StringAnalyzeState& state) {
  return state.Cast<BlobAnalyzeState>().score;
}

template<typename Codec>
constexpr StringCompressionFunction AnalyzedBlobFunction() {
  auto function = CompressionFunctionOf<BlobCompressionState<Codec>>();
  function.init_analyze = &BlobInitAnalyze<Codec::kCodec>;
  function.analyze = &BlobAnalyze;
  function.final_analyze = &BlobFinalAnalyze;
  return function;
}

}  // namespace

const StringCompressionFunction& Lz4CompressionFunction() {
  static constexpr auto kFunction = AnalyzedBlobFunction<Lz4>();
  return kFunction;
}

const StringCompressionFunction& Lz4HcCompressionFunction() {
  static constexpr auto kFunction =
    CompressionFunctionOf<BlobCompressionState<Lz4Hc>>();
  return kFunction;
}

const StringCompressionFunction& ZstdCompressionFunction() {
  static constexpr auto kFunction =
    CompressionFunctionOf<BlobCompressionState<Zstd>>();
  return kFunction;
}

const StringCompressionFunction& ZxcCompressionFunction() {
  static constexpr auto kFunction =
    CompressionFunctionOf<BlobCompressionState<Zxc>>();
  return kFunction;
}

}  // namespace irs::codecs::text
