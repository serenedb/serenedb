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

#include <absl/base/optimization.h>

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/ann/collector.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/table_filter.hpp"
#include "iresearch/search/detail/vector_slots.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_provider.hpp"

namespace irs::ann {

struct IvfProbe {
  uint32_t query;
  const float* centroid;
};

struct IvfCluster {
  PostingMeta meta;
  uint32_t first;
  uint32_t count;
};

class IvfQueryScan {
 public:
  IvfQueryScan(std::unique_ptr<QuantizerReader> reader, score_t boost,
               float width);

  void Score(const irs::detail::ScoreArgs& args, const SubReader& segment,
             const FieldProperties& field, irs::detail::TableFilter* table);

  QuantizerReader& Reader() noexcept { return *_reader; }

  void StartCluster(const float* centroid,
                    const irs::detail::ClusterLayout& layout);

  void Collect(irs::detail::VectorBlockReader& pay, Collector& collector,
               std::span<const doc_id_t> docs, const uint64_t* admitted,
               uint32_t base);

  void CollectScored(std::span<const byte_type> window, Collector& collector,
                     std::span<const doc_id_t> docs, const uint64_t* admitted,
                     std::span<score_t> scores, uint32_t offset, score_t floor);

 private:
  static constexpr uint32_t kRun = doc_limits::kBlockSize;

  void Keep(Collector& collector, std::span<const doc_id_t> docs,
            const uint64_t* admitted, const score_t* scores,
            const score_t* errors, score_t floor);

  uint32_t Narrow(uint32_t n);

  std::unique_ptr<QuantizerReader> _reader;
  irs::detail::ClusterScorer _scorer;
  irs::detail::ScaleProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher* _fetcher = nullptr;
  irs::detail::TableFilter* _table = nullptr;
  score_t _boost;
  float _width;
  bool _started = false;
  ABSL_CACHELINE_ALIGNED std::array<score_t, kRun> _raw;
  ABSL_CACHELINE_ALIGNED std::array<score_t, kRun> _err;
  ABSL_CACHELINE_ALIGNED std::array<doc_id_t, kRun> _docs;
  ABSL_CACHELINE_ALIGNED std::array<score_t, kRun> _kept;
  ABSL_CACHELINE_ALIGNED std::array<score_t, kRun> _spreads;
  ABSL_CACHELINE_ALIGNED std::array<score_t, kRun> _scores;
  ABSL_CACHELINE_ALIGNED std::array<doc_id_t, kRun> _all;
  std::vector<score_t> _window_err;
};

void ScanIvfClusters(const IndexInput& doc_in, const IndexInput& payload,
                     const PayloadBlockSetting& setting,
                     std::span<const IvfCluster> clusters,
                     std::span<const IvfProbe> probes,
                     std::span<IvfQueryScan> scans,
                     std::span<Collector> collectors, const uint64_t* admitted);

}  // namespace irs::ann
