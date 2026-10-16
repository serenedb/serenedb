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

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/formats/hnsw/hnsw_graph.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"

namespace irs {

class ColumnReader;
class ReadContext;

class HnswWriter final : public AnnWriter {
 public:
  HnswWriter(AnnInfo info, HnswBases* bases);
  ~HnswWriter() final;

  AnnKind Kind() const noexcept final { return AnnKind::Hnsw; }

  field_id ColumnId() const noexcept final { return _info.centroids_id; }

  bool Empty() const noexcept final { return _graph.Empty(); }

  void SetMergeSources(std::span<const MergeSource> sources) noexcept final {
    _merge_sources = sources;
  }

  yaclib::Task<> Compute(const ColumnReader& col, ReadContext& ctx,
                         const AnnBuildEnv* env) final;

  void Flush() final;

 private:
  std::shared_ptr<const QuantizerStats> SerializeStats();

  std::shared_ptr<const QuantizerStats> EncodeRotated(
    std::span<const float> vectors, std::span<const uint8_t> valid,
    const AnnBuildEnv* env);

  bool TrainQuantizer(const ColumnReader& col, ReadContext& ctx, bool normalize,
                      std::vector<float>& batch_buf, const AnnBuildEnv* env);

  AnnInfo _info;
  HnswBases* _bases;
  std::span<const MergeSource> _merge_sources;
  HnswGraphWriter _graph;
  std::vector<float> _vectors;
  std::vector<float> _rotation;
  std::vector<float> _norms;
  std::vector<float> _tails;
  bstring _codes;
  bstring _stats_blob;
  std::unique_ptr<QuantizerWriter> _qw;
  std::vector<float> _centroid;
  uint32_t _d = 0;
  uint32_t _record_size = 0;
  uint64_t _rows = 0;
};

}  // namespace irs
