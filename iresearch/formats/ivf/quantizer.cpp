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

#include "iresearch/formats/ivf/quantizer.hpp"

#include <faiss/utils/random.h>

#include <cstring>
#include <vector>

#include "iresearch/formats/ivf/quantizers/common.hpp"

namespace irs {

void GenerateSigns(uint32_t rotated_d, int64_t seed,
                   std::vector<float>& signs) {
  signs.resize(rotated_d);
  faiss::rand_signs(signs.data(), rotated_d, ~seed);
}

namespace {

constexpr uint32_t kPanoramaMinDim = 64;

}  // namespace

bool PanoramaApplies(VectorMetric metric, uint32_t d) noexcept {
  return metric != VectorMetric::L1 && d >= kPanoramaMinDim;
}

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(
  VectorQuantization quant, uint32_t d, VectorMetric metric, uint32_t pq_m,
  uint32_t pq_niter, uint32_t nb_bits, bool row_major, bool half_bit,
  uint32_t query_bits) {
  switch (quant) {
    case VectorQuantization::None:
      return quantizers::MakePanoramaWriter(d, metric);
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
    case VectorQuantization::USQ8:
    case VectorQuantization::USQ4:
    case VectorQuantization::FP16:
    case VectorQuantization::BF16:
      return quantizers::MakeScalarWriter(d, quant);
    case VectorQuantization::PQ:
      return quantizers::MakePqWriter(metric, d, pq_m, pq_niter, row_major);
    case VectorQuantization::TQ:
      return quantizers::MakeTurboQuantWriter(metric, d, nb_bits, half_bit,
                                              row_major);
    case VectorQuantization::RaBitQ:
      return quantizers::MakeRaBitQWriter(metric, d, nb_bits, row_major);
    case VectorQuantization::BQ:
      return quantizers::MakeBinaryWriter(metric, d, nb_bits, half_bit,
                                          query_bits);
    case VectorQuantization::OSQ:
      return quantizers::MakeOsqWriter(metric, d, nb_bits, query_bits);
  }
  return nullptr;
}

std::shared_ptr<const QuantizerStats> MakeQuantizerStats(
  VectorQuantization quant, uint32_t d, std::span<const byte_type> stats,
  VectorMetric metric, bool row_major) {
  switch (quant) {
    case VectorQuantization::None:
      return quantizers::MakePanoramaStats(metric, d, stats);
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
    case VectorQuantization::USQ8:
    case VectorQuantization::USQ4:
    case VectorQuantization::FP16:
    case VectorQuantization::BF16:
      return quantizers::MakeScalarStats(metric, d, quant, stats);
    case VectorQuantization::PQ:
      return quantizers::MakePqStats(metric, d, stats, row_major);
    case VectorQuantization::TQ:
      return quantizers::MakeTurboQuantStats(metric, d, stats, row_major);
    case VectorQuantization::RaBitQ:
      return quantizers::MakeRaBitQStats(metric, d, stats, row_major);
    case VectorQuantization::BQ:
      return quantizers::MakeBinaryStats(metric, d, stats);
    case VectorQuantization::OSQ:
      return quantizers::MakeOsqStats(metric, d, stats);
  }
  return nullptr;
}

void QuantizerReader::ComputeGathered(const byte_type* base,
                                      uint32_t record_size,
                                      std::span<const uint32_t> ids,
                                      score_t threshold, score_t* out) {
  _gather.resize(ids.size() * static_cast<size_t>(record_size));
  for (size_t i = 0; i < ids.size(); ++i) {
    std::memcpy(_gather.data() + i * record_size,
                base + static_cast<size_t>(ids[i]) * record_size, record_size);
  }
  ComputeBlock(_gather, threshold, out);
}

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook) {
  return codebook ? codebook->MakeReader() : nullptr;
}

}  // namespace irs
