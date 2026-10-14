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

#include "iresearch/formats/hnsw/hnsw_bases.hpp"

#include <absl/algorithm/container.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/fp16.h>

#include <algorithm>
#include <utility>

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "iresearch/formats/hnsw/hnsw_graph.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {
namespace {

float HalfDot(const uint16_t* row, const float* q, uint32_t d) noexcept {
  uint32_t i = 0;
  float sum = 0.f;
#ifdef __AVX2__
  __m256 acc0 = _mm256_setzero_ps();
  __m256 acc1 = _mm256_setzero_ps();
  for (; i + 16 <= d; i += 16) {
    acc0 = _mm256_fmadd_ps(_mm256_cvtph_ps(_mm_loadu_si128(
                             reinterpret_cast<const __m128i*>(row + i))),
                           _mm256_loadu_ps(q + i), acc0);
    acc1 = _mm256_fmadd_ps(_mm256_cvtph_ps(_mm_loadu_si128(
                             reinterpret_cast<const __m128i*>(row + i + 8))),
                           _mm256_loadu_ps(q + i + 8), acc1);
  }
  const __m256 v = _mm256_add_ps(acc0, acc1);
  __m128 h = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
  h = _mm_add_ps(h, _mm_movehl_ps(h, h));
  h = _mm_add_ss(h, _mm_movehdup_ps(h));
  sum = _mm_cvtss_f32(h);
#elif defined(__aarch64__)
  float32x4_t acc0 = vdupq_n_f32(0.f);
  float32x4_t acc1 = vdupq_n_f32(0.f);
  for (; i + 8 <= d; i += 8) {
    acc0 =
      vfmaq_f32(acc0, vcvt_f32_f16(vreinterpret_f16_u16(vld1_u16(row + i))),
                vld1q_f32(q + i));
    acc1 =
      vfmaq_f32(acc1, vcvt_f32_f16(vreinterpret_f16_u16(vld1_u16(row + i + 4))),
                vld1q_f32(q + i + 4));
  }
  sum = vaddvq_f32(vaddq_f32(acc0, acc1));
#endif
  for (; i < d; ++i) {
    sum += faiss::decode_fp16(row[i]) * q[i];
  }
  return sum;
}

}  // namespace

HnswBases::HalfBasis MakeHalfBasis(std::span<const float> basis) {
  std::vector<uint16_t> half(basis.size());
  for (size_t i = 0; i < half.size(); ++i) {
    half[i] = faiss::encode_fp16(basis[i]);
  }
  return std::make_shared<const std::vector<uint16_t>>(std::move(half));
}

HnswBases::HalfBasis HnswBases::Half(const Basis& basis) {
  absl::MutexLock lock{&_mutex};
  for (const auto& halved : _halves) {
    if (halved.basis.lock() == basis) {
      if (auto half = halved.half.lock()) {
        return half;
      }
    }
  }
  std::erase_if(_halves, [](const Halved& halved) {
    return halved.basis.expired() || halved.half.expired();
  });
  auto half = MakeHalfBasis(*basis);
  _halves.push_back({.basis = basis, .half = half});
  return half;
}

HnswBases::Basis HnswBases::Find(field_id field) const {
  absl::MutexLock lock{&_mutex};
  const auto it = _published.find(field);
  return it == _published.end() ? nullptr : it->second;
}

HnswBases::Basis HnswBases::Publish(field_id field, Basis basis) {
  absl::MutexLock lock{&_mutex};
  auto& slot = _published[field];
  if (!slot) {
    slot = std::move(basis);
  }
  return slot;
}

HnswBases::Basis HnswBases::Share(field_id field, std::vector<float> basis) {
  absl::MutexLock lock{&_mutex};
  auto& published = _published[field];
  if (published && *published == basis) {
    return published;
  }
  for (const auto& loaded : _loaded) {
    if (auto shared = loaded.lock(); shared && *shared == basis) {
      return shared;
    }
  }
  std::erase_if(_loaded, [](const auto& loaded) { return loaded.expired(); });
  auto shared = std::make_shared<const std::vector<float>>(std::move(basis));
  if (published) {
    _loaded.push_back(shared);
  } else {
    published = shared;
  }
  return shared;
}

std::shared_ptr<const PanoramaQuery> AnnQueryCache::Get(
  const Basis& basis, const HalfBasis& half, std::span<const float> query) {
  const auto d = static_cast<uint32_t>(query.size());
  SDB_ASSERT(basis->size() == size_t{d} * d);
  Entry* entry = nullptr;
  {
    absl::MutexLock lock{&_mutex};
    for (const auto& cached : _entries) {
      if (cached->basis == basis) {
        entry = cached.get();
        break;
      }
    }
    if (!entry) {
      auto made = std::make_unique<Entry>();
      made->basis = basis;
      made->query = std::make_shared<PanoramaQuery>();
      made->query->rotated.resize(d);
      made->query->tails.resize(HnswPanoramaLevels(d) + 1);
      entry = _entries.emplace_back(std::move(made)).get();
    }
  }
  auto& out = *entry->query;
  const uint32_t slices = HnswPanoramaLevels(d);
  for (auto slice = entry->next.fetch_add(1, std::memory_order_relaxed);
       slice < slices;
       slice = entry->next.fetch_add(1, std::memory_order_relaxed)) {
    const uint32_t first = slice * kHnswPanoramaLevel;
    const uint32_t last = std::min(d, first + kHnswPanoramaLevel);
    for (uint32_t row = first; row < last; ++row) {
      out.rotated[row] =
        half ? HalfDot(half->data() + size_t{row} * d, query.data(), d)
             : faiss::fvec_inner_product(basis->data() + size_t{row} * d,
                                         query.data(), d);
    }
    if (entry->done.fetch_add(1, std::memory_order_acq_rel) + 1 == slices) {
      HnswPanoramaTails(out.rotated.data(), d, out.tails.data());
      entry->ready.store(true, std::memory_order_release);
      entry->ready.notify_all();
    }
  }
  entry->ready.wait(false, std::memory_order_acquire);
  return entry->query;
}

std::shared_ptr<const QuantizerCodebook> AnnQueryCache::Codebook(
  const QuantizerStats& stats, std::span<const float> query) {
  const auto key = stats.CodebookKey();
  if (key.empty()) {
    return stats.MakeCodebook(query);
  }
  absl::MutexLock lock{&_mutex};
  for (const auto& shared : _codebooks) {
    if (absl::c_equal(shared.key, key)) {
      return shared.codebook;
    }
  }
  auto codebook = stats.MakeCodebook(query);
  _codebooks.push_back({.key = {key.begin(), key.end()}, .codebook = codebook});
  return codebook;
}

}  // namespace irs
