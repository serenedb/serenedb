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

#include <algorithm>
#include <cmath>
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

inline constexpr float kNarrowQueryPeak = 16383.f;

float NarrowQuery(std::span<const float> query, std::vector<int16_t>& out) {
  float peak = 0.f;
  for (const float v : query) {
    if (!std::isfinite(v)) {
      return 0.f;
    }
    peak = std::max(peak, std::abs(v));
  }
  out.resize(query.size());
  if (peak == 0.f) {
    return 0.f;
  }
  const float scale = kNarrowQueryPeak / peak;
  for (size_t i = 0; i < query.size(); ++i) {
    out[i] = static_cast<int16_t>(std::nearbyint(query[i] * scale));
  }
  return peak / kNarrowQueryPeak;
}

float ByteDot(const int8_t* row, const int16_t* q, uint32_t d) noexcept {
  SDB_ASSERT(d <= kHnswPanoramaMaxDim);
  uint32_t i = 0;
  float sum = 0.f;
#ifdef __AVX2__
  __m256i acc0 = _mm256_setzero_si256();
  __m256i acc1 = _mm256_setzero_si256();
  for (; i + 32 <= d; i += 32) {
    const __m256i r0 = _mm256_cvtepi8_epi16(
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + i)));
    const __m256i r1 = _mm256_cvtepi8_epi16(
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + i + 16)));
    acc0 = _mm256_add_epi32(
      acc0, _mm256_madd_epi16(
              r0, _mm256_loadu_si256(reinterpret_cast<const __m256i*>(q + i))));
    acc1 = _mm256_add_epi32(
      acc1,
      _mm256_madd_epi16(
        r1, _mm256_loadu_si256(reinterpret_cast<const __m256i*>(q + i + 16))));
  }
  const __m256 v = _mm256_cvtepi32_ps(_mm256_add_epi32(acc0, acc1));
  __m128 h = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
  h = _mm_add_ps(h, _mm_movehl_ps(h, h));
  h = _mm_add_ss(h, _mm_movehdup_ps(h));
  sum = _mm_cvtss_f32(h);
#elif defined(__aarch64__)
  int32x4_t acc0 = vdupq_n_s32(0);
  int32x4_t acc1 = vdupq_n_s32(0);
  for (; i + 8 <= d; i += 8) {
    const int16x8_t r = vmovl_s8(vld1_s8(row + i));
    const int16x8_t v = vld1q_s16(q + i);
    acc0 = vmlal_s16(acc0, vget_low_s16(r), vget_low_s16(v));
    acc1 = vmlal_high_s16(acc1, r, v);
  }
  sum = vaddvq_f32(vaddq_f32(vcvtq_f32_s32(acc0), vcvtq_f32_s32(acc1)));
#endif
  int32_t tail = 0;
  for (; i < d; ++i) {
    tail += int32_t{row[i]} * q[i];
  }
  return sum + static_cast<float>(tail);
}

}  // namespace

HnswBases::ByteBasis MakeByteBasis(std::span<const float> basis) {
  const auto d = static_cast<size_t>(
    std::llround(std::sqrt(static_cast<double>(basis.size()))));
  SDB_ASSERT(d * d == basis.size());
  HnswByteBasis bytes;
  bytes.codes.resize(basis.size());
  bytes.scales.resize(d);
  for (size_t row = 0; row < d; ++row) {
    const auto values = basis.subspan(row * d, d);
    float peak = 0.f;
    for (const float v : values) {
      peak = std::max(peak, std::abs(v));
    }
    const float scale = peak > 0.f ? peak / 127.f : 1.f;
    bytes.scales[row] = scale;
    for (size_t col = 0; col < d; ++col) {
      bytes.codes[row * d + col] = static_cast<int8_t>(
        std::clamp(std::nearbyint(values[col] / scale), -127.f, 127.f));
    }
  }
  return std::make_shared<const HnswByteBasis>(std::move(bytes));
}

HnswBases::ByteBasis HnswBases::Bytes(const Basis& basis) {
  absl::MutexLock lock{&_mutex};
  for (const auto& narrowed : _narrowed) {
    if (narrowed.basis.lock() == basis) {
      if (auto bytes = narrowed.bytes.lock()) {
        return bytes;
      }
    }
  }
  std::erase_if(_narrowed, [](const Narrowed& narrowed) {
    return narrowed.basis.expired() || narrowed.bytes.expired();
  });
  auto bytes = MakeByteBasis(*basis);
  _narrowed.push_back({.basis = basis, .bytes = bytes});
  return bytes;
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
  const Basis& basis, const ByteBasis& bytes, std::span<const float> query) {
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
      if (bytes) {
        made->unit = NarrowQuery(query, made->narrow);
      }
      entry = _entries.emplace_back(std::move(made)).get();
    }
  }
  const bool narrow = bytes && !entry->narrow.empty();
  auto& out = *entry->query;
  const uint32_t slices = HnswPanoramaLevels(d);
  for (auto slice = entry->next.fetch_add(1, std::memory_order_relaxed);
       slice < slices;
       slice = entry->next.fetch_add(1, std::memory_order_relaxed)) {
    const uint32_t first = slice * kHnswPanoramaLevel;
    const uint32_t last = std::min(d, first + kHnswPanoramaLevel);
    for (uint32_t row = first; row < last; ++row) {
      out.rotated[row] =
        narrow ? bytes->scales[row] * entry->unit *
                   ByteDot(bytes->codes.data() + size_t{row} * d,
                           entry->narrow.data(), d)
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

std::shared_ptr<const PanoramaQuery> RotateQuery(
  AnnQueryCache* cache, const HnswBases::Basis& basis,
  const HnswBases::ByteBasis& bytes, std::span<const float> query) {
  return cache ? cache->Get(basis, bytes, query)
               : AnnQueryCache{}.Get(basis, bytes, query);
}

std::shared_ptr<const QuantizerCodebook> QueryCodebook(
  AnnQueryCache* cache, const QuantizerStats& stats,
  std::span<const float> query) {
  return cache ? cache->Codebook(stats, query) : stats.MakeCodebook(query);
}

}  // namespace irs
