#include <benchmark/benchmark.h>

#include <iresearch/index/document_mask.hpp>
#include <numeric>
#include <random>
#include <vector>

using irs::doc_id_t;
using irs::DocumentBitMask;
using irs::DocumentHashMask;
using irs::DocumentMask;
using irs::DocumentMaskKind;
using std::vector;

namespace {

// TODO: decide whereas these values are reasonable or should they be Benchmark
// parameters
static constexpr size_t kPostingListSize = 1000;
static constexpr size_t kPostingListCount = 20;

DocumentHashMask BuildHashMask(size_t doc_count, size_t deleted_count,
                               int seed) {
  DocumentHashMask mask{irs::IResourceManager::gNoop};
  mask.HintDeletedDocCount(deleted_count);
  vector<doc_id_t> all_ids(doc_count);
  std::iota(all_ids.begin(), all_ids.end(), 1);
  std::shuffle(all_ids.begin(), all_ids.end(), std::mt19937(seed));
  for (size_t i = 0; i < deleted_count; ++i) {
    mask.MarkDeleted(all_ids[i]);
  }
  return mask;
}

template<typename MaskType>
MaskType BuildMask(size_t doc_count, size_t deleted_count, int seed) {
  auto source = BuildMask<DocumentHashMask>(doc_count, deleted_count, seed);
  return MaskType(irs::IResourceManager::gNoop, source);
}

template<>
DocumentHashMask BuildMask<DocumentHashMask>(size_t doc_count,
                                             size_t deleted_count, int seed) {
  return BuildHashMask(doc_count, deleted_count, seed);
}

template<>
DocumentBitMask BuildMask<DocumentBitMask>(size_t doc_count,
                                           size_t deleted_count, int seed) {
  auto source = BuildHashMask(doc_count, deleted_count, seed);
  return DocumentBitMask{irs::IResourceManager::gNoop, doc_count, source};
}

vector<doc_id_t> BuildPostingList(size_t doc_count,
                                  size_t posting_deleted_count,
                                  const DocumentMask& mask, size_t seed) {
  vector<doc_id_t> deleted_ids, live_ids;
  deleted_ids.reserve(mask.DeletedDocCount());
  live_ids.reserve(doc_count - mask.DeletedDocCount());
  for (doc_id_t i = 1; i <= doc_count; ++i) {
    if (mask.IsDeleted(i)) {
      deleted_ids.push_back(i);
    } else {
      live_ids.push_back(i);
    }
  }
  std::mt19937 rng(seed);
  std::shuffle(deleted_ids.begin(), deleted_ids.end(), rng);
  std::shuffle(live_ids.begin(), live_ids.end(), rng);

  size_t posting_live_count = kPostingListSize - posting_deleted_count;

  vector<doc_id_t> posting_list(kPostingListSize);
  std::copy(deleted_ids.begin(), deleted_ids.begin() + posting_deleted_count,
            posting_list.begin());
  std::copy(live_ids.begin(), live_ids.begin() + posting_live_count,
            posting_list.begin() + posting_deleted_count);
  std::sort(posting_list.begin(), posting_list.end());
  return posting_list;
}

template<typename MaskType>
void BmIsDeleted(benchmark::State& state) {
  constexpr size_t kSeed = 43;

  const auto docs_count = static_cast<uint32_t>(state.range(0));
  const auto delete_percent = static_cast<int>(state.range(1));

  auto mask =
    BuildMask<MaskType>(docs_count, docs_count * delete_percent / 100, kSeed);

  vector<vector<doc_id_t>> posting_lists(kPostingListCount);
  for (size_t i = 0; i < kPostingListCount; ++i) {
    posting_lists[i] = BuildPostingList(
      docs_count, kPostingListSize * delete_percent / 100, mask, kSeed + i);
  }

  size_t posting_list_ind = 0;
  for (auto _ : state) {
    for (auto doc_id : posting_lists[posting_list_ind]) {
      benchmark::DoNotOptimize(mask.IsDeleted(doc_id));
    }
    posting_list_ind = (posting_list_ind + 1) % kPostingListCount;
  }
}

void MaskArgs(benchmark::internal::Benchmark* b) {
  for (int doc_cnt : {10000, 100000, 1000000}) {
    for (int pct : {1, 10, 30, 50, 70, 90, 99}) {
      b->Args({doc_cnt, pct});
    }
  }
}

BENCHMARK_TEMPLATE(BmIsDeleted, DocumentHashMask)->Apply(MaskArgs);
BENCHMARK_TEMPLATE(BmIsDeleted, DocumentBitMask)->Apply(MaskArgs);

}  // namespace

BENCHMARK_MAIN();
