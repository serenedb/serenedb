#include <benchmark/benchmark.h>

#include <iresearch/index/document_mask.hpp>
#include <numeric>
#include <random>
#include <vector>

using irs::doc_id_t;
using irs::DocumentBitMask;
using irs::DocumentDeletedHashMask;
using irs::DocumentAliveHashMask;
using irs::DocumentMask;
using irs::DocumentMaskKind;
using std::vector;

namespace {

// TODO: decide whereas these values are reasonable or should they be Benchmark
// parameters
static constexpr size_t kPostingListSize = 1000;
static constexpr size_t kPostingListCount = 20;

// Build uniformly random set of deleted_count ids to be put as deleted in mask
DocumentDeletedHashMask BuildHashMask(size_t doc_count, size_t deleted_count,
                               int seed) {
  DocumentDeletedHashMask mask{irs::IResourceManager::gNoop, doc_count, deleted_count};
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
  auto source = BuildMask<DocumentDeletedHashMask>(doc_count, deleted_count, seed);
  return MaskType(irs::IResourceManager::gNoop, source);
}

template<>
DocumentDeletedHashMask BuildMask<DocumentDeletedHashMask>(size_t doc_count,
                                             size_t deleted_count, int seed) {
  return BuildHashMask(doc_count, deleted_count, seed);
}

template<>
DocumentBitMask BuildMask<DocumentBitMask>(size_t doc_count,
                                           size_t deleted_count, int seed) {
  auto source = BuildHashMask(doc_count, deleted_count, seed);
  return DocumentBitMask(irs::IResourceManager::gNoop, std::move(source));
}

// Builds a vector of size kPostingListSize of document ids to lookup in
// document mask. It allows to set a fixed amount of deleted ones in the list to
// immitate a real posting list, where a fraction of documents may be deleted
// and a the others are not.
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

// Generates a set of posting lists (with proportional amount of deleted
// documents) to benchmark DocumentMask implementations' IsDeleted method.
// Technically benchmarks a random lookups in document mask with the specifics
// that lookups are splitted into sorted blocks.
template<typename MaskType>
void BmIsDeleted(benchmark::State& state) {
  constexpr size_t kSeed = 43;

  const auto doc_count = static_cast<size_t>(state.range(0));
  const auto delete_permille = static_cast<size_t>(state.range(1));

  auto mask =
    BuildMask<MaskType>(doc_count, doc_count * delete_permille / 1000, kSeed);

  vector<vector<doc_id_t>> posting_lists(kPostingListCount);
  for (size_t i = 0; i < kPostingListCount; ++i) {
    posting_lists[i] = BuildPostingList(
      doc_count, kPostingListSize * delete_permille / 1000, mask, kSeed + i);
  }

  size_t posting_list_ind = 0;
  for (auto _ : state) {
    for (auto doc_id : posting_lists[posting_list_ind]) {
      benchmark::DoNotOptimize(mask.IsDeleted(doc_id));
    }
    posting_list_ind = (posting_list_ind + 1) % kPostingListCount;
  }
}

// Benchmarks a sequential scan of document mask.
template<typename MaskType>
void BmScanIsDeleted(benchmark::State& state) {
  constexpr size_t kSeed = 43;

  const auto doc_count = static_cast<size_t>(state.range(0));
  const auto delete_permille = static_cast<size_t>(state.range(1));

  auto mask =
    BuildMask<MaskType>(doc_count, doc_count * delete_permille / 1000, kSeed);

  for (auto _ : state) {
    for (doc_id_t doc_id = irs::doc_limits::min();
         doc_id < irs::doc_limits::min() + doc_count; ++doc_id) {
      benchmark::DoNotOptimize(mask.IsDeleted(doc_id));
    }
  }
}

void MaskArgs(benchmark::internal::Benchmark* b) {
  for (auto doc_cnt : {10000, 100000, 1000000}) {
    for (auto pmle : {10, 100, 300, 500, 700, 900, 990}) {
      b->Args({doc_cnt, pmle});
    }
  }
}

void BmDispatchInsideLoop(benchmark::State& state) {
  constexpr size_t kSeed = 43;

  const auto doc_count = static_cast<size_t>(state.range(0));
  const auto delete_permille = static_cast<size_t>(state.range(1));

  auto mask_src = BuildMask<DocumentBitMask>(
    doc_count, doc_count * delete_permille / 1000, kSeed);
  auto mask =
    irs::MakeDocumentMask(irs::IResourceManager::gNoop,
                          DocumentMaskKind::DenseBitset, std::move(mask_src));
  const auto doc_mask_view = irs::DocumentMaskView(mask.get());
  for (auto _ : state) {
    for (doc_id_t doc_id = irs::doc_limits::min();
         doc_id < irs::doc_limits::min() + doc_count; ++doc_id) {
      benchmark::DoNotOptimize(doc_mask_view.IsDeleted(doc_id));
    }
  }
}

template<typename MaskType>
void RunLoopIsDeleted(const irs::DocumentMaskView& doc_mask_view, size_t doc_count) {
  for (doc_id_t doc_id = irs::doc_limits::min();
       doc_id < irs::doc_limits::min() + doc_count; ++doc_id) {
    benchmark::DoNotOptimize(
      static_cast<const MaskType*>(doc_mask_view.Mask())->IsDeleted(doc_id));
  }
}
void BmDispatchOutsideLoop(benchmark::State& state) {
  constexpr size_t kSeed = 43;

  const auto doc_count = static_cast<size_t>(state.range(0));
  const auto delete_permille = static_cast<size_t>(state.range(1));

  auto bit_mask = BuildMask<DocumentBitMask>(
    doc_count, doc_count * delete_permille / 1000, kSeed);
  auto mask =
    irs::MakeDocumentMask(irs::IResourceManager::gNoop,
                          DocumentMaskKind::DenseBitset, std::move(bit_mask));
  const irs::DocumentMaskView doc_mask_view(mask.get());
  for (auto _ : state) {
    switch (doc_mask_view.Kind()) {
      case DocumentMaskKind::None:
        break;
      case DocumentMaskKind::DenseBitset:
        RunLoopIsDeleted<DocumentBitMask>(doc_mask_view, doc_count);
        break;
      case DocumentMaskKind::DeletedHashSet:
        RunLoopIsDeleted<DocumentDeletedHashMask>(doc_mask_view, doc_count);
        break;
      case DocumentMaskKind::AliveHashSet:
        RunLoopIsDeleted<DocumentAliveHashMask>(doc_mask_view, doc_count);
    }
  }
}

BENCHMARK_TEMPLATE(BmIsDeleted, DocumentDeletedHashMask)->Apply(MaskArgs);
BENCHMARK_TEMPLATE(BmIsDeleted, DocumentBitMask)->Apply(MaskArgs);
BENCHMARK_TEMPLATE(BmIsDeleted, DocumentAliveHashMask)->Apply(MaskArgs);
BENCHMARK_TEMPLATE(BmScanIsDeleted, DocumentDeletedHashMask)->Apply(MaskArgs);
BENCHMARK_TEMPLATE(BmScanIsDeleted, DocumentBitMask)->Apply(MaskArgs);
BENCHMARK_TEMPLATE(BmScanIsDeleted, DocumentAliveHashMask)->Apply(MaskArgs);
BENCHMARK(BmDispatchInsideLoop)->Args({1000000, 30});
BENCHMARK(BmDispatchOutsideLoop)->Args({1000000, 30});

}  // namespace

BENCHMARK_MAIN();
