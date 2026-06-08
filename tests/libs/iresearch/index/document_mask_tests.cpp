#include <absl/container/flat_hash_set.h>
#include <gtest/gtest.h>

#include <numeric>
#include <random>
#include <vector>

#include "iresearch/index/document_mask.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

using irs::doc_id_t;
using irs::DocumentAliveHashMask;
using irs::DocumentBitMask;
using irs::DocumentDeletedHashMask;

DocumentBitMask BuildBitMask(size_t doc_count,
                             const std::vector<doc_id_t>& deleted_ids) {
  DocumentBitMask mask{irs::IResourceManager::gNoop, doc_count};
  for (auto id : deleted_ids) {
    mask.MarkDeleted(id);
  }
  return mask;
}

template<typename MaskType>
MaskType BuildMask(size_t doc_count, const std::vector<doc_id_t>& deleted_ids) {
  auto source = BuildBitMask(doc_count, deleted_ids);
  return MaskType{irs::IResourceManager::gNoop, source};
}

template<>
DocumentBitMask BuildMask<DocumentBitMask>(
  size_t doc_count, const std::vector<doc_id_t>& deleted_ids) {
  return BuildBitMask(doc_count, deleted_ids);
}

std::vector<doc_id_t> RandomDeletedIds(size_t doc_count, size_t deleted_count,
                                       int seed) {
  std::vector<doc_id_t> all_ids(doc_count);
  std::iota(all_ids.begin(), all_ids.end(), irs::doc_limits::min());
  std::mt19937 rng(seed);
  std::shuffle(all_ids.begin(), all_ids.end(), rng);
  return std::vector<doc_id_t>(all_ids.begin(), all_ids.begin() + deleted_count);
}

template<typename MaskType>
class DocumentMaskTest : public ::testing::Test {};

using MaskTypes = ::testing::Types<DocumentDeletedHashMask, DocumentBitMask,
                                   DocumentAliveHashMask>;
TYPED_TEST_SUITE(DocumentMaskTest, MaskTypes);

TYPED_TEST(DocumentMaskTest, DocCounts) {
  constexpr size_t kDocCount = 300;
  const std::vector<doc_id_t> deleted_ids{1, 64, 67, 127, 13, 128, 299, 300};
  auto mask = BuildMask<TypeParam>(kDocCount, deleted_ids);

  EXPECT_EQ(kDocCount, mask.DocCount());
  EXPECT_EQ(deleted_ids.size(), mask.DeletedDocCount());
}

TYPED_TEST(DocumentMaskTest, IsDeleted) {
  constexpr size_t kDocCount = 300;
  const std::vector<doc_id_t> deleted_ids{1, 64, 67, 127, 13, 128, 299, 300};
  const absl::flat_hash_set<doc_id_t> deleted_set(deleted_ids.begin(),
                                                  deleted_ids.end());
  auto mask = BuildMask<TypeParam>(kDocCount, deleted_ids);

  for (doc_id_t id = irs::doc_limits::min();
       id < irs::doc_limits::min() + kDocCount; ++id) {
    EXPECT_EQ(deleted_set.contains(id), mask.IsDeleted(id)) << "doc_id=" << id;
  }

  EXPECT_FALSE(mask.IsDeleted(0));
  EXPECT_FALSE(
    mask.IsDeleted(static_cast<doc_id_t>(irs::doc_limits::min() + kDocCount)));
}

TYPED_TEST(DocumentMaskTest, IsDeletedEmpty) {
  constexpr size_t kDocCount = 68;
  auto mask = BuildMask<TypeParam>(kDocCount, {});

  EXPECT_EQ(kDocCount, mask.DocCount());
  EXPECT_EQ(0, mask.DeletedDocCount());
  EXPECT_TRUE(mask.IsEmpty());

  for (doc_id_t id = irs::doc_limits::min();
       id < irs::doc_limits::min() + kDocCount; ++id) {
    EXPECT_FALSE(mask.IsDeleted(id)) << "doc_id=" << id;
  }
}

TYPED_TEST(DocumentMaskTest, IsDeletedAllDeleted) {
  constexpr size_t kDocCount = 68;
  std::vector<doc_id_t> all_ids(kDocCount);
  std::iota(all_ids.begin(), all_ids.end(), irs::doc_limits::min());
  auto mask = BuildMask<TypeParam>(kDocCount, all_ids);

  EXPECT_EQ(kDocCount, mask.DocCount());
  EXPECT_EQ(kDocCount, mask.DeletedDocCount());

  for (doc_id_t id = irs::doc_limits::min();
       id < irs::doc_limits::min() + kDocCount; ++id) {
    EXPECT_TRUE(mask.IsDeleted(id)) << "doc_id=" << id;
  }
}

TYPED_TEST(DocumentMaskTest, ForEachDeleted) {
  constexpr size_t kDocCount = 300;
  const std::vector<doc_id_t> deleted_ids{1, 64, 67, 127, 13, 128, 299, 300};
  const absl::flat_hash_set<doc_id_t> deleted_set(deleted_ids.begin(),
                                                  deleted_ids.end());
  auto mask = BuildMask<TypeParam>(kDocCount, deleted_ids);

  absl::flat_hash_set<doc_id_t> collected;
  irs::ForEachDeleted(mask, [&](doc_id_t id) { collected.insert(id); });

  EXPECT_EQ(deleted_set, collected);
}

TYPED_TEST(DocumentMaskTest, ForEachAlive) {
  constexpr size_t kDocCount = 300;
  const std::vector<doc_id_t> deleted_ids{1, 64, 67, 127, 13, 128, 299, 300};
  const absl::flat_hash_set<doc_id_t> deleted_set(deleted_ids.begin(),
                                                  deleted_ids.end());
  auto mask = BuildMask<TypeParam>(kDocCount, deleted_ids);

  absl::flat_hash_set<doc_id_t> expected_alive;
  for (doc_id_t id = irs::doc_limits::min();
       id < irs::doc_limits::min() + kDocCount; ++id) {
    if (!deleted_set.contains(id)) {
      expected_alive.insert(id);
    }
  }

  absl::flat_hash_set<doc_id_t> collected;
  irs::ForEachAlive(mask, [&](doc_id_t id) { collected.insert(id); });

  EXPECT_EQ(expected_alive, collected);
}

TYPED_TEST(DocumentMaskTest, ForEachPartiotioning) {
  constexpr size_t kDocCount = 100;
  const std::vector<doc_id_t> deleted_ids{5, 13, 52, 100};
  auto mask = BuildMask<TypeParam>(kDocCount, deleted_ids);

  absl::flat_hash_set<doc_id_t> deleted_collected, alive_collected;
  irs::ForEachDeleted(mask, [&](doc_id_t id) { deleted_collected.insert(id); });
  irs::ForEachAlive(mask, [&](doc_id_t id) { alive_collected.insert(id); });

  ASSERT_EQ(kDocCount, deleted_collected.size() + alive_collected.size());
  for (doc_id_t id = irs::doc_limits::min();
       id < irs::doc_limits::min() + kDocCount; ++id) {
    EXPECT_NE(deleted_collected.contains(id), alive_collected.contains(id))
      << "doc_id=" << id << " must appear in exactly one of the two sets";
  }
}

template<typename MaskType, size_t DeletedPermille>
struct RandomMaskParams {
  using Mask = MaskType;
  static constexpr size_t kDeletedPermille = DeletedPermille;
};

using RandomMaskTypes =
  ::testing::Types<RandomMaskParams<DocumentDeletedHashMask, 10>,
                   RandomMaskParams<DocumentDeletedHashMask, 300>,
                   RandomMaskParams<DocumentDeletedHashMask, 990>,
                   RandomMaskParams<DocumentBitMask, 10>,
                   RandomMaskParams<DocumentBitMask, 300>,
                   RandomMaskParams<DocumentBitMask, 990>,
                   RandomMaskParams<DocumentAliveHashMask, 10>,
                   RandomMaskParams<DocumentAliveHashMask, 300>,
                   RandomMaskParams<DocumentAliveHashMask, 990>>;

template<typename Params>
class DocumentMaskRandomTest : public ::testing::Test {};
TYPED_TEST_SUITE(DocumentMaskRandomTest, RandomMaskTypes);

TYPED_TEST(DocumentMaskRandomTest, DocCounts) {
  constexpr size_t kDocCount = 5000;
  constexpr size_t kDeletedCount =
    kDocCount * TypeParam::kDeletedPermille / 1000;
  auto deleted_ids = RandomDeletedIds(kDocCount, kDeletedCount, 43);
  auto mask = BuildMask<typename TypeParam::Mask>(kDocCount, deleted_ids);

  EXPECT_EQ(kDocCount, mask.DocCount());
  EXPECT_EQ(kDeletedCount, mask.DeletedDocCount());
}

TYPED_TEST(DocumentMaskRandomTest, IsDeleted) {
  constexpr size_t kDocCount = 5000;
  constexpr size_t kDeletedCount =
    kDocCount * TypeParam::kDeletedPermille / 1000;
  auto deleted_ids = RandomDeletedIds(kDocCount, kDeletedCount, 43);
  const absl::flat_hash_set<doc_id_t> deleted_set(deleted_ids.begin(),
                                                  deleted_ids.end());
  auto mask = BuildMask<typename TypeParam::Mask>(kDocCount, deleted_ids);

  for (doc_id_t id = irs::doc_limits::min();
       id < irs::doc_limits::min() + kDocCount; ++id) {
    EXPECT_EQ(deleted_set.contains(id), mask.IsDeleted(id)) << "doc_id=" << id;
  }
  EXPECT_FALSE(mask.IsDeleted(0));
  EXPECT_FALSE(
    mask.IsDeleted(static_cast<doc_id_t>(irs::doc_limits::min() + kDocCount)));
}

TYPED_TEST(DocumentMaskRandomTest, ForEachDeleted) {
  constexpr size_t kDocCount = 5000;
  constexpr size_t kDeletedCount =
    kDocCount * TypeParam::kDeletedPermille / 1000;
  auto deleted_ids = RandomDeletedIds(kDocCount, kDeletedCount, 43);
  const absl::flat_hash_set<doc_id_t> deleted_set(deleted_ids.begin(),
                                                  deleted_ids.end());
  auto mask = BuildMask<typename TypeParam::Mask>(kDocCount, deleted_ids);

  absl::flat_hash_set<doc_id_t> collected;
  irs::ForEachDeleted(mask, [&](doc_id_t id) { collected.insert(id); });

  EXPECT_EQ(deleted_set, collected);
}

TYPED_TEST(DocumentMaskRandomTest, ForEachAlive) {
  constexpr size_t kDocCount = 5000;
  constexpr size_t kDeletedCount =
    kDocCount * TypeParam::kDeletedPermille / 1000;
  auto deleted_ids = RandomDeletedIds(kDocCount, kDeletedCount, 43);
  const absl::flat_hash_set<doc_id_t> deleted_set(deleted_ids.begin(),
                                                  deleted_ids.end());
  auto mask = BuildMask<typename TypeParam::Mask>(kDocCount, deleted_ids);

  absl::flat_hash_set<doc_id_t> expected_alive;
  for (doc_id_t id = irs::doc_limits::min();
       id < irs::doc_limits::min() + kDocCount; ++id) {
    if (!deleted_set.contains(id)) {
      expected_alive.insert(id);
    }
  }

  absl::flat_hash_set<doc_id_t> collected;
  irs::ForEachAlive(mask, [&](doc_id_t id) { collected.insert(id); });

  EXPECT_EQ(expected_alive, collected);
}

TYPED_TEST(DocumentMaskRandomTest, ForEachPartitionsAllDocs) {
  constexpr size_t kDocCount = 5000;
  constexpr size_t kDeletedCount =
    kDocCount * TypeParam::kDeletedPermille / 1000;
  auto deleted_ids = RandomDeletedIds(kDocCount, kDeletedCount, 7);
  auto mask = BuildMask<typename TypeParam::Mask>(kDocCount, deleted_ids);

  absl::flat_hash_set<doc_id_t> deleted_collected, alive_collected;
  irs::ForEachDeleted(mask, [&](doc_id_t id) { deleted_collected.insert(id); });
  irs::ForEachAlive(mask, [&](doc_id_t id) { alive_collected.insert(id); });

  ASSERT_EQ(kDocCount, deleted_collected.size() + alive_collected.size());
  for (doc_id_t id = irs::doc_limits::min();
       id < irs::doc_limits::min() + kDocCount; ++id) {
    EXPECT_NE(deleted_collected.contains(id), alive_collected.contains(id))
      << "doc_id=" << id << " must appear in exactly one of the two sets";
  }
}

}  // namespace
