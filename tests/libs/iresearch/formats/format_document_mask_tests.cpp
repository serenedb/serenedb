#include <gtest/gtest.h>

#include "iresearch/formats/segment_meta_reader.hpp"
#include "iresearch/formats/segment_meta_writer.hpp"
#include "iresearch/index/document_mask.hpp"
#include "iresearch/store/memory_directory.hpp"

namespace {

irs::DocumentDeletedHashMask MakeDeletedHashMask(
  size_t doc_count, const std::vector<irs::doc_id_t>& deleted_ids) {
  irs::DocumentDeletedHashMask mask{irs::IResourceManager::gNoop, doc_count,
                                    deleted_ids.size()};
  for (auto id : deleted_ids) {
    mask.Store(id);
  }
  return mask;
}

void WriteHeader(irs::MemoryIndexOutput& out, size_t doc_count,
                 size_t deleted_count) {
  out.WriteV32(static_cast<uint32_t>(doc_count));
  out.WriteV32(static_cast<uint32_t>(deleted_count));
}

}  // namespace

TEST(FormatDocumentMask, DeletedVarintListRoundtrip) {
  constexpr size_t kDocCount = 1000;
  auto mask = MakeDeletedHashMask(kDocCount, {1, 68, 42, 133, 700, 2, 999});

  irs::MemoryDirectory dir;
  irs::MemoryFile file{irs::IResourceManager::gNoop};
  {
    irs::MemoryIndexOutput out{file};
    WriteHeader(out, kDocCount, mask.DeletedDocCount());
    irs::WriteDocumentMaskDeletedVarintList(dir, out, mask);
    out.Flush();
  }

  irs::MemoryIndexInput in{file};
  auto [read_mask, _] = irs::ReadDocumentMask(in, irs::IResourceManager::gNoop);

  ASSERT_NE(nullptr, read_mask);
  ASSERT_EQ(mask, *read_mask);
}

TEST(FormatDocumentMask, AliveVarintListRoundtrip) {
  constexpr size_t kDocCount = 1000;
  irs::DocumentDeletedHashMask big_mask{irs::IResourceManager::gNoop, kDocCount,
                                        kDocCount - 3};
  for (irs::doc_id_t id = irs::doc_limits::min();
       id < irs::doc_limits::min() + kDocCount; ++id) {
    if (id != 12 && id != 167 && id != 1000) {
      big_mask.Store(id);
    }
  }

  irs::MemoryDirectory dir;
  irs::MemoryFile file{irs::IResourceManager::gNoop};
  {
    irs::MemoryIndexOutput out{file};
    WriteHeader(out, kDocCount, big_mask.DeletedDocCount());
    irs::WriteDocumentMaskAliveVarintList(dir, out, big_mask);
    out.Flush();
  }

  irs::MemoryIndexInput in{file};
  auto [read_mask, _] = irs::ReadDocumentMask(in, irs::IResourceManager::gNoop);

  ASSERT_NE(nullptr, read_mask);
  ASSERT_EQ(big_mask, *read_mask);
}

TEST(FormatDocumentMask, DenseBitsetRoundtrip) {
  constexpr size_t kDocCount = 1000;
  const std::vector<irs::doc_id_t> deleted_ids{
    1, 7, 13, 64, 65, 127, 128, 255, 256, 512, 513, 999, 1000,
  };
  auto mask = MakeDeletedHashMask(kDocCount, deleted_ids);

  irs::MemoryDirectory dir;
  irs::MemoryFile file{irs::IResourceManager::gNoop};
  {
    irs::MemoryIndexOutput out{file};
    WriteHeader(out, kDocCount, mask.DeletedDocCount());
    irs::WriteDocumentMaskDenseBitset(dir, out, mask);
    out.Flush();
  }

  irs::MemoryIndexInput in{file};
  auto [read_mask, _] = irs::ReadDocumentMask(in, irs::IResourceManager::gNoop);

  ASSERT_NE(nullptr, read_mask);
  ASSERT_EQ(mask, *read_mask);
}
