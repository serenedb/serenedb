////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>
#include <yaclib/async/future.hpp>

#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/doc_removal.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/merge_writer.hpp"
#include "iresearch/index/segment_reader.hpp"
#include "iresearch/index/segment_writer.hpp"
#include "iresearch/index/segment_writer_options.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "iresearch/utils/object_pool.hpp"

namespace duckdb {

class DatabaseInstance;
}

namespace irs {

struct Directory;

using Compaction = std::vector<const SubReader*>;
using CompactionView = std::span<const SubReader* const>;

using CompactingSegments = absl::flat_hash_set<std::string_view>;

using CompactionPolicy =
  std::function<void(Compaction& candidates, const IndexReader& index,
                     const CompactingSegments& compacting_segments)>;

struct SegmentOptions {
  size_t segment_memory_max{0};

  uint32_t segment_docs_max{0};
};

struct IndexWriterOptions : public SegmentOptions {
  IndexReaderOptions reader_options;

  size_t segment_pool_size = 128;

  bool cleanup_on_open = true;

  duckdb::DatabaseInstance* db = nullptr;

  ColumnOptionsProvider column_options;
  NormColumnIdProvider norm_column_id;
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  AnnFieldsByColumn anns;

  const AnnBuildEnv* ann_env = nullptr;

  IndexWriterOptions() {}
};

class IndexWriter : private util::Noncopyable {
  struct ConstructToken {
    explicit ConstructToken() = default;
  };

 public:
  struct SegmentContext;

  struct FlushedSegment {
    SegmentMeta meta;
    DocumentMaskBuilder docs_mask;
  };

  class Document : private util::Noncopyable {
   public:
    Document(SegmentContext& segment, doc_id_t batch_size = 1);

    Document(Document&&) = default;
    Document& operator=(Document&&) = delete;

    ~Document() noexcept;

    void NextFieldBatch() noexcept { _doc_id = _writer.FirstBatchDocId(); }

#ifdef SDB_GTEST
    void NextDocument() noexcept {
      Finish();
      ++_doc_id;
    }
#endif

    explicit operator bool() const noexcept { return _writer.valid(); }

    std::string_view Error() const noexcept { return _writer.Error(); }

    template<typename... Args>
    bool WithField(Args&&... args) const {
      return _writer.WithField(std::forward<Args>(args)...);
    }

    template<typename... Args>
    bool WithMemberDocs(Args&&... args) const {
      return _writer.WithMemberDocs(std::forward<Args>(args)...);
    }

    template<typename... Args>
    bool WithTokens(Args&&... args) const {
      return _writer.WithTokens(std::forward<Args>(args)...);
    }

    template<typename... Args>
    bool WithEntryTokens(Args&&... args) const {
      return _writer.WithEntryTokens(std::forward<Args>(args)...);
    }

#ifdef SDB_GTEST
    SegmentWriter& Writer() noexcept { return _writer; }
#endif

    ColWriter* GetColWriter() const noexcept { return _writer.GetColWriter(); }

    doc_id_t DocId() const noexcept { return _doc_id; }

   private:
    void Finish() noexcept;

    SegmentWriter& _writer;
    doc_id_t _doc_id{irs::doc_limits::eof()};
  };
  static_assert(std::is_nothrow_move_constructible_v<Document>);

  class Transaction : private util::Noncopyable {
   public:
    Transaction() = default;
    explicit Transaction(IndexWriter& writer) noexcept : _writer{&writer} {}

    Transaction(Transaction&& other) noexcept
      : _writer{std::exchange(other._writer, nullptr)},
        _segment{std::move(other._segment)},
        _hot_segment{other._hot_segment},
        _field_options{std::move(other._field_options)},
        _hot{other._hot} {}
    Transaction& operator=(Transaction&& other) noexcept {
      if (this != &other) {
        Abort();
        _writer = std::exchange(other._writer, nullptr);
        _segment = std::move(other._segment);
        _hot_segment = other._hot_segment;
        _field_options = std::move(other._field_options);
        _hot = other._hot;
      }
      return *this;
    }

    ~Transaction() { Abort(); }

    Document Insert(doc_id_t batch_size = 1) {
      UpdateSegment(batch_size);
      return {*_segment, batch_size};
    }

    std::span<const FlushedSegment> FlushAndFsync() {
      return FlushSegments(true);
    }

    std::span<const FlushedSegment> FlushSegments(bool sync);

    std::shared_ptr<const SegmentReaderImpl> SealPiece(uint64_t id);

    void CopyRows(const ColReader& source, std::span<const field_id> columns,
                  uint64_t first, uint64_t count) {
      SDB_ASSERT(_segment);
      _segment->writer->CopyRows(source, columns, first, count);
    }

    void Abort() noexcept;

    void Flush();

    bool Valid() const noexcept { return _writer != nullptr; }

    void SetFieldOptions(
      std::shared_ptr<const IndexFieldOptions> options) noexcept {
      _field_options = std::move(options);
    }

    void SetHot(uint64_t segment_id) noexcept;

    const SegmentWriter* HotWriter() const noexcept {
      return _segment ? _segment->writer.get() : nullptr;
    }

   private:
    void UpdateSegment(doc_id_t batch_size);

    IndexWriter* _writer{nullptr};
    std::shared_ptr<SegmentContext> _segment;
    uint64_t _hot_segment{0};
    std::shared_ptr<const IndexFieldOptions> _field_options;
    bool _hot{false};
  };
  static_assert(std::is_nothrow_move_constructible_v<Transaction>);
  static_assert(std::is_nothrow_move_assignable_v<Transaction>);

  Transaction GetBatch() noexcept { return Transaction{*this}; }

  using ptr = std::shared_ptr<IndexWriter>;

  struct MergeClaim : private util::Noncopyable {
    MergeClaim() = default;
    MergeClaim(IndexWriter& owner, Compaction&& claimed) noexcept
      : writer{&owner}, candidates{std::move(claimed)} {}
    MergeClaim(MergeClaim&& other) noexcept
      : writer{std::exchange(other.writer, nullptr)},
        candidates{std::move(other.candidates)} {}
    MergeClaim& operator=(MergeClaim&& other) noexcept {
      if (this != &other) {
        Release();
        writer = std::exchange(other.writer, nullptr);
        candidates = std::move(other.candidates);
      }
      return *this;
    }
    ~MergeClaim() { Release(); }

    void Release() noexcept;

    IndexWriter* writer = nullptr;
    Compaction candidates;
  };

  struct Merged {
    DirectoryReader source;
    MergeClaim claim;
    SegmentMeta meta;
    std::shared_ptr<const SegmentReaderImpl> reader;
    std::vector<DocMap> maps;
    FileRefs refs;
  };

  auto MergeAsync(const CompactionPolicy& policy, DirectoryReader source,
                  const IndexFieldOptions* field_options,
                  const MergeWriter::FlushProgress& progress,
                  const AnnBuildEnv* env) -> yaclib::Future<Merged>;

  auto WriteAsync(DirectoryReader source, uint64_t id,
                  const IndexFieldOptions* field_options,
                  const MergeWriter::FlushProgress& progress,
                  const AnnBuildEnv* env) -> yaclib::Future<Merged>;

  Merged MergePieces(DirectoryReader source, uint64_t id,
                     const IndexFieldOptions* field_options);

  class [[nodiscard]] CompactionFloorGuard : private util::Noncopyable {
   public:
    CompactionFloorGuard() = default;
    CompactionFloorGuard(IndexWriter& writer, uint64_t floor) noexcept
      : _writer{&writer}, _floor{floor} {}
    CompactionFloorGuard(CompactionFloorGuard&& rhs) noexcept
      : _writer{std::exchange(rhs._writer, nullptr)}, _floor{rhs._floor} {}
    CompactionFloorGuard& operator=(CompactionFloorGuard&& rhs) noexcept {
      if (this != &rhs) {
        Release();
        _writer = std::exchange(rhs._writer, nullptr);
        _floor = rhs._floor;
      }
      return *this;
    }
    ~CompactionFloorGuard() { Release(); }

    bool Arm() noexcept;
    uint64_t Floor() const noexcept { return _floor; }

   private:
    void Release() noexcept;

    IndexWriter* _writer = nullptr;
    uint64_t _floor = 0;
  };

  CompactionFloorGuard ClaimCompactionFloor();
  bool Compacting();

  uint64_t NextSegmentId() noexcept;
  uint64_t CurrentSegmentId() const noexcept;
  void ReserveSegmentId(uint64_t id) noexcept;

  struct Replacement {
    SegmentMeta meta;
    std::vector<DocMap> absorbed;
  };

  static IndexWriter::ptr Make(Directory& dir, IndexWriterOptions opts = {});

  void Options(const SegmentOptions& opts) noexcept { _segment_limits = opts; }

  bool FlushRequired(const SegmentWriter& writer,
                     doc_id_t batch_size) const noexcept;

  IndexWriter(ConstructToken, Directory& dir, size_t segment_pool_size,
              const SegmentOptions& segment_limits,
              const IndexReaderOptions& reader_options);

  struct SegmentContext {
    using ptr = std::unique_ptr<SegmentContext>;

    RefTrackingDirectory dir;
    ManagedVector<FlushedSegment> flushed;
    std::unique_ptr<SegmentWriter> writer;
    SegmentMeta writer_meta;

    static ptr make(Directory& dir, const SegmentWriterOptions& options);

    SegmentContext(Directory& dir, const SegmentWriterOptions& options);

    void Flush();

    void Prepare(uint64_t segment_id);

    void Reset() noexcept;
  };

 private:
  struct SegmentLimits {
   private:
    static constexpr auto kSizeMax = std::numeric_limits<size_t>::max();
    static constexpr auto kDocsMax = doc_limits::kMaxDocs;
    static auto ZeroMax(auto value, auto max) noexcept {
      return std::min(value - 1, max - 1) + 1;
    }

    static void Assign(auto& to, auto value, auto max) noexcept {
      to.store(ZeroMax(value, max), std::memory_order_relaxed);
    }

    std::atomic_uint32_t _docs;
    std::atomic_size_t _memory;

   public:
    explicit SegmentLimits(const SegmentOptions& opts) noexcept
      : _docs{ZeroMax(opts.segment_docs_max, kDocsMax)},
        _memory{ZeroMax(opts.segment_memory_max, kSizeMax)} {}

    SegmentLimits& operator=(const SegmentOptions& opts) noexcept {
      Assign(_docs, opts.segment_docs_max, kDocsMax);
      Assign(_memory, opts.segment_memory_max, kSizeMax);
      return *this;
    }

    auto Docs() const noexcept { return _docs.load(std::memory_order_relaxed); }
    auto Memory() const noexcept {
      return _memory.load(std::memory_order_relaxed);
    }
  };

  using SegmentPool = UnboundedObjectPool<SegmentContext>;

  std::shared_ptr<SegmentContext> GetSegmentContext();

  SegmentWriterOptions GetSegmentWriterOptions(
    bool compaction, const IndexFieldOptions* field_options) const noexcept;

  auto FlushMerge(Merged merged, uint64_t id,
                  const IndexFieldOptions* field_options,
                  const MergeWriter::FlushProgress& progress,
                  const AnnBuildEnv* env) -> yaclib::Future<Merged>;

  ScorerPtr _topk_scorer;
  duckdb::DatabaseInstance* _db = nullptr;
  const AnnBuildEnv* _ann_env = nullptr;
  std::unique_ptr<HnswBases> _hnsw_bases;
  std::shared_ptr<const IndexFieldOptions> _field_options;
  static constexpr uint64_t kPendingFloor =
    std::numeric_limits<uint64_t>::max();

  struct {
    absl::Mutex lock;
    CompactingSegments segments;
    uint64_t floor = 0;
  } _compacting;
  Directory& _dir;
  IndexReaderOptions _reader_options;
  SegmentLimits _segment_limits;
  SegmentPool _segment_writer_pool;
  std::atomic_uint64_t _seg_counter{0};
};

}  // namespace irs
