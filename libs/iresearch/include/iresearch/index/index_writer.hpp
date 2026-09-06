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

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>
#include <string_view>
#include <yaclib/algo/wait_group.hpp>

#include "basics/async_utils.hpp"
#include "basics/noncopyable.hpp"
#include "basics/object_pool.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/column_info.hpp"

namespace duckdb {

class DatabaseInstance;
}
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader_options.hpp"
#include "iresearch/index/merge_writer.hpp"
#include "iresearch/index/segment_reader.hpp"
#include "iresearch/index/segment_writer.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class Comparer;
struct Directory;

enum OpenMode {
  kOmCreate = 1,

  kOmAppend = 2,
};

ENABLE_BITMASK_ENUM(OpenMode);

using Compaction = std::vector<const SubReader*>;
using CompactionView = std::span<const SubReader* const>;

using CompactingSegments = absl::flat_hash_set<std::string_view>;

using CompactionPolicy =
  std::function<void(Compaction& candidates, const IndexReader& index,
                     const CompactingSegments& compacting_segments)>;

enum class CompactionError : uint32_t {
  Fail = 0,

  Ok,

  Pending,

  Busy,
};

struct CompactionResult {
  size_t size{0};

  CompactionError error{CompactionError::Fail};

  operator bool() const noexcept { return error != CompactionError::Fail; }
};

struct SegmentOptions {
  size_t segment_count_max{0};

  size_t segment_memory_max{0};

  uint32_t segment_docs_max{0};
};

using ProgressReportCallback =
  std::function<void(std::string_view phase, size_t current, size_t total)>;

using PayloadProvider = std::function<bool(uint64_t, bstring&)>;

struct IndexWriterOptions : public SegmentOptions {
  IndexReaderOptions reader_options;

  PayloadProvider meta_payload_provider;

  const Comparer* comparator = nullptr;

  size_t segment_pool_size = 128;

  bool lock_repository = true;

  bool cleanup_on_open = true;

  duckdb::DatabaseInstance* db = nullptr;

  ColumnOptionsProvider column_options;
  NormColumnIdProvider norm_column_id;
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;

  IndexWriterOptions() {}
};

struct CommitInfo {
  uint64_t tick = writer_limits::kMaxTick;
  ProgressReportCallback progress;
  bool reopen_reader = false;
};

struct CommitOnFlush {
  std::atomic<uint64_t>& tick;
  bool committed = false;
};

class IndexWriter : private util::Noncopyable {
 public:
  struct SegmentContext;
  struct FlushedSegment;

 private:
  struct FlushContext;

  using FlushContextPtr =
    std::unique_ptr<FlushContext, void (*)(FlushContext*)>;

  struct ConstructToken {
    explicit ConstructToken() = default;
  };

  class ActiveSegmentContext {
   public:
    ActiveSegmentContext() = default;
    ActiveSegmentContext(
      std::shared_ptr<SegmentContext> segment,
      std::atomic_size_t& segments_active, FlushContext* flush = nullptr,
      size_t pending_segment_offset = writer_limits::kInvalidOffset) noexcept;
    ActiveSegmentContext(ActiveSegmentContext&& other) noexcept;
    ActiveSegmentContext& operator=(ActiveSegmentContext&& other) noexcept;

    ~ActiveSegmentContext();

    auto* Segment() const noexcept { return _segment.get(); }
    auto* Segment() noexcept { return _segment.get(); }
    auto* Flush() noexcept { return _flush; }

   private:
    friend struct FlushContext;

    std::shared_ptr<SegmentContext> _segment;
    std::atomic_size_t* _segments_active{nullptr};
    FlushContext* _flush{nullptr};
    size_t _pending_segment_offset{writer_limits::kInvalidOffset};
  };

  static_assert(std::is_nothrow_move_constructible_v<ActiveSegmentContext>);
  static_assert(std::is_nothrow_move_assignable_v<ActiveSegmentContext>);

  auto GetSnapshotImpl() const noexcept {
    auto reader =
      std::atomic_load_explicit(&_committed_reader, std::memory_order_acquire);
    SDB_ASSERT(reader);
    return reader;
  }

 public:
  struct QueryContext {
    using FilterPtr = std::shared_ptr<const irs::Filter>;

    QueryContext() = default;

    static constexpr uintptr_t kDone = 0;
    static constexpr uintptr_t kReplace = std::numeric_limits<uintptr_t>::max();

    QueryContext(FilterPtr filter, uint64_t tick, uintptr_t data)
      : filter{std::move(filter)}, tick{tick}, _data{data} {
      SDB_ASSERT(this->filter != nullptr);
    }
    QueryContext(const irs::Filter& filter, uint64_t tick, size_t data)
      : QueryContext{{FilterPtr{}, &filter}, tick, data} {}
    QueryContext(irs::Filter::ptr&& filter, uint64_t tick, size_t data)
      : QueryContext{FilterPtr{std::move(filter)}, tick, data} {}

    FilterPtr filter;
    uint64_t tick;

    bool IsDone() const noexcept { return _data == kDone; }
    void ForceDone() noexcept { _data = kDone; }
    void Done() noexcept {
      SDB_ASSERT(!IsDone());
      Done(this);
    }
    void DependsOn(QueryContext& query) noexcept {
      SDB_ASSERT(!IsDone());
      if (query._data == kDone) {
        Done(this);
      } else {
        SDB_ASSERT(query._data == kReplace);
        query._data = reinterpret_cast<uintptr_t>(this);
      }
    }

   private:
    uintptr_t _data{kDone};

    static void Done(QueryContext* query) noexcept {
      while (true) {
        auto next = std::exchange(query->_data, kDone);
        SDB_ASSERT(next != kDone);
        if (next == kReplace) {
          return;
        }
        query = reinterpret_cast<QueryContext*>(next);
        SDB_ASSERT(query != nullptr);
      }
    }
  };
  static_assert(std::is_nothrow_move_constructible_v<QueryContext>);

  class Document : private util::Noncopyable {
   public:
    Document(SegmentContext& segment, SegmentWriter::DocContext doc,
             doc_id_t batch_size = 1, QueryContext* query = nullptr);

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

    template<typename... Args>
    bool WithField(Args&&... args) const {
      return _writer.WithField(std::forward<Args>(args)...);
    }

    template<typename... Args>
    bool WithTokens(Args&&... args) const {
      return _writer.WithTokens(std::forward<Args>(args)...);
    }

#ifdef SDB_GTEST
    SegmentWriter& Writer() noexcept { return _writer; }
#endif

    ColWriter* GetColWriter() noexcept { return _writer.GetColWriter(); }

    doc_id_t DocId() const noexcept { return _doc_id; }

   private:
    void Finish() noexcept;

    SegmentWriter& _writer;
    QueryContext* _query;
    doc_id_t _doc_id{irs::doc_limits::eof()};
  };
  static_assert(std::is_nothrow_move_constructible_v<Document>);

  class Transaction : private util::Noncopyable {
   public:
    Transaction() = default;
    explicit Transaction(IndexWriter& writer,
                         bool exclusive_segment = false) noexcept
      : _writer{&writer}, _exclusive_segment{exclusive_segment} {}

    Transaction(Transaction&& other) = default;
    Transaction& operator=(Transaction&& other) = default;

    ~Transaction() { Abort(); }

    Document Insert(bool disable_flush = false, doc_id_t batch_size = 1,
                    CommitOnFlush* commit_on_flush = nullptr) {
      UpdateSegment(disable_flush, commit_on_flush);
      return {*_active.Segment(), SegmentWriter::DocContext{_queries},
              batch_size};
    }

    template<bool TickBound = true, typename Filter>
    void Remove(Filter&& filter) {
      UpdateSegment(true, nullptr);
      _active.Segment()->queries.emplace_back(std::forward<Filter>(filter),
                                              _queries, QueryContext::kDone);
      if constexpr (TickBound) {
        ++_queries;
      }
    }

    template<typename Filter>
    Document Replace(Filter&& filter, bool disable_flush = false) {
      UpdateSegment(disable_flush, nullptr);
      auto& segment = *_active.Segment();
      auto& query = segment.queries.emplace_back(
        std::forward<Filter>(filter), _queries, QueryContext::kReplace);
      segment.has_replace = true;
      return {segment,
              SegmentWriter::DocContext{++_queries, segment.queries.size() - 1},
              1, &query};
    }

    void Reset() noexcept;

    void RegisterFlush() noexcept;

    bool Commit() noexcept {
      auto* segment = _active.Segment();
      if (segment == nullptr) {
        return true;
      }
      if (_tick_source) {
        return CommitImpl(_tick_source(_queries + 1));
      }
      const auto first_tick =
        _writer->_tick.fetch_add(_queries, std::memory_order_relaxed);
      return CommitImpl(first_tick + _queries);
    }

    void SetTickSource(std::function<uint64_t(uint64_t)> source) noexcept {
      _tick_source = std::move(source);
    }

    bool FlushAndCommit() noexcept {
      try {
        Flush();
      } catch (...) {
        return false;
      }
      return Commit();
    }

    std::span<const FlushedSegment> FlushAndFsync();

    bool FlushAndCommit(uint64_t last_tick) noexcept {
      try {
        Flush();
      } catch (...) {
        return false;
      }
      return Commit(last_tick);
    }

    bool Commit(uint64_t last_tick) noexcept {
      auto* segment = _active.Segment();
      if (segment == nullptr) {
        return true;
      }
      return CommitImpl(last_tick);
    }

    void Abort() noexcept;

    void Flush() {
      auto* segment = _active.Segment();
      if (segment == nullptr) {
        return;
      }
      try {
        segment->Flush();
      } catch (...) {
        segment->Reset(true);
        throw;
      }
    }

    bool FlushRequired() const noexcept {
      auto* segment = _active.Segment();
      if (segment == nullptr) {
        return false;
      }
      return _writer->FlushRequired(*segment->writer);
    }

    size_t ActiveMemory() const noexcept {
      auto* segment = _active.Segment();
      if (segment == nullptr) {
        return 0;
      }
      return segment->writer->memory_active();
    }

    bool Valid() const noexcept { return _writer != nullptr; }

    uint64_t GetQueries() const noexcept { return _queries; }

    void AdvanceQueries(uint64_t n = 1) noexcept { _queries += n; }

    void SetFieldOptions(
      std::shared_ptr<const IndexFieldOptions> options) noexcept {
      _field_options = std::move(options);
    }

   private:
    bool CommitImpl(uint64_t last_tick) noexcept;
    void UpdateSegment(bool disable_flush, CommitOnFlush* commit_on_flush);

    IndexWriter* _writer{nullptr};
    ActiveSegmentContext _active;
    uint64_t _queries{0};
    std::shared_ptr<const IndexFieldOptions> _field_options;
    std::function<uint64_t(uint64_t)> _tick_source;
    bool _exclusive_segment{false};
  };
  static_assert(std::is_nothrow_move_constructible_v<Transaction>);
  static_assert(std::is_nothrow_move_assignable_v<Transaction>);

  Transaction GetBatch(bool exclusive_segment = false) noexcept {
    return Transaction{*this, exclusive_segment};
  }

  using ptr = std::shared_ptr<IndexWriter>;

  static constexpr std::string_view kWriteLockName = "write.lock";

  ~IndexWriter() noexcept;

  auto GetSnapshot() const noexcept {
    return DirectoryReader{GetSnapshotImpl()};
  }

  uint64_t BufferedDocs() const;

  bool HasActiveSegments() const noexcept {
    return _segments_active.load(std::memory_order_acquire) != 0;
  }

  void Clear(uint64_t tick = writer_limits::kMinTick);

  CompactionResult Compact(const CompactionPolicy& policy,
                           const IndexFieldOptions* field_options = nullptr,
                           Format::ptr codec = nullptr,
                           const MergeWriter::FlushProgress& progress = {});

  bool AdoptSegment(std::string_view meta_file, const Format::ptr& codec,
                    uint64_t tick);

  bool Import(const IndexReader& reader, Format::ptr codec = nullptr,
              const MergeWriter::FlushProgress& progress = {});

  static IndexWriter::ptr Make(Directory& dir, Format::ptr codec, OpenMode mode,
                               const IndexWriterOptions& opts = {});

  void Options(const SegmentOptions& opts) noexcept { _segment_limits = opts; }

  const Comparer* Comparator() const noexcept { return _comparator; }

  bool RefreshBegin(const CommitInfo& info = {}) {
    _commit_lock.ForgetDeadlockInfo();
    std::lock_guard lock{_commit_lock};
    return Start(info);
  }

  void RefreshAbort() {
    _commit_lock.ForgetDeadlockInfo();
    std::lock_guard lock{_commit_lock};
    Abort();
  }

  bool RefreshCommit(const CommitInfo& info = {}) {
    _commit_lock.ForgetDeadlockInfo();
    std::lock_guard lock{_commit_lock};
    const bool modified = Start(info);
    Finish();
    return modified;
  }

  bool FlushRequired(const SegmentWriter& writer) const noexcept;

  IndexWriter(ConstructToken, IndexLock::ptr&& lock,
              IndexFileRefs::ref_t&& lock_file_ref, Directory& dir,
              Format::ptr codec, size_t segment_pool_size,
              const SegmentOptions& segment_limits, const Comparer* comparator,
              const PayloadProvider& meta_payload_provider,
              std::shared_ptr<const DirectoryReaderImpl>&& committed_reader);

 private:
  struct CompactionContext : util::Noncopyable {
    std::shared_ptr<const DirectoryReaderImpl> compaction_reader;
    Compaction candidates;
    std::optional<MergeWriter> merger;
  };

  static_assert(std::is_nothrow_move_constructible_v<CompactionContext>);

  struct ImportContext {
    ImportContext(
      IndexSegment&& segment, uint64_t tick, FileRefs&& refs,
      Compaction&& compaction_candidates,
      std::shared_ptr<const SegmentReaderImpl>&& reader,
      std::shared_ptr<const DirectoryReaderImpl>&& compaction_reader,
      MergeWriter&& merger) noexcept
      : tick{tick},
        segment{std::move(segment)},
        refs{std::move(refs)},
        reader{std::move(reader)},
        compaction_ctx{.compaction_reader = std::move(compaction_reader),
                       .candidates = std::move(compaction_candidates),
                       .merger = std::move(merger)} {}

    ImportContext(
      IndexSegment&& segment, uint64_t tick, FileRefs&& refs,
      Compaction&& compaction_candidates,
      std::shared_ptr<const SegmentReaderImpl>&& reader,
      std::shared_ptr<const DirectoryReaderImpl>&& compaction_reader) noexcept
      : tick{tick},
        segment{std::move(segment)},
        refs{std::move(refs)},
        reader{std::move(reader)},
        compaction_ctx{.compaction_reader = std::move(compaction_reader),
                       .candidates = std::move(compaction_candidates)} {}

    ImportContext(IndexSegment&& segment, uint64_t tick, FileRefs&& refs,
                  std::shared_ptr<const SegmentReaderImpl>&& reader) noexcept
      : tick{tick},
        segment{std::move(segment)},
        refs{std::move(refs)},
        reader{std::move(reader)} {}

    ImportContext(ImportContext&&) = default;

    ImportContext& operator=(const ImportContext&) = delete;
    ImportContext& operator=(ImportContext&&) = delete;

    uint64_t tick;
    IndexSegment segment;
    FileRefs refs;
    std::shared_ptr<const SegmentReaderImpl> reader;
    CompactionContext compaction_ctx;
  };

  static_assert(std::is_nothrow_move_constructible_v<ImportContext>);

 public:
  struct FlushedSegment : public IndexSegment {
    FlushedSegment() = default;
    explicit FlushedSegment(IndexSegment&& segment, DocMap&& old2new,
                            DocsMask&& docs_mask, size_t docs_begin) noexcept
      : IndexSegment{std::move(segment)},
        old2new{std::move(old2new)},
        docs_mask{std::move(docs_mask)},
        document_mask{{this->docs_mask.set.get_allocator()}},
        _docs_begin{docs_begin},
        _docs_end{_docs_begin + meta.docs_count} {}

    size_t GetDocsBegin() const noexcept { return _docs_begin; }
    size_t GetDocsEnd() const noexcept { return _docs_end; }

    bool SetCommitted(size_t committed) noexcept {
      SDB_ASSERT(GetDocsBegin() <= committed);
      SDB_ASSERT(committed < GetDocsEnd());
      _docs_end = committed;
      return _docs_begin != committed;
    }

    DocMap old2new;
    DocMap new2old;
    DocsMask docs_mask;
    DocumentMask document_mask;
    bool was_flush = false;
    bool meta_on_disk = false;

   private:
    size_t _docs_begin;
    size_t _docs_end;
  };

  struct SegmentContext {
    using segment_meta_generator_t = std::function<SegmentMeta()>;
    using ptr = std::unique_ptr<SegmentContext>;

    std::atomic_size_t buffered_docs{0};
    RefTrackingDirectory dir;

    ManagedVector<QueryContext> queries;
    ManagedVector<FlushedSegment> flushed;
    ManagedVector<SegmentWriter::DocContext> flushed_docs;

    segment_meta_generator_t meta_generator;

    size_t flushed_queries{0};
    size_t committed_queries{0};
    size_t committed_buffered_docs{0};
    size_t committed_flushed_docs{0};

    uint64_t first_tick{writer_limits::kMaxTick};
    uint64_t last_tick{writer_limits::kMinTick};

    std::unique_ptr<SegmentWriter> writer;
    IndexSegment writer_meta;
    bool has_replace{false};

    static std::unique_ptr<SegmentContext> make(
      Directory& dir, segment_meta_generator_t&& meta_generator,
      const SegmentWriterOptions& options);

    SegmentContext(Directory& dir, segment_meta_generator_t&& meta_generator,
                   const SegmentWriterOptions& options);

    void Rollback() noexcept;

    void Commit(uint64_t queries, uint64_t last_tick);

    void Flush();

    void Prepare();

    void Reset(bool store_flushed = false) noexcept;
  };

 private:
  struct SegmentLimits {
   private:
    static constexpr auto kSizeMax = std::numeric_limits<size_t>::max();
    static constexpr auto kDocsMax = std::numeric_limits<uint32_t>::max() - 2;
    static auto ZeroMax(auto value, auto max) noexcept {
      return std::min(value - 1, max - 1) + 1;
    }

    static void Assign(auto& to, auto value, auto max) noexcept {
      to.store(ZeroMax(value, max), std::memory_order_relaxed);
    }

    std::atomic_uint32_t _docs;
    std::atomic_size_t _count;
    std::atomic_size_t _memory;

   public:
    explicit SegmentLimits(const SegmentOptions& opts) noexcept
      : _docs{ZeroMax(opts.segment_docs_max, kDocsMax)},
        _count{ZeroMax(opts.segment_count_max, kSizeMax)},
        _memory{ZeroMax(opts.segment_memory_max, kSizeMax)} {}

    SegmentLimits& operator=(const SegmentOptions& opts) noexcept {
      Assign(_docs, opts.segment_docs_max, kDocsMax);
      Assign(_count, opts.segment_count_max, kSizeMax);
      Assign(_memory, opts.segment_memory_max, kSizeMax);
      return *this;
    }

    auto Docs() const noexcept { return _docs.load(std::memory_order_relaxed); }
    auto Count() const noexcept {
      return _count.load(std::memory_order_relaxed);
    }
    auto Memory() const noexcept {
      return _memory.load(std::memory_order_relaxed);
    }
  };

  using SegmentPool = UnboundedObjectPool<SegmentContext>;
  using Freelist = ConcurrentStack<size_t>;

  struct PendingSegmentContext : public Freelist::NodeType {
    std::shared_ptr<SegmentContext> segment;

    PendingSegmentContext(std::shared_ptr<SegmentContext> segment,
                          size_t pending_segment_context_offset)
      : Freelist::NodeType{.value = pending_segment_context_offset},
        segment{std::move(segment)} {
      SDB_ASSERT(this->segment != nullptr);
    }
  };

  using CachedReaders =
    absl::flat_hash_map<FlushedSegment*,
                        std::shared_ptr<const SegmentReaderImpl>>;

  struct FlushContext {
    RefTrackingDirectory::ptr dir;
    absl::Mutex context_mutex;
    FlushContext* next{nullptr};

    std::vector<std::shared_ptr<SegmentContext>> segments;
    CachedReaders cached;

    std::vector<ImportContext> imports;

    void ClearPending() noexcept {
      while (pending_freelist.pop() != nullptr) {
      }
      pending_segments.clear();
    }

    std::deque<PendingSegmentContext> pending_segments;
    Freelist pending_freelist;
    yaclib::WaitGroup<> pending{1};
    absl::Mutex pending_mutex;

    CompactingSegments segment_mask;

    FlushContext() = default;

    ~FlushContext() noexcept { Reset(); }

    void Emplace(ActiveSegmentContext&& active);

    void AddToPending(ActiveSegmentContext& active);

    uint64_t FlushPending(uint64_t committed_tick, uint64_t tick);

    void Reset() noexcept;
  };

  void Cleanup(FlushContext& curr, FlushContext* next = nullptr) noexcept;

  struct PendingBase {
    FlushContextPtr ctx{nullptr, nullptr};
    uint64_t tick{writer_limits::kMinTick};

    void StartReset(IndexWriter& writer, bool keep_next = false) noexcept {
      auto* curr = ctx.get();
      if (curr != nullptr) {
        std::lock_guard lock{writer._compacting.lock};
        writer.Cleanup(*curr, keep_next ? nullptr : curr->next);
      }
    }
  };

  struct PendingContext : PendingBase {
    IndexMeta meta;
    std::vector<SegmentReader> readers;
    std::vector<std::string_view> files_to_sync;

    bool Empty() const noexcept { return !ctx; }
  };

  static_assert(std::is_nothrow_move_constructible_v<PendingContext>);
  static_assert(std::is_nothrow_move_assignable_v<PendingContext>);

  struct PendingState : PendingBase {
    std::shared_ptr<const DirectoryReaderImpl> commit;

    bool Valid() const noexcept { return ctx && commit; }

    void FinishReset() noexcept {
      ctx.reset();
      commit.reset();
    }

    void Reset(IndexWriter& writer) noexcept {
      StartReset(writer);
      FinishReset();
    }
  };

  static_assert(std::is_nothrow_move_constructible_v<PendingState>);
  static_assert(std::is_nothrow_move_assignable_v<PendingState>);

  PendingContext PrepareFlush(const CommitInfo& info);
  void ApplyFlush(PendingContext&& context);

  FlushContextPtr GetFlushContext() const noexcept;
  FlushContextPtr SwitchFlushContext() noexcept;

  ActiveSegmentContext GetSegmentContext(bool exclusive = false);

  SegmentWriterOptions GetSegmentWriterOptions(
    bool compaction, const IndexFieldOptions* field_options) const noexcept;

  uint64_t NextSegmentId() noexcept;
  uint64_t CurrentSegmentId() const noexcept;
  void InitMeta(IndexMeta& meta, uint64_t tick) const;

  bool Start(const CommitInfo& info);
  void Finish();
  void Abort() noexcept;

  IndexFeatures _score_bound_features{};
  ScorerPtr _topk_scorer;
  duckdb::DatabaseInstance* _db = nullptr;
  std::shared_ptr<const IndexFieldOptions> _field_options;
  PayloadProvider _meta_payload_provider;
  const Comparer* _comparator;
  Format::ptr _codec;
  absl::Mutex _commit_lock;
  struct {
    std::recursive_mutex lock;
    CompactingSegments segments;
  } _compacting;
  Directory& _dir;
  std::atomic<FlushContext*> _flush_context;
  std::shared_ptr<const DirectoryReaderImpl> _committed_reader;
  PendingState _pending_state;
  SegmentLimits _segment_limits;
  SegmentPool _segment_writer_pool;
  std::atomic_size_t _segments_active{0};
  std::atomic_uint64_t _seg_counter;
  std::atomic_uint64_t _tick{writer_limits::kMinTick + 1};
  uint64_t _committed_tick{writer_limits::kMinTick};
  uint64_t _last_gen;
  IndexMetaWriter::ptr _writer;
  IndexLock::ptr _write_lock;
  IndexFileRefs::ref_t _write_lock_file_ref;
  std::array<FlushContext, 2> _flush_contexts;
};

}  // namespace irs
