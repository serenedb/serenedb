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
#include <string_view>

#include "basics/async_utils.hpp"
#include "basics/noncopyable.hpp"
#include "basics/object_pool.hpp"
#include "basics/thread_utils.hpp"
#include "basics/wait_group.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/column_info.hpp"
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

// Defines how index writer should be opened
enum OpenMode {
  // Creates new index repository. In case if repository already
  // exists, all contents will be cleared.
  kOmCreate = 1,

  // Opens existing index repository. In case if repository does not
  // exists, error will be generated.
  kOmAppend = 2,
};

ENABLE_BITMASK_ENUM(OpenMode);

// A set of candidates denoting an instance of consolidation
using Consolidation = std::vector<const SubReader*>;
using ConsolidationView = std::span<const SubReader* const>;

// segments that are under consolidation
using ConsolidatingSegments = absl::flat_hash_set<std::string_view>;

// Mark consolidation candidate segments matching the current policy
// candidates the segments that should be consolidated
// in: segment candidates that may be considered by this policy
// out: actual segments selected by the current policy
// dir the segment directory
// meta the index meta containing segments to be considered
// Consolidating_segments segments that are currently in progress
// of consolidation
// Final candidates are all segments selected by at least some policy
using ConsolidationPolicy =
  std::function<void(Consolidation& candidates, const IndexReader& index,
                     const ConsolidatingSegments& consolidating_segments)>;

enum class ConsolidationError : uint32_t {
  // Consolidation failed
  Fail = 0,

  // Consolidation successfully finished
  Ok,

  // Consolidation was scheduled for the upcoming commit
  Pending,
};

// Represents result of a consolidation
struct ConsolidationResult {
  // Number of candidates
  size_t size{0};

  // Error code
  ConsolidationError error{ConsolidationError::Fail};

  // intentionally implicit
  operator bool() const noexcept { return error != ConsolidationError::Fail; }
};

// Options the the writer should use for segments
struct SegmentOptions {
  // Segment acquisition requests will block and wait for free segments
  // after this many segments have been acquired e.g. via GetBatch()
  // 0 == unlimited
  size_t segment_count_max{0};

  // Flush the segment to the repository after its in-memory size
  // grows beyond this byte limit, in-flight documents will still be
  // written to the segment before flush
  // 0 == unlimited
  size_t segment_memory_max{0};

  // Flush the segment to the repository after its total document
  // count (live + masked) grows beyond this byte limit, in-flight
  // documents will still be written to the segment before flush
  // 0 == unlimited
  uint32_t segment_docs_max{0};
};

// Progress report callback types for commits.
using ProgressReportCallback =
  std::function<void(std::string_view phase, size_t current, size_t total)>;

// Functor for creating payload. Operation tick is provided for
// payload generation.
using PayloadProvider = std::function<bool(uint64_t, bstring&)>;

// Options the the writer should use after creation
struct IndexWriterOptions : public SegmentOptions {
  // Options for snapshot management
  IndexReaderOptions reader_options;

  // Returns column info for a feature the writer should use for
  // columnstore
  FeatureInfoProvider features;

  // Returns column info the writer should use for columnstore
  ColumnInfoProvider column_info;

  // Provides payload for index_meta created by writer
  PayloadProvider meta_payload_provider;

  // Comparator defines physical order of documents in each segment
  // produced by an index_writer.
  // empty == use default system sorting order
  const Comparer* comparator{nullptr};

  // Number of free segments cached in the segment pool for reuse
  // 0 == do not cache any segments, i.e. always create new segments
  size_t segment_pool_size{128};  // arbitrary size

  // Acquire an exclusive lock on the repository to guard against index
  // corruption from multiple index_writers
  bool lock_repository{true};

  IndexWriterOptions() {}  // compiler requires non-default definition
};

struct CommitInfo {
  uint64_t tick = writer_limits::kMaxTick;
  ProgressReportCallback progress;
  bool reopen_columnstore = false;
};

// The object is using for indexing data. Only one writer can write to
// the same directory simultaneously.
// Thread safe.
class IndexWriter : private util::Noncopyable {
 public:
  struct SegmentContext;

 private:
  struct FlushContext;

  using FlushContextPtr =
    std::unique_ptr<FlushContext, void (*)(FlushContext*)>;

  // Disallow using public constructor
  struct ConstructToken {
    explicit ConstructToken() = default;
  };

  class ActiveSegmentContext {
   public:
    ActiveSegmentContext() = default;
    ActiveSegmentContext(
      std::shared_ptr<SegmentContext> segment,
      std::atomic_size_t& segments_active,
      // the FlushContext the SegmentContext is currently registered with
      FlushContext* flush = nullptr,
      // the segment offset in flush->pending_segments_
      size_t pending_segment_offset = writer_limits::kInvalidOffset) noexcept;
    ActiveSegmentContext(ActiveSegmentContext&& other) noexcept;
    ActiveSegmentContext& operator=(ActiveSegmentContext&& other) noexcept;

    ~ActiveSegmentContext();

    auto* Segment() const noexcept { return _segment.get(); }
    auto* Segment() noexcept { return _segment.get(); }
    auto* Flush() noexcept { return _flush; }

   private:
    friend struct FlushContext;  // for FlushContext::AddToPending(...)

    std::shared_ptr<SegmentContext> _segment;
    // reference to IndexWriter::segments_active_
    std::atomic_size_t* _segments_active{nullptr};
    // nullptr will not match any FlushContext
    FlushContext* _flush{nullptr};
    // segment offset in flush_->pending_segments_
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
  // Additional information required for remove/replace requests
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

    // keep a handle to the filter for the case when this object has ownership
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

  // A context allowing index modification operations.
  // The object is non-thread-safe, each thread should use its own
  // separate instance.
  class Document : private util::Noncopyable {
   public:
    Document(SegmentContext& segment, SegmentWriter::DocContext doc,
             doc_id_t batch_size = 1, QueryContext* query = nullptr);

    Document(Document&&) = default;
    Document& operator=(Document&&) = delete;

    ~Document() noexcept;

    // Start completely new field batch and start filling it from first document
    // in the insert batch
    void NextFieldBatch() noexcept { _doc_id = _writer.FirstBatchDocId(); }

    // End of field batch for current document, move to next document in batch
    void NextDocument() noexcept {
      Finish();
      _writer.ResetNorms();
      ++_doc_id;
    }

    // Return current state of the object
    // Note that if the object is in an invalid state all further operations
    // will not take any effect
    explicit operator bool() const noexcept { return _writer.valid(); }

    // Inserts the specified field into the document according to the
    // specified ACTION
    // Note that 'Field' type type must satisfy the Field concept
    // field attribute to be inserted
    // Return true, if field was successfully inserted
    template<Action A, typename Field>
    bool Insert(Field&& field) const {
      return _writer.insert<A>(std::forward<Field>(field), _doc_id);
    }

    // Inserts the specified field (denoted by the pointer) into the
    //        document according to the specified ACTION
    // Note that 'Field' type type must satisfy the Field concept
    // Note that pointer must not be nullptr
    // field attribute to be inserted
    // Return true, if field was successfully inserted
    template<Action A, typename Field>
    bool Insert(Field* field) const {
      return _writer.insert<A>(*field, _doc_id);
    }

    // Inserts the specified range of fields, denoted by the [begin;end)
    // into the document according to the specified ACTION
    // Note that 'Iterator' underline value type must satisfy the Field concept
    // begin the beginning of the fields range
    // end the end of the fields range
    // Return true, if the range was successfully inserted
    template<Action A, typename Iterator>
    bool Insert(Iterator begin, Iterator end) const {
      for (; _writer.valid() && begin != end; ++begin) {
        Insert<A>(*begin);
      }

      return _writer.valid();
    }
#ifdef SDB_GTEST
    SegmentWriter& Writer() noexcept { return _writer; }
#endif

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
    explicit Transaction(IndexWriter& writer) noexcept : _writer{&writer} {}

    Transaction(Transaction&& other) = default;
    Transaction& operator=(Transaction&& other) = default;

    ~Transaction() {
      // FIXME(gnusi): consider calling Abort in future
      // Commit can throw in such case -> better error handling
      Commit();
    }

    // Create a document to filled by the caller
    // for insertion into the index index
    // applied upon return value deallocation
    // `disable_flush` don't trigger segment flush
    //
    // The changes are not visible until commit()
    // Transaction should be valid
    Document Insert(bool disable_flush = false, doc_id_t batch_size = 1) {
      UpdateSegment(disable_flush);
      return {*_active.Segment(), SegmentWriter::DocContext{_queries},
              batch_size};
    }

    // Marks all documents matching the filter for removal.
    // TickBound - Remove filter usage is restricted by document creation tick.
    // Filter the filter selecting which documents should be removed.
    // Note that changes are not visible until commit().
    // Note that filter must be valid until commit().
    // Remove</*TickBound=*/false> is applied even for documents created after
    // the Remove call and until next TickBound Remove or Replace.
    // Transaction should be valid
    template<bool TickBound = true, typename Filter>
    void Remove(Filter&& filter) {
      UpdateSegment(/*disable_flush=*/true);
      _active.Segment()->queries.emplace_back(std::forward<Filter>(filter),
                                              _queries, QueryContext::kDone);
      if constexpr (TickBound) {
        ++_queries;
      }
    }

    // Create a document to filled by the caller
    // for replacement of existing documents already in the index
    // matching filter with the filled document
    // applied upon return value deallocation
    // filter the filter selecting which documents should be replaced
    // Note the changes are not visible until commit()
    // Note that filter must be valid until commit()
    // Transaction should be valid
    template<typename Filter>
    Document Replace(Filter&& filter, bool disable_flush = false) {
      UpdateSegment(disable_flush);
      auto& segment = *_active.Segment();
      auto& query = segment.queries.emplace_back(
        std::forward<Filter>(filter), _queries, QueryContext::kReplace);
      segment.has_replace = true;
      return {segment,
              SegmentWriter::DocContext{++_queries, segment.queries.size() - 1},
              1,  // Replace is only for single document
              &query};
    }

    // Revert all pending document modifications and release resources
    // noexcept because all insertions reserve enough space for rollback
    void Reset() noexcept;

    // Register underlying segment to be flushed with the upcoming index commit
    void RegisterFlush();

    // Commit all accumulated modifications and release resources
    // return successful or not, if not call Abort
    bool Commit() noexcept {
      auto* segment = _active.Segment();
      if (segment == nullptr) {
        return true;
      }
      const auto first_tick =
        _writer->_tick.fetch_add(_queries, std::memory_order_relaxed);
      return CommitImpl(first_tick + _queries);
    }

    bool Commit(uint64_t last_tick) noexcept {
      auto* segment = _active.Segment();
      if (segment == nullptr) {
        return true;
      }
      return CommitImpl(last_tick);
    }

    // Reset all accumulated modifications and release resources
    void Abort() noexcept;

    bool FlushRequired() const noexcept {
      auto* segment = _active.Segment();
      if (segment == nullptr) {
        return false;
      }
      return _writer->FlushRequired(*segment->writer);
    }

    bool Valid() const noexcept { return _writer != nullptr; }

   private:
    bool CommitImpl(uint64_t last_tick) noexcept;
    // refresh segment if required (guarded by FlushContext::context_mutex_)
    // is is thread-safe to use ctx_/segment_ while holding 'flush_context_ptr'
    // since active 'flush_context' will not change and hence no reload required
    void UpdateSegment(bool disable_flush);

    IndexWriter* _writer{nullptr};
    // the segment_context used for storing changes (lazy-initialized)
    ActiveSegmentContext _active;
    // We can use active_.Segment()->queries_.size() for same purpose
    uint64_t _queries{0};
  };
  static_assert(std::is_nothrow_move_constructible_v<Transaction>);
  static_assert(std::is_nothrow_move_assignable_v<Transaction>);

  // Returns a context allowing index modification operations
  // All document insertions will be applied to the same segment on a
  // best effort basis, e.g. a flush_all() will cause a segment switch
  Transaction GetBatch() noexcept { return Transaction{*this}; }

  using ptr = std::shared_ptr<IndexWriter>;

  // Name of the lock for index repository
  static constexpr std::string_view kWriteLockName = "write.lock";

  ~IndexWriter() noexcept;

  // Returns current index snapshot
  auto GetSnapshot() const noexcept {
    return DirectoryReader{GetSnapshotImpl()};
  }

  // Returns overall number of buffered documents in a writer
  uint64_t BufferedDocs() const;

  // Clears the existing index repository by staring an empty index.
  // Previously opened readers still remain valid.
  // truncate transaction tick
  // Call will rollback any opened transaction.
  void Clear(uint64_t tick = writer_limits::kMinTick);

  // Merges segments accepted by the specified defragment policy into
  // a new segment. For all accepted segments frees the space occupied
  // by the documents marked as deleted and deduplicate terms.
  // Policy the specified defragmentation policy
  // Codec desired format that will be used for segment creation,
  // nullptr == use index_writer's codec
  // Progress callback triggered for consolidation steps, if the
  // callback returns false then consolidation is aborted
  // For deferred policies during the commit stage each policy will be
  // given the exact same index_meta containing all segments in the
  // commit, however, the resulting acceptor will only be segments not
  // yet marked for consolidation by other policies in the same commit
  ConsolidationResult Consolidate(
    const ConsolidationPolicy& policy, Format::ptr codec = nullptr,
    const MergeWriter::FlushProgress& progress = {});

  // Imports index from the specified index reader into new segment
  // Reader the index reader to import.
  // Desired format that will be used for segment creation,
  // nullptr == use index_writer's codec.
  // Progress callback triggered for consolidation steps, if the
  // callback returns false then consolidation is aborted.
  // Returns true on success.
  bool Import(const IndexReader& reader, Format::ptr codec = nullptr,
              const MergeWriter::FlushProgress& progress = {});

  // Opens new index writer.
  // dir directory where index will be should reside
  // codec format that will be used for creating new index segments
  // mode specifies how to open a writer
  // options the configuration parameters for the writer
  static IndexWriter::ptr Make(Directory& dir, Format::ptr codec, OpenMode mode,
                               const IndexWriterOptions& opts = {});

  // Modify the runtime segment options as per the specified values
  // options will apply no later than after the next commit()
  void Options(const SegmentOptions& opts) noexcept { _segment_limits = opts; }

  // Returns comparator using for sorting documents by a primary key
  // nullptr == default sort order
  const Comparer* Comparator() const noexcept { return _comparator; }

  // Begins the two-phase transaction.
  // payload arbitrary user supplied data to store in the index
  // Returns true if transaction has been successfully started.

  bool Begin(const CommitInfo& info = {}) {
    _commit_lock.ForgetDeadlockInfo();
    std::lock_guard lock{_commit_lock};
    return Start(info);
  }

  // Rollbacks the two-phase transaction
  void Rollback() {
    _commit_lock.ForgetDeadlockInfo();
    std::lock_guard lock{_commit_lock};
    Abort();
  }

  // Make all buffered changes visible for readers.
  // payload arbitrary user supplied data to store in the index
  // Return whether any changes were committed.
  //
  // Note that if begin() has been already called commit() is
  // relatively lightweight operation.
  // FIXME(gnusi): Commit() should return committed index snapshot
  bool Commit(const CommitInfo& info = {}) {
    _commit_lock.ForgetDeadlockInfo();
    std::lock_guard lock{_commit_lock};
    const bool modified = Start(info);
    Finish();
    return modified;
  }

  // Returns field features.
  const FeatureInfoProvider& FeatureInfo() const noexcept {
    return _feature_info;
  }

  bool FlushRequired(const SegmentWriter& writer) const noexcept;

  // public because we want to use std::make_shared
  IndexWriter(ConstructToken, IndexLock::ptr&& lock,
              IndexFileRefs::ref_t&& lock_file_ref, Directory& dir,
              Format::ptr codec, size_t segment_pool_size,
              const SegmentOptions& segment_limits, const Comparer* comparator,
              const ColumnInfoProvider& column_info,
              const FeatureInfoProvider& feature_info,
              const PayloadProvider& meta_payload_provider,
              std::shared_ptr<const DirectoryReaderImpl>&& committed_reader);

 private:
  struct ConsolidationContext : util::Noncopyable {
    std::shared_ptr<const DirectoryReaderImpl> consolidation_reader;
    Consolidation candidates;
    MergeWriter merger;
  };

  static_assert(std::is_nothrow_move_constructible_v<ConsolidationContext>);

  struct ImportContext {
    ImportContext(
      IndexSegment&& segment, uint64_t tick, FileRefs&& refs,
      Consolidation&& consolidation_candidates,
      std::shared_ptr<const SegmentReaderImpl>&& reader,
      std::shared_ptr<const DirectoryReaderImpl>&& consolidation_reader,
      MergeWriter&& merger) noexcept
      : tick{tick},
        segment{std::move(segment)},
        refs{std::move(refs)},
        reader{std::move(reader)},
        consolidation_ctx{
          .consolidation_reader = std::move(consolidation_reader),
          .candidates = std::move(consolidation_candidates),
          .merger = std::move(merger)} {}

    ImportContext(IndexSegment&& segment, uint64_t tick, FileRefs&& refs,
                  std::shared_ptr<const SegmentReaderImpl>&& reader,
                  IResourceManager& resource_manager) noexcept
      : tick{tick},
        segment{std::move(segment)},
        refs{std::move(refs)},
        reader{std::move(reader)},
        consolidation_ctx{.merger{resource_manager}} {}

    ImportContext(ImportContext&&) = default;

    ImportContext& operator=(const ImportContext&) = delete;
    ImportContext& operator=(ImportContext&&) = delete;

    uint64_t tick;
    IndexSegment segment;
    FileRefs refs;
    std::shared_ptr<const SegmentReaderImpl> reader;
    ConsolidationContext consolidation_ctx;
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
    // Flushed segment removals
    DocsMask docs_mask;
    DocumentMask document_mask;
    bool was_flush = false;

   private:
    // starting doc_id that should be added to docs_mask
    // TODO(mbkkt) Better to remove, but only after parallel Commit
    size_t _docs_begin;
    size_t _docs_end;
  };

  // The segment writer and its associated ref tracking directory
  // for use with an unbounded_object_pool
  struct SegmentContext {
    using segment_meta_generator_t = std::function<SegmentMeta()>;
    using ptr = std::unique_ptr<SegmentContext>;

    // for use with index_writer::buffered_docs(), asynchronous call
    std::atomic_size_t buffered_docs{0};
    // ref tracking for SegmentWriter to allow for easy ref removal on
    // SegmentWriter reset
    RefTrackingDirectory dir;

    // sequential list of pending modification
    ManagedVector<QueryContext> queries;
    // all of the previously flushed versions of this segment
    ManagedVector<FlushedSegment> flushed;
    // update_contexts to use with 'flushed_'
    // sequentially increasing through all offsets
    // (sequential doc_id in 'flushed_' == offset + doc_limits::min(), size()
    // == sum of all 'flushed_'.'docs_count')
    ManagedVector<SegmentWriter::DocContext> flushed_docs;

    // function to get new SegmentMeta from
    segment_meta_generator_t meta_generator;

    size_t flushed_queries{0};
    // Transaction::Commit was not called for these:
    size_t committed_queries{0};
    size_t committed_buffered_docs{0};
    size_t committed_flushed_docs{0};

    uint64_t first_tick{writer_limits::kMaxTick};
    uint64_t last_tick{writer_limits::kMinTick};

    std::unique_ptr<SegmentWriter> writer;
    // the SegmentMeta this writer was initialized with
    IndexSegment writer_meta;
    // TODO(mbkkt) Better to be per FlushedSegment
    bool has_replace{false};

    static std::unique_ptr<SegmentContext> make(
      Directory& dir, segment_meta_generator_t&& meta_generator,
      const SegmentWriterOptions& options);

    SegmentContext(Directory& dir, segment_meta_generator_t&& meta_generator,
                   const SegmentWriterOptions& options);

    void Rollback() noexcept;

    void Commit(uint64_t queries, uint64_t last_tick);

    // Flush current writer state into a materialized segment.
    // Return tick of last committed transaction.
    void Flush();

    // Ensure writer is ready to receive documents
    void Prepare();

    // Reset segment state to the initial state
    // store_flushed should store info about flushed segments?
    // Note should be true if something went wrong during segment flush
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

    // see segment_options::max_segment_docs
    std::atomic_uint32_t _docs;
    // see segment_options::max_segment_count
    std::atomic_size_t _count;
    // see segment_options::max_segment_memory
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
  // 'value' == node offset into 'pending_segment_context_'
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

  // The context containing data collected for the next commit() call
  // Note a 'segment_context' is tracked by at most 1 'flush_context', it is
  // the job of the 'documents_context' to guarantee that the
  // 'segment_context' is not used once the tracker 'flush_context' is no
  // longer active.
  struct FlushContext {
    // ref tracking directory used by this context
    // (tracks all/only refs for this context)
    RefTrackingDirectory::ptr dir;
    // guard for the current context during flush
    // (write) operations vs update (read)
    absl::Mutex context_mutex;
    // the next context to switch to
    FlushContext* next{nullptr};

    std::vector<std::shared_ptr<SegmentContext>> segments;
    CachedReaders cached;

    // complete segments to be added during next commit (import)
    std::vector<ImportContext> imports;

    void ClearPending() noexcept {
      while (pending_freelist.pop() != nullptr) {
      }
      pending_segments.clear();
    }

    // segment writers with data pending for next commit
    // (all segments that have been used by this flush_context)
    // must be std::deque to guarantee that element memory location does
    // not change for use with 'pending_segment_contexts_freelist_'
    std::deque<PendingSegmentContext> pending_segments;
    // entries from 'pending_segments_' that are available for reuse
    Freelist pending_freelist;
    WaitGroup pending;

    // set of segments to be removed from the index upon commit
    ConsolidatingSegments segment_mask;

    FlushContext() = default;

    ~FlushContext() noexcept { Reset(); }

    // release segment to this FlushContext
    void Emplace(ActiveSegmentContext&& active);

    // add the segment to this flush_context pending segments
    // but not to freelist. So this segment would be waited upon flushing
    void AddToPending(ActiveSegmentContext& active);

    uint64_t FlushPending(uint64_t committed_tick, uint64_t tick);

    void Reset() noexcept;
  };

  void Cleanup(FlushContext& curr, FlushContext* next = nullptr) noexcept;

  struct PendingBase {
    // Reference to flush context held until end of commit
    FlushContextPtr ctx{nullptr, nullptr};
    uint64_t tick{writer_limits::kMinTick};

    void StartReset(IndexWriter& writer, bool keep_next = false) noexcept {
      auto* curr = ctx.get();
      if (curr != nullptr) {
        std::lock_guard lock{writer._consolidating.lock};
        writer.Cleanup(*curr, keep_next ? nullptr : curr->next);
      }
    }
  };

  struct PendingContext : PendingBase {
    // Index meta of the next commit
    IndexMeta meta;
    // Segment readers of the next commit
    std::vector<SegmentReader> readers;
    // Files to sync
    std::vector<std::string_view> files_to_sync;

    bool Empty() const noexcept { return !ctx; }
  };

  static_assert(std::is_nothrow_move_constructible_v<PendingContext>);
  static_assert(std::is_nothrow_move_assignable_v<PendingContext>);

  struct PendingState : PendingBase {
    // meta + references of next commit
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

  // Return a usable segment or a nullptr segment if retry is required
  // (e.g. no free segments available)
  ActiveSegmentContext GetSegmentContext();

  // Return options for SegmentWriter
  SegmentWriterOptions GetSegmentWriterOptions(
    bool consolidation) const noexcept;

  // Return next segment identifier
  uint64_t NextSegmentId() noexcept;
  // Return current segment identifier
  uint64_t CurrentSegmentId() const noexcept;
  // Initialize new index meta
  void InitMeta(IndexMeta& meta, uint64_t tick) const;

  // Start transaction
  bool Start(const CommitInfo& info);
  // Finish transaction
  void Finish();
  // Abort transaction
  void Abort() noexcept;

  IndexFeatures _wand_features{};  // Set of features required for wand
  ScorersView _wand_scorers;
  FeatureInfoProvider _feature_info;
  ColumnInfoProvider _column_info;
  PayloadProvider _meta_payload_provider;  // provides payload for new segments
  const Comparer* _comparator;
  Format::ptr _codec;
  // Prevent concurrent Begin/Commit/Rollback/Clear and multiple Consolidate
  absl::Mutex _commit_lock;
  struct {
    std::recursive_mutex lock;  // TODO(mbkkt) make it absl::Mutex
    // It's recursive because our tests, where consolidation policy calls commit
    ConsolidatingSegments segments;  // segments that are under consolidation
  } _consolidating;
  // directory used for initialization of readers
  Directory& _dir;
  // currently active context accumulating data to be
  // processed during the next flush
  std::atomic<FlushContext*> _flush_context;
  // latest/active index snapshot
  std::shared_ptr<const DirectoryReaderImpl> _committed_reader;
  // current state awaiting commit completion
  PendingState _pending_state;
  // limits for use with respect to segments
  SegmentLimits _segment_limits;
  // a cache of segments available for reuse
  SegmentPool _segment_writer_pool;
  // number of segments currently in use by the writer
  std::atomic_size_t _segments_active{0};
  std::atomic_uint64_t _seg_counter;  // segment counter
  // current modification/update tick
  std::atomic_uint64_t _tick{writer_limits::kMinTick + 1};
  uint64_t _committed_tick{writer_limits::kMinTick};
  // last committed index meta generation. Not related to ticks!
  uint64_t _last_gen;
  IndexMetaWriter::ptr _writer;
  IndexLock::ptr _write_lock;  // exclusive write lock for directory
  IndexFileRefs::ref_t _write_lock_file_ref;  // file ref for lock file
  // Should be last to be destroyed first
  // Flushed contexts, while one commiting another writing
  // TODO(mbkkt) Code maybe not ready to more than 2 FlushContext.
  std::array<FlushContext, 2> _flush_contexts;
};

}  // namespace irs
