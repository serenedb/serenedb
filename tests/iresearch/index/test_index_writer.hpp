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

#pragma once

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <iresearch/formats/hnsw/hnsw_bases.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge_writer.hpp>
#include <iresearch/store/directory.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

namespace tests {

enum OpenMode : uint8_t {
  kOmCreate = 1,
  kOmAppend = 2,
};

constexpr OpenMode operator|(OpenMode lhs, OpenMode rhs) noexcept {
  return static_cast<OpenMode>(static_cast<uint8_t>(lhs) |
                               static_cast<uint8_t>(rhs));
}

constexpr OpenMode& operator|=(OpenMode& lhs, OpenMode rhs) noexcept {
  return lhs = lhs | rhs;
}

enum class CompactionError : uint32_t {
  Fail = 0,
  Ok,
  Pending,
  Busy,
};

struct CompactionResult {
  size_t size = 0;
  CompactionError error = CompactionError::Fail;

  operator bool() const noexcept { return error != CompactionError::Fail; }
};

using LazyRemoval = std::function<std::shared_ptr<const irs::DocRemoval>(
  const irs::DirectoryReader&)>;

class IndexWriter {
  struct Span {
    uint64_t seq = 0;
    uint64_t first = 0;
    uint64_t last = 0;
  };

  struct OpenSegment {
    irs::IndexWriter::Transaction trx;
    uint64_t docs = 0;
    std::vector<Span> spans;
    bool held = false;
  };

  struct Removal {
    std::shared_ptr<const irs::DocRemoval> docs;
    LazyRemoval lazy;
    uint64_t bound = 0;
  };

 public:
  using ptr = std::shared_ptr<IndexWriter>;
  using Document = irs::IndexWriter::Document;

  class Transaction {
   public:
    Transaction() = default;
    Transaction(IndexWriter& owner, bool exclusive) noexcept
      : _owner{&owner}, _exclusive{exclusive} {}
    Transaction(Transaction&& other) noexcept;
    Transaction& operator=(Transaction&& other) noexcept;
    ~Transaction() { Abort(); }

    Document Insert(bool = false, irs::doc_id_t batch_size = 1);
    void Remove(std::shared_ptr<const irs::DocRemoval> removal) {
      _removals.push_back({.docs = std::move(removal), .bound = _inserted});
    }
    void Remove(LazyRemoval removal) {
      _removals.push_back({.lazy = std::move(removal), .bound = _inserted});
    }
    bool Commit();
    void Abort() noexcept;
    bool Valid() const noexcept { return _owner != nullptr; }
    irs::IndexWriter::Transaction& Native();
    std::span<const irs::IndexWriter::FlushedSegment> FlushAndFsync() {
      return Native().FlushAndFsync();
    }
    void SetFieldOptions(
      std::shared_ptr<const irs::IndexFieldOptions> options) noexcept;

   private:
    IndexWriter* _owner = nullptr;
    bool _exclusive = false;
    irs::IndexWriter::Transaction _native;
    OpenSegment* _segment = nullptr;
    uint64_t _first = 0;
    uint64_t _inserted = 0;
    bool _touched = false;
    std::shared_ptr<const irs::IndexFieldOptions> _field_options;
    std::vector<Removal> _removals;
  };

  static ptr Make(irs::Directory& dir, OpenMode mode,
                  irs::IndexWriterOptions options = {});

  IndexWriter(irs::Directory& dir, OpenMode mode,
              irs::IndexWriterOptions options);

  Transaction GetBatch(bool exclusive = false) {
    return Transaction{*this, exclusive};
  }

  bool RefreshBegin();
  void RefreshFinish();
  void RefreshAbort();
  bool RefreshCommit();
  bool Commit() { return RefreshCommit(); }

  irs::DirectoryReader GetSnapshot() const;

  CompactionResult Compact(
    const irs::CompactionPolicy& policy,
    const irs::IndexFieldOptions* field_options = nullptr,
    const irs::MergeWriter::FlushProgress& progress = {});

  void Options(const irs::SegmentOptions& options) noexcept {
    _writer->Options(options);
  }
  irs::IndexWriter::CompactionFloorGuard ClaimCompactionFloor() {
    return _writer->ClaimCompactionFloor();
  }
  auto MergeAsync(auto&&... args) {
    return _writer->MergeAsync(std::forward<decltype(args)>(args)...);
  }
  uint64_t CurrentSegmentId() const noexcept {
    return _writer->CurrentSegmentId();
  }
  irs::IndexWriter& Native() noexcept { return *_writer; }

 private:
  struct Change {
    uint64_t seq = 0;
    std::vector<irs::SegmentMeta> added;
    irs::FileRefs pins;
    std::vector<Removal> removals;
  };

  struct Flushed {
    std::vector<irs::SegmentMeta> metas;
    std::vector<uint64_t> starts;
    std::vector<Span> spans;
    irs::FileRefs pins;
  };

  struct Staged {
    std::vector<irs::SegmentMeta> segments;
    uint64_t cut = 0;
    std::vector<size_t> flushed;
    size_t merges = 0;
  };

  OpenSegment& Acquire() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void FlushFree() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  std::vector<irs::SegmentMeta> Apply(
    std::vector<irs::SegmentMeta> segments, std::span<const Change> changes,
    std::span<const Flushed* const> flushed,
    std::span<irs::IndexWriter::Merged> merges) const;

  irs::Directory& _dir;
  std::unique_ptr<irs::HnswBases> _hnsw_bases;
  irs::IndexReaderOptions _reader_options;
  irs::IndexWriter::ptr _writer;
  mutable absl::Mutex _mutex;
  uint64_t _seq ABSL_GUARDED_BY(_mutex) = 0;
  irs::FileRefs _pins ABSL_GUARDED_BY(_mutex);
  std::vector<std::unique_ptr<OpenSegment>> _open ABSL_GUARDED_BY(_mutex);
  std::vector<Flushed> _flushed ABSL_GUARDED_BY(_mutex);
  std::vector<Change> _pending ABSL_GUARDED_BY(_mutex);
  std::vector<irs::IndexWriter::Merged> _merges ABSL_GUARDED_BY(_mutex);
  std::vector<irs::IndexWriter::Merged> _retired ABSL_GUARDED_BY(_mutex);
  std::atomic<std::thread::id> _policy_thread;
  std::optional<Staged> _staged ABSL_GUARDED_BY(_mutex);
};

irs::DirectoryReader OpenReader(const irs::Directory& dir,
                                const irs::IndexReaderOptions& options = {});

}  // namespace tests
