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

#include "search/store.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/directory_cleaner.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/store/overlay_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "catalog/catalog.h"
#include "catalog/record.h"
#include "catalog/transaction.h"
#include "connector/column_id.h"
#include "connector/constraint_keys.h"
#include "connector/search_sink_writer.hpp"
#include "connector/term_dict.h"
#include "pg/connection_context.h"
#include "scheduler/background_scheduler.h"
#include "search/log_entry.h"
#include "search/scorer_options.h"
#include "search/task.h"
#include "search/version_chain.tpp"
#include "server/utils/lifecycle.h"
#include "storage_engine/search_engine.h"
#include "utils/file_utils.h"

namespace sdb::search {
namespace {

catalog::CompressionByColumn DeclaredCompression(
  const duckdb::ColumnList& columns) {
  catalog::CompressionByColumn compression;
  for (const auto& column : columns.Logical()) {
    if (column.CompressionType() != duckdb::CompressionType::COMPRESSION_AUTO) {
      compression.emplace(connector::TableColumnId(column),
                          column.CompressionType());
    }
  }
  return compression;
}

irs::containers::FlatHashSet<irs::field_id> TableColumns(
  const duckdb::ColumnList& columns) {
  irs::containers::FlatHashSet<irs::field_id> ids;
  for (const auto& column : columns.Physical()) {
    ids.insert(connector::TableColumnId(column));
  }
  return ids;
}

template<typename Config>
std::vector<Config> WithConfig(std::vector<Config> configs, Config config) {
  const auto it = absl::c_lower_bound(
    configs, config.oid,
    [](const Config& other, duckdb::idx_t oid) { return other.oid < oid; });
  if (it != configs.end() && it->oid == config.oid) {
    *it = std::move(config);
  } else {
    configs.insert(it, std::move(config));
  }
  return configs;
}

uint64_t MillisSince(std::chrono::steady_clock::time_point begin) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::steady_clock::now() - begin)
    .count();
}

absl::Status RemoveUnreferenced(irs::Directory& dir, duckdb::idx_t id,
                                bool orphans) {
  try {
    if (orphans) {
      irs::directory_utils::RemoveAllUnreferenced(dir);
    } else {
      duckdb::TaskScheduler::BlockingScope blocking;
      irs::DirectoryCleaner::clean(dir);
    }
  } catch (const std::exception& e) {
    return absl::InternalError(
      absl::StrCat("caught exception while cleaning up ", id, ": ",
                   duckdb::ErrorData{e}.RawMessage()));
  }
  return absl::OkStatus();
}

struct RecoveryTask final : duckdb::BaseExecutorTask {
  RecoveryTask(duckdb::TaskExecutor& executor, absl::AnyInvocable<void()> run)
    : BaseExecutorTask{executor}, run{std::move(run)} {}

  void ExecuteTask() final { run(); }

  std::string TaskType() const final { return "SearchRecovery"; }

  absl::AnyInvocable<void()> run;
};

}  // namespace

Store::Store(std::shared_ptr<catalog::DatabaseDirectory> directory,
             bool in_memory, bool read_only, duckdb::idx_t db_id,
             duckdb::idx_t id, const catalog::InvertedIndexSettings& options,
             const std::optional<irs::ScorerOptions>& top_k_scorer, bool is_new)
  : _id{id},
    _db_id{db_id},
    _directory{std::move(directory)},
    _in_memory{in_memory},
    _read_only{read_only},
    _is_new{is_new},
    _segment_memory_max{options.segment_memory_max},
    _segment_docs_max{options.segment_docs_max},
    _topk_options{top_k_scorer} {
  SDB_ASSERT(id != 0);
  SetTasksSettings(options);
  if (_topk_options) {
    _topk_scorer = MakeScorer(*_topk_options);
  }
  if (is_new) {
    Open();
  }
}

Store::Store(std::shared_ptr<catalog::DatabaseDirectory> directory,
             bool in_memory, bool read_only, duckdb::idx_t db_id,
             duckdb::idx_t id, bool is_new,
             const catalog::SearchTableOptions& options,
             const duckdb::ColumnList& columns)
  : _id{id},
    _db_id{db_id},
    _directory{std::move(directory)},
    _in_memory{in_memory},
    _read_only{read_only},
    _is_new{is_new},
    _segment_memory_max{options.segment_memory_max},
    _table_columns{TableColumns(columns)},
    _row_group_size{options.row_group_size},
    _block_size{options.block_size},
    _compression{DeclaredCompression(columns)} {
  if (!options.optimize_top_k.empty()) {
    _topk_options = ParseScorerExpression(nullptr, options.optimize_top_k);
    _topk_scorer = MakeScorer(*_topk_options);
  }
  SetTasksSettings(options);
  RebuildConfig(std::vector<IndexConfig>{}, std::nullopt);
}

Store::~Store() {
  _table.reset();
  _writer.reset();
  _dir.reset();
  if (OwnerDropped()) {
    catalog::DatabaseDirectory::RemoveStorage(std::move(_directory), _id);
  }
}

StorageDirectory OpenStorageDirectory(
  const catalog::DatabaseDirectory& database, duckdb::idx_t oid, bool is_new,
  bool in_memory, const irs::ResourceManagementOptions& resources) {
  std::optional<std::filesystem::path> path;
  if (!in_memory) {
    path = is_new ? database.CreateStorage(oid) : database.OpenStorage(oid);
  }
  if (!path) {
    return {.directory = std::make_unique<irs::MemoryDirectory>(
              irs::DirectoryAttributes{}, resources),
            .absent = !in_memory};
  }
  return {.directory = std::make_unique<irs::MMapDirectory>(
            *path, irs::DirectoryAttributes{}, resources),
          .on_disk = true};
}

irs::IndexReaderOptions Store::ReaderOptions() const {
  irs::IndexReaderOptions options;
  options.db = &irs::DuckDBEngine::Instance().instance();
  options.scorer = _topk_scorer.get();
  return options;
}

void Store::Open() {
  auto opened = OpenStorageDirectory(*_directory, _id, _is_new, _in_memory,
                                     irs::ResourceManagementOptions{});
  _dir = std::move(opened.directory);
  _absent = opened.absent;
  _on_disk = opened.on_disk;
  if (_read_only && _on_disk) {
    _dir = std::make_unique<irs::OverlayDirectory>(std::move(_dir));
  }

  irs::IndexWriterOptions writer_options;
  writer_options.ann_env = &AnnBuildEnv();
  writer_options.segment_memory_max = _segment_memory_max;
  writer_options.segment_docs_max = _segment_docs_max;
  writer_options.reader_options = ReaderOptions();
  writer_options.db = writer_options.reader_options.db;
  writer_options.cleanup_on_open = _is_new;
  if (!IsSearchTable()) {
    SDB_IF_FAILURE("segment_1000_docs_max") {
      writer_options.segment_docs_max = 1000;
    }
  }

  _writer = irs::IndexWriter::Make(*_dir, std::move(writer_options));
  {
    absl::MutexLock lock{&_layout_mutex};
    _table =
      std::make_unique<Table>(*_dir, ReaderOptions(), _layout.published_config);
  }
  if (IsSearchTable() && _writer) {
    _piece_dir = std::make_unique<irs::MemoryDirectory>();
    irs::IndexWriterOptions piece_options;
    piece_options.ann_env = &AnnBuildEnv();
    piece_options.reader_options = ReaderOptions();
    piece_options.db = piece_options.reader_options.db;
    _piece_writer =
      irs::IndexWriter::Make(*_piece_dir, std::move(piece_options));
  }
}

void Store::EnsureOpen() {
  if (!Opened()) {
    Open();
  }
}

void Store::ClaimColumnOids(const irs::DirectoryReader& reader) const {
  auto& db_manager =
    duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance());
  const auto claim = [&](irs::field_id id) {
    if (id <= connector::kMaxRealColumnIdValue) {
      db_manager.ClaimOid(id);
    }
  };
  for (const auto& segment : reader) {
    for (const auto id : segment.field_ids()) {
      claim(id);
    }
    if (const auto* columns = segment.GetColReader()) {
      for (const auto& column : columns->Columns()) {
        claim(column->Id());
      }
    }
  }
}

void Store::SyncSegments() const {
  if (_on_disk) {
    utils::file_utils::SyncDirectory(Path());
  }
}

void Store::PruneVersions(std::span<const Tick> running, Tick published) {
  if (_table) {
    _table->Versions().Prune(running, published);
  }
}

bool Store::FreeVersions() {
  return _table && _table->Versions().FreeRetired();
}

irs::DirectoryReader Store::BuildSnapshot() {
  SDB_ASSERT(_table);
  WriteOut();
  return _table->ColdView();
}

bool Store::Settle() {
  if (!_table || !_table->Unsettled()) {
    return false;
  }
  WriteOut();
  return true;
}

std::vector<OwnSegment> Store::OpenFlushed(
  std::span<const irs::IndexWriter::FlushedSegment> flushed) const {
  std::vector<OwnSegment> segments;
  segments.reserve(flushed.size());
  for (const auto& entry : flushed) {
    segments.push_back(OpenOwn(entry.meta));
  }
  return segments;
}

OwnSegment Store::OpenOwn(irs::SegmentMeta meta) const {
  auto reader = irs::SegmentReaderImpl::Open(*_dir, meta, ReaderOptions());
  return {.meta = std::move(meta), .reader = std::move(reader)};
}

Store::Piece Store::StartPiece() {
  SDB_ASSERT(_piece_writer);
  Piece piece{.trx = _piece_writer->GetBatch(), .id = _writer->NextSegmentId()};
  piece.trx.SetHot(piece.id);
  return piece;
}

void Store::PiecesAdded() {
  if ((_table->PieceBytes() + _table->PatchBytes() < _segment_memory_max &&
       !_table->HasMergeRun()) ||
      !BackgroundScheduler::gInstance ||
      _piece_requests.fetch_add(1, std::memory_order_acq_rel) != 0) {
    return;
  }
  BackgroundScheduler::instance()
    .Run([weak = weak_from_this()] {
      const auto store = weak.lock();
      if (!store) {
        return;
      }
      auto requests = store->_piece_requests.load(std::memory_order_acquire);
      do {
        try {
          store->MaintainPieces();
        } catch (const std::exception& e) {
          store->_piece_requests.store(0, std::memory_order_release);
          SDB_WARN(SEARCH, "table ", store->_id,
                   ": writing out the rows it holds in memory failed: ",
                   duckdb::ErrorData{e}.RawMessage());
          return;
        }
        requests = store->_piece_requests.fetch_sub(requests,
                                                    std::memory_order_acq_rel) -
                   requests;
      } while (requests != 0);
    })
    .Detach();
}

void Store::MaintainPieces() {
  if (!_piece_writer || OwnerDropped()) {
    return;
  }
  for (;;) {
    {
      absl::MutexLock lock{&_write_out_mutex};
      const auto pieces = _table->PieceBytes();
      const auto patches = _table->PatchBytes();
      if (pieces + patches < _segment_memory_max) {
        const auto run = _table->MergeRun();
        if (run.empty() || !MergePiecesLocked(run)) {
          return;
        }
        continue;
      }
      if (pieces >= patches) {
        WriteOutLocked();
        continue;
      }
    }
    const auto patches = _table->PatchBytes();
    if (!FoldPatches() || _table->PatchBytes() >= patches) {
      return;
    }
  }
}

bool Store::FoldPatches() {
  if (!_table || OwnerDropped()) {
    return false;
  }
  std::string target;
  uint64_t most = 0;
  for (const auto& unit : _table->State()->units) {
    if (unit->piece) {
      continue;
    }
    uint64_t bytes = 0;
    for (const auto& patch : unit->patches) {
      bytes += patch.patch->Bytes();
    }
    if (bytes > most) {
      most = bytes;
      target = unit->meta.name;
    }
  }
  const auto options = CompactionOptions();
  if (target.empty() || !options) {
    return false;
  }
  const irs::CompactionPolicy fold =
    [&target](irs::Compaction& candidates, const irs::IndexReader& reader,
              const irs::CompactingSegments& busy) {
      for (size_t i = 0; i < reader.size(); ++i) {
        if (reader[i].Meta().name == target && !busy.contains(target)) {
          candidates.emplace_back(&reader[i]);
          return;
        }
      }
    };
  bool empty = false;
  const auto folded = irs::GetBlocking(CompactUnsafeAsync(
    ClaimMaintenance(), fold, [] { return true; }, empty, options.get(),
    &AnnBuildEnv()));
  return folded.res.ok() && !empty;
}

bool Store::MergePiecesLocked(
  std::span<const std::shared_ptr<const TableUnit>> pieces) {
  const auto field_options = CompactionOptions();
  SDB_ASSERT(field_options);
  auto source = _table->PieceView(pieces, /*keep_deleted=*/true);
  const auto id = _writer->NextSegmentId();
  uint64_t docs = 0;
  for (const auto& piece : pieces) {
    docs += piece->docs;
  }
  irs::IndexWriter::Merged merged;
  if (docs < field_options->row_group_size) {
    merged =
      _piece_writer->MergePieces(std::move(source), id, field_options.get());
  } else {
    merged = irs::GetBlocking(_piece_writer->WriteAsync(
      std::move(source), id, field_options.get(), [] { return true; },
      &AnnBuildEnv()));
    auto& refs = _piece_dir->attributes().refs();
    for (const auto& file :
         irs::SegmentFiles(merged.meta.name, merged.meta.kinds)) {
      _piece_dir->remove(file);
      refs.remove(file);
    }
  }
  if (!merged.reader) {
    return false;
  }
  std::vector<uint64_t> ids;
  std::vector<Tick> folded;
  ids.reserve(pieces.size());
  folded.reserve(pieces.size());
  for (const auto& piece : pieces) {
    ids.push_back(piece->id);
    folded.push_back(piece->reader->PatchTick());
  }
  std::vector<std::shared_ptr<TableUnit>> added;
  added.push_back(PieceUnit(std::move(merged.reader), pieces.front()->first));
  CommitReplace(ids, folded, std::move(added), std::move(merged.maps), 0,
                /*logged=*/false);
  SDB_PARK_ONCE_ON_FAILURE("pause_search_pieces_merged");
  return true;
}

std::shared_ptr<const TableState> Store::CommitReplace(
  std::span<const uint64_t> replaced, std::span<const Tick> folded,
  std::vector<std::shared_ptr<TableUnit>> added, std::vector<irs::DocMap> maps,
  uint64_t memory_below, bool logged) {
  SDB_ASSERT(!_read_only);
  std::optional<Table::Replacement> prepared;
  std::shared_ptr<const TableState> published;
  catalog::Transactions::Get().CommitStorage(
    _table->CommitMutex(),
    [&] {
      prepared.emplace(_table->PrepareReplace(
        replaced, folded, std::move(added), std::move(maps), memory_below));
      std::vector<uint8_t> record;
      if (logged && !_in_memory && !OwnerDropped() && !Empty(prepared->delta)) {
        record = catalog::SearchRecord(_db_id, _id, prepared->delta);
      }
      return record;
    },
    [&](Tick tick) {
      published = _table->Replace(tick, std::move(*prepared));
      _storage_tick.store(tick, std::memory_order_release);
      QueueReleaseCleanup(shared_from_this(), tick);
    });
  return published;
}

void Store::CommitUnlogged(absl::FunctionRef<void(Tick)> apply) {
  catalog::Transactions::Get().CommitStorage(
    _table->CommitMutex(), [] { return std::vector<uint8_t>{}; },
    [&](Tick tick) {
      apply(tick);
      _storage_tick.store(tick, std::memory_order_release);
    });
}

void Store::WriteOut() {
  if (!_table || !_piece_writer || OwnerDropped()) {
    return;
  }
  absl::MutexLock lock{&_write_out_mutex};
  while (WriteOutLocked()) {
  }
}

bool Store::WriteOutLocked() {
  auto memory = _table->InMemory();
  auto& pieces = memory.pieces;
  if (pieces.empty()) {
    return false;
  }
  bool rest = false;
  uint64_t docs = 0;
  for (size_t i = 0; i < pieces.size(); ++i) {
    docs += pieces[i]->docs;
    if (docs > irs::doc_limits::kMaxDocs) {
      SDB_ASSERT(i != 0);
      memory.end = pieces[i]->first;
      pieces.resize(i);
      rest = true;
      break;
    }
  }
  const auto field_options = CompactionOptions();
  auto merged = irs::GetBlocking(_writer->WriteAsync(
    _table->PieceView(pieces, /*keep_deleted=*/false), _writer->NextSegmentId(),
    field_options.get(), [] { return true; }, &AnnBuildEnv()));
  SDB_IF_FAILURE("search_cleanup_during_write_out") {
    irs::directory_utils::RemoveAllUnreferenced(*_dir);
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_search_write_out");
  std::vector<uint64_t> ids;
  std::vector<Tick> folded;
  ids.reserve(pieces.size());
  folded.reserve(pieces.size());
  for (const auto& piece : pieces) {
    ids.push_back(piece->id);
    folded.push_back(piece->reader->PatchTick());
  }
  std::vector<std::shared_ptr<TableUnit>> added;
  if (merged.reader) {
    added.push_back(_table->SegmentUnit(OwnSegment{
      .meta = std::move(merged.meta), .reader = std::move(merged.reader)}));
  }
  SyncSegments();
  CommitReplace(ids, folded, std::move(added), std::move(merged.maps),
                memory.end, /*logged=*/true);
  NudgeCompaction();
  return rest;
}

std::shared_ptr<const TableState> Store::SnapshotState(
  Tick snapshot, catalog::WalkCounters& walks) {
  EnsureOpen();
  if (!_table || _in_memory) {
    return nullptr;
  }
  const auto& version = _table->VersionAt(snapshot, walks);
  return version.tick <= snapshot ? version.state : nullptr;
}

std::vector<uint8_t> Store::SnapshotRecord(const TableState& state) const {
  irs::containers::FlatHashSet<irs::field_id> stored;
  {
    absl::MutexLock lock{&_layout_mutex};
    if (_table_columns) {
      stored = *_table_columns;
    }
  }
  TableDelta delta;
  MemoryDeletes memory;
  for (const auto& unit : state.units) {
    if (unit->deleted && !unit->deleted->Empty()) {
      if (unit->piece) {
        memory.Add(unit->first, unit->deleted);
      } else {
        delta.deletes.push_back({.segment = unit->id, .docs = unit->deleted});
      }
    }
    if (!unit->piece) {
      delta.add.push_back(
        {.id = unit->id, .kinds = unit->meta.kinds, .rows = unit->docs});
      for (const auto& patch : unit->patches) {
        delta.updates.push_back({.segment = unit->id, .patch = patch});
      }
      continue;
    }
    auto& rows = delta.rows.emplace_back(LoggedRows{.first = unit->first});
    std::vector<const irs::ColumnReader*> readers;
    duckdb::vector<duckdb::LogicalType> types;
    if (const auto* columns = unit->reader->GetColReader()) {
      for (const auto* column : columns->Columns()) {
        if (stored.contains(column->Id())) {
          readers.push_back(column);
          rows.columns.push_back(column->Id());
          types.push_back(column->Type());
        }
      }
    }
    irs::ReadContext ctx{*ReaderOptions().db};
    std::vector<irs::ColumnReader::ScanState> scans;
    scans.reserve(readers.size());
    for (const auto* reader : readers) {
      scans.push_back(reader->InitScan(ctx));
    }
    duckdb::DataChunk chunk;
    chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);
    for (irs::doc_id_t first = 0; first < unit->docs;) {
      const auto count =
        std::min<irs::doc_id_t>(unit->docs - first, STANDARD_VECTOR_SIZE);
      chunk.Reset();
      for (size_t i = 0; i < readers.size(); ++i) {
        readers[i]->Scan(scans[i], chunk.data[i], count);
      }
      chunk.SetChildCardinality(count);
      rows.chunks.push_back(LogChunk(chunk));
      first += count;
    }
  }
  std::move(memory).Finish(delta);
  delta.manifest = state.manifest;
  return catalog::SearchRecord(_db_id, _id, delta);
}

void Store::ApplyOptions(const catalog::InvertedIndexSettings& options) {
  SetTasksSettings(options);
  _segment_memory_max = options.segment_memory_max;
  _segment_docs_max = options.segment_docs_max;
  if (!_writer) {
    return;
  }
  irs::SegmentOptions segment_options;
  segment_options.segment_memory_max = options.segment_memory_max;
  segment_options.segment_docs_max = options.segment_docs_max;
  _writer->Options(segment_options);
}

void Store::StartTasks() {
  if (!_writer || _read_only) {
    return;
  }
#ifdef SDB_DEV
  const bool already = _tasks_started.exchange(true);
  SDB_ASSERT(!already, "Store::StartTasks called twice for store ", _id);
#endif
  _dir->attributes().refs().refs().OnRelease(
    [weak = weak_from_this()](const std::string&) noexcept {
      if (const auto store = weak.lock()) {
        store->RequestCleanup();
      }
    });
  GetSearchEngine().StartTasks(shared_from_this());
}

auto Store::TryClaimReindex() noexcept -> ReindexClaim {
  uint32_t idle = 0;
  if (!_reindex_state.compare_exchange_strong(idle, kReindexing)) {
    return {};
  }
  return ReindexClaim{this};
}

auto Store::ClaimReindex(duckdb::ClientContext& context) -> ReindexClaim {
  _reindex_state.fetch_add(kReindexWaiter);
  absl::Cleanup leave = [this] { _reindex_state.fetch_sub(kReindexWaiter); };
  absl::MutexLock lock{&duckdb::ClientContext::AwaitMutex()};
  context.Await([this] { return !(_reindex_state.load() & kReindexing); });
  [[maybe_unused]] const auto previous = _reindex_state.fetch_or(kReindexing);
  SDB_ASSERT(!(previous & kReindexing));
  return ReindexClaim{this};
}

void Store::ReindexReleaser::operator()(Store* store) const noexcept {
  SDB_ASSERT(store);
  if (store->_reindex_state.fetch_and(~kReindexing) >= kReindexWaiter) {
    duckdb::ClientContext::Notify();
  }
}

ResultWithTime Store::CleanupUnsafe(bool orphans) {
  const auto maintenance = ClaimMaintenance();
  if (!maintenance) {
    return {absl::OkStatus(), 0};
  }
  const auto begin = std::chrono::steady_clock::now();
  if (_table) {
    auto& transactions = catalog::Transactions::Get();
    const auto oldest = transactions.RefreshOldest();
    if (!_read_only && _table->Collectable(oldest)) {
      CommitUnlogged([&](Tick tick) { _table->Collect(tick, oldest); });
    }
    const auto running = transactions.RunningSnapshots();
    const auto* own = FindSnapshots(running.stores, _id);
    PruneVersions(own ? std::span<const Tick>{own->running} : running.snapshots,
                  running.published);
    transactions.AdvanceEpoch();
    if (FreeVersions()) {
      transactions.GarbageQueued();
    }
  }
  auto result = RemoveUnreferenced(*_dir, _id, orphans);
  SDB_PARK_ONCE_ON_FAILURE("search_cleanup_after_remove");
  const auto time_ms = MillisSince(begin);
  _maintenance.RecordCleanup(result, time_ms);
  return {std::move(result), time_ms};
}

void Store::RequestCleanup() noexcept {
  if (!BackgroundScheduler::gInstance || lifecycle::IsStopping() ||
      BackgroundScheduler::instance().IsStopping() ||
      _cleanup_requests.fetch_add(1, std::memory_order_acq_rel) != 0) {
    return;
  }
  try {
    BackgroundScheduler::instance()
      .Run([weak = weak_from_this()] {
        const auto store = weak.lock();
        if (!store) {
          return;
        }
        auto requests =
          store->_cleanup_requests.load(std::memory_order_acquire);
        do {
          try {
            DoCleanup(*store);
          } catch (const std::exception& e) {
            store->_cleanup_requests.store(0, std::memory_order_release);
            SDB_WARN(SEARCH, "table ", store->_id,
                     ": cleaning up its files failed: ",
                     duckdb::ErrorData{e}.RawMessage());
            return;
          }
          requests = store->_cleanup_requests.fetch_sub(
                       requests, std::memory_order_acq_rel) -
                     requests;
        } while (requests != 0);
      })
      .Detach();
  } catch (...) {
    _cleanup_requests.store(0, std::memory_order_release);
  }
}

auto Store::CompactUnsafeAsync(utils::DrainGate::Pass maintenance,
                               const irs::CompactionPolicy& policy,
                               const irs::MergeWriter::FlushProgress& progress,
                               bool& empty_compaction,
                               const irs::IndexFieldOptions* field_options,
                               const irs::AnnBuildEnv* env)
  -> yaclib::Future<ResultWithTime> {
  empty_compaction = false;
  if (!maintenance) {
    co_return ResultWithTime{BackupInProgressError(), 0};
  }
  const irs::MergeWriter::FlushProgress interruptible = [&progress] {
    return !IsBackupActive() && progress();
  };
  const auto begin = std::chrono::steady_clock::now();
  auto result = co_await CompactUnsafeImpl(
    policy, interruptible, empty_compaction, field_options, env);
  const auto time_ms = MillisSince(begin);
  if (result.ok() && empty_compaction && IsBackupActive()) {
    co_return ResultWithTime{BackupInProgressError(), time_ms};
  }
  _maintenance.RecordCompaction(result, empty_compaction, time_ms);
  co_return ResultWithTime{std::move(result), time_ms};
}

auto Store::CompactUnsafeImpl(const irs::CompactionPolicy& policy,
                              const irs::MergeWriter::FlushProgress& progress,
                              bool& empty_compaction,
                              const irs::IndexFieldOptions* field_options,
                              const irs::AnnBuildEnv* env)
  -> yaclib::Future<absl::Status> {
  empty_compaction = false;
  _merges.fetch_add(1, std::memory_order_relaxed);
  absl::Cleanup counted = [this] {
    _merges.fetch_sub(1, std::memory_order_acq_rel);
    duckdb::ClientContext::Notify();
  };
  SDB_PARK_ONCE_ON_FAILURE("search_compaction_before_selection");
  if (IsSearchTable() && !IsCurrentConfig(field_options)) {
    empty_compaction = true;
    co_return absl::OkStatus();
  }
  try {
    auto merged = co_await _writer->MergeAsync(policy, _table->ColdView(),
                                               field_options, progress, env);
    if (!merged.reader) {
      empty_compaction = true;
      co_return absl::OkStatus();
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_search_compact_before_publish");
    std::vector<uint64_t> ids;
    std::vector<Tick> folded;
    ids.reserve(merged.claim.candidates.size());
    folded.reserve(merged.claim.candidates.size());
    for (const auto* candidate : merged.claim.candidates) {
      const auto& impl =
        *irs::utils::downCast<irs::SegmentReader>(*candidate).GetImpl();
      ids.push_back(impl.Id());
      folded.push_back(impl.PatchTick());
    }
    SyncSegments();
    std::vector<std::shared_ptr<TableUnit>> added;
    added.push_back(_table->SegmentUnit(OwnSegment{
      .meta = std::move(merged.meta), .reader = std::move(merged.reader)}));
    CommitReplace(ids, folded, std::move(added), std::move(merged.maps), 0,
                  /*logged=*/true);
  } catch (const std::exception& e) {
    co_return absl::InternalError(
      absl::StrCat("compaction of table ", _id,
                   " failed: ", duckdb::ErrorData{e}.RawMessage()));
  }
  co_return absl::OkStatus();
}

std::shared_ptr<const irs::IndexFieldOptions> Store::CompactionOptions() {
  if (IsSearchTable()) {
    absl::MutexLock lock{&_layout_mutex};
    if (_layout.compaction_stale) {
      RebuildConfig(_layout.configs, _layout.building_index);
    }
    return _layout.compaction_config;
  }
  // Cloned for this merge: the merge encodes against what it took for its
  // whole length, and an ALTER writes a new configuration rather than mutating
  // this one, while a DROP destroys the bound index but not this clone.
  auto& db = irs::DuckDBEngine::Instance().instance();
  for (auto& attached : duckdb::DatabaseManager::Get(db).GetDatabases()) {
    if (attached->oid != _db_id) {
      continue;
    }
    const catalog::CommittedView view;
    const auto entry =
      attached->GetCatalog()
        .Cast<catalog::SereneDBCatalog>()
        .FindIn<catalog::InvertedIndexEntry>(view.Transaction(db), _id);
    return entry ? entry->Config() : nullptr;
  }
  return nullptr;
}

absl::Status Store::VacuumCompact(duckdb::ClientContext& context,
                                  uint32_t target_segments) {
  const irs::MergeWriter::FlushProgress progress = [&context] {
    return !context.IsInterrupted();
  };
  const auto target = std::max<uint32_t>(1, target_segments);
  GetSearchEngine().Maintenance().Enter(context).reset();
  WriteOut();
  const ManualCompactionSlot slot;
  const irs::AnnBuildEnv* env = slot.AnnEnv();
  for (size_t pass = 0; pass < 8; ++pass) {
    std::vector<std::vector<std::string>> buckets;
    {
      const auto snapshot = _table->ColdView();
      if (snapshot.size() <= target) {
        break;
      }
      buckets.resize(target);
      for (size_t i = 0; i < snapshot.size(); ++i) {
        buckets[i % target].emplace_back(snapshot[i].Meta().name);
      }
    }
    absl::Status status;
    bool merged = false;
    for (const auto& names : buckets) {
      bool contended = false;
      const irs::CompactionPolicy bucket =
        [&names, &contended](irs::Compaction& candidates,
                             const irs::IndexReader& reader,
                             const irs::CompactingSegments& busy) {
          for (size_t i = 0; i < reader.size(); ++i) {
            const auto& segment = reader[i];
            const auto& name = segment.Meta().name;
            if (!absl::c_linear_search(names, name)) {
              continue;
            }
            if (busy.contains(name)) {
              contended = true;
            } else {
              candidates.emplace_back(&segment);
            }
          }
        };
      bool empty = false;
      absl::Status res;
      do {
        const auto field_options = CompactionOptions();
        res =
          irs::GetBlocking(CompactUnsafeAsync(
                             GetSearchEngine().Maintenance().Enter(context),
                             bucket, progress, empty, field_options.get(), env))
            .res;
      } while (absl::IsUnavailable(res));
      context.InterruptCheck();
      status.Update(res);
      merged = merged || !empty;
      if (contended) {
        AwaitMerges(context);
        merged = true;
      }
    }
    if (!status.ok()) {
      return status;
    }
    if (!merged) {
      break;
    }
  }
  auto cleanup = CleanupUnsafe(false).res;
  catalog::Transactions::Get().AwaitPublished(
    _storage_tick.load(std::memory_order_acquire));
  return cleanup;
}

void Store::AwaitMerges(duckdb::ClientContext& context) {
  absl::MutexLock lock{&duckdb::ClientContext::AwaitMutex()};
  context.Await([this] { return _merges.load() == 0; });
}

irs::IndexWriter::CompactionFloorGuard Store::ArmCompactionFloor(
  duckdb::ClientContext& context) {
  auto floor = _writer->ClaimCompactionFloor();
  if (floor.Arm()) {
    return floor;
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_search_backfill_floor_retry");
  {
    absl::MutexLock lock{&duckdb::ClientContext::AwaitMutex()};
    context.Await([this] { return !_writer->Compacting(); });
  }
  [[maybe_unused]] const bool armed = floor.Arm();
  SDB_ASSERT(armed);
  return floor;
}

StoreStats Store::GetStats() const {
  auto stats = _table->Stats();
  _maintenance.Fill(stats);
  return stats;
}

std::shared_ptr<const TableState> Store::ReplaceSegments(
  std::span<const irs::SubReader* const> replaced,
  std::vector<irs::IndexWriter::Replacement> replacements) {
  SDB_ASSERT(_table);
  std::vector<uint64_t> ids;
  std::vector<Tick> folded;
  ids.reserve(replaced.size());
  folded.reserve(replaced.size());
  for (const auto* sub : replaced) {
    const auto& impl =
      *irs::utils::downCast<irs::SegmentReader>(*sub).GetImpl();
    ids.push_back(impl.Id());
    folded.push_back(impl.PatchTick());
  }
  std::vector<std::shared_ptr<TableUnit>> added;
  std::vector<irs::DocMap> maps;
  for (auto& replacement : replacements) {
    auto opened = OpenOwn(std::move(replacement.meta));
    const auto target = opened.reader->Id();
    for (auto& map : replacement.absorbed) {
      map.target = target;
      maps.push_back(std::move(map));
    }
    added.push_back(_table->SegmentUnit(std::move(opened)));
  }
  SyncSegments();
  auto published = CommitReplace(ids, folded, std::move(added), std::move(maps),
                                 0, /*logged=*/true);
  _table->Rewritten(published->tick);
  return published;
}

void Store::DrainPriorWriters(duckdb::ClientContext& context) {
  _writers.Drain(
    context,
    ConnectionContext::Get(context).Transaction().SearchTxn().Registration(
      _id));
}

std::shared_ptr<const catalog::InvertedIndexConfig> Store::Config() const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return _layout.config;
}

bool Store::IsCurrentConfig(const irs::IndexFieldOptions* options) const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return _layout.compaction_config.get() == options;
}

std::shared_ptr<const catalog::InvertedIndexConfig> Store::MergeConfigs(
  std::span<const IndexConfig> configs, std::span<const IndexConfig> retired,
  std::optional<duckdb::idx_t> skip) const {
  std::vector<catalog::InvertedIndexKey> keys;
  catalog::InvertedIndexFields fields;
  auto constraint_keys = _constraint_keys;
  const auto add = [&](const IndexConfig& index) {
    for (const auto& [id, field] : index.config->fields) {
      fields.emplace(id, field);
    }
    keys.insert(keys.end(), index.config->keys.begin(),
                index.config->keys.end());
    constraint_keys.insert(constraint_keys.end(),
                           index.config->constraint_keys.begin(),
                           index.config->constraint_keys.end());
  };
  for (const auto& index : configs) {
    if (index.oid != skip) {
      add(index);
    }
  }
  absl::c_for_each(retired, add);
  for (const auto& key : constraint_keys) {
    fields.emplace(key.field,
                   catalog::InvertedIndexField{{.indexed_term_dict = true}});
  }
  auto merged = std::make_shared<catalog::InvertedIndexConfig>(
    std::move(keys), std::move(fields));
  merged->pk = catalog::PkColumnKind::None;
  merged->top_k_scorer = _topk_options;
  merged->row_group_size = _row_group_size;
  merged->block_size = _block_size;
  merged->declared_compression = _compression;
  merged->table_columns = *_table_columns;
  merged->constraint_keys = std::move(constraint_keys);
  return merged;
}

template<typename Configs>
void Store::RebuildConfig(Configs&& configs,
                          std::optional<duckdb::idx_t> building_index) {
  auto config = MergeConfigs(configs, {}, std::nullopt);
  auto scan_config =
    building_index ? MergeConfigs(configs, {}, building_index) : config;
  auto compaction_config =
    _layout.retired.empty()
      ? config
      : MergeConfigs(configs, _layout.retired, std::nullopt);
  SDB_IF_FAILURE("SearchTable::RebuildConfig") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  _layout.configs = std::forward<Configs>(configs);
  _layout.building_index = building_index;
  _layout.config = std::move(config);
  _layout.scan_config = std::move(scan_config);
  _layout.compaction_config = std::move(compaction_config);
  _layout.compaction_stale = false;
  ScanConfigChanged();
}

void Store::ScanConfigChanged() {
  if (_layout.owner != kNoLayoutOwner) {
    _layout.owner_changed = true;
    return;
  }
  _layout.published_config = _layout.scan_config;
  if (_table) {
    _table->SetConfig(_layout.published_config);
  }
}

void Store::MergeIndexConfig(
  duckdb::idx_t index_oid,
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  absl::MutexLock lock{&_layout_mutex};
  RebuildConfig(
    WithConfig(_layout.configs, IndexConfig{index_oid, std::move(config)}),
    _layout.building_index);
}

void Store::ClaimLayout(duckdb::ClientContext& context, std::string_view name,
                        TableChange change) {
  if (Dropped()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("could not serialize access due to concurrent update of \"", name,
              "\""));
  }
  ConnectionContext::Get(context).Transaction().ClaimSearchLayout(
    shared_from_this(), name, change);
}

void Store::ClaimExclusive(duckdb::ClientContext& context,
                           std::string_view name, Exclusive kind) {
  auto& session = ConnectionContext::Get(context).Transaction();
  session.ClaimSearchLayout(shared_from_this(), name, TableChange::Layout);
  session.SearchTxn().ClaimExclusive(shared_from_this(), kind);
  absl::MutexLock lock{&_layout_mutex};
  _layout.exclusive = kind;
}

Store::Exclusive Store::ExclusiveClaim() const {
  absl::MutexLock lock{&_layout_mutex};
  return _layout.owner == kNoLayoutOwner ? Exclusive::None : _layout.exclusive;
}

bool Store::AcquireLayout(duckdb::transaction_t owner, std::string_view name,
                          TableChange change) {
  SDB_ASSERT(owner != kNoLayoutOwner);
  absl::MutexLock lock{&_layout_mutex};
  const bool acquired = _layout.owner != owner;
  if (acquired && _layout.owner != kNoLayoutOwner) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("table \"", name, "\" is being changed by another transaction"),
      ERR_HINT("Retry once that transaction commits or rolls back."));
  }
  _layout.owner = owner;
  if (change == TableChange::Schema) {
    SDB_ASSERT(IsSearchTable());
    if (!_layout.schema_undo) {
      _layout.schema_undo = SchemaUndo{
        .epoch = _schema_epoch.load(),
        .table_columns = *_table_columns,
        .compression = _compression,
        .constraint_keys = _constraint_keys,
      };
    }
    _schema_epoch.fetch_add(1);
  }
  return acquired;
}

void Store::ReleaseLayout(duckdb::transaction_t owner,
                          bool committed) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  SDB_ASSERT(_layout.owner == owner);
  if (committed) {
    AdoptOwnConfig();
  }
  _layout.owner = kNoLayoutOwner;
  _layout.exclusive = Exclusive::None;
  _layout.owner_changed = false;
  auto undo = std::exchange(_layout.schema_undo, std::nullopt);
  if (undo && !committed) {
    _schema_epoch.store(undo->epoch);
    _table_columns = std::move(undo->table_columns);
    _compression = std::move(undo->compression);
    _constraint_keys = std::move(undo->constraint_keys);
    RebuildConfig(_layout.configs, _layout.building_index);
  }
}

std::shared_ptr<const catalog::InvertedIndexConfig> Store::OwnScanConfig(
  duckdb::transaction_t owner) const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return _layout.owner == owner && _layout.owner_changed ? _layout.scan_config
                                                         : nullptr;
}

void Store::CommitLayout(duckdb::transaction_t owner) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  if (_layout.owner == owner) {
    AdoptOwnConfig();
  }
}

void Store::PublishLayout(Tick commit) {
  if (_table) {
    _table->PublishConfig(commit);
  }
}

void Store::AdoptOwnConfig() noexcept {
  if (!_layout.owner_changed ||
      _layout.published_config == _layout.scan_config) {
    return;
  }
  _layout.published_config = _layout.scan_config;
  if (_table) {
    _table->SetConfig(_layout.published_config);
  }
}

void Store::AddTableColumns(const duckdb::ColumnList& columns) {
  absl::MutexLock lock{&_layout_mutex};
  _table_columns->merge(TableColumns(columns));
  _compression.merge(DeclaredCompression(columns));
  RebuildConfig(_layout.configs, _layout.building_index);
}

void Store::SetTableColumns(const duckdb::ColumnList& columns) {
  absl::MutexLock lock{&_layout_mutex};
  _table_columns = TableColumns(columns);
  _compression = DeclaredCompression(columns);
  RebuildConfig(_layout.configs, _layout.building_index);
}

void Store::SetConstraintKeys(std::vector<catalog::ConstraintKey> keys) {
  absl::MutexLock lock{&_layout_mutex};
  const auto same = [](const catalog::ConstraintKey& lhs,
                       const catalog::ConstraintKey& rhs) {
    return lhs.field == rhs.field && lhs.columns == rhs.columns &&
           lhs.types == rhs.types && lhs.expressions == rhs.expressions;
  };
  if (absl::c_equal(keys, _constraint_keys, same)) {
    return;
  }
  _constraint_keys = std::move(keys);
  RebuildConfig(_layout.configs, _layout.building_index);
}

void Store::AddReferencing(std::weak_ptr<const Referencing> referencing) {
  absl::MutexLock lock{&_referencing_mutex};
  std::erase_if(_referencing,
                [](const auto& entry) { return entry.expired(); });
  _referencing.push_back(std::move(referencing));
}

std::vector<std::shared_ptr<const Referencing>> Store::ReferencingKeys() const {
  std::vector<std::shared_ptr<const Referencing>> live;
  absl::MutexLock lock{&_referencing_mutex};
  std::erase_if(_referencing, [&](const auto& entry) {
    auto referencing = entry.lock();
    if (!referencing) {
      return true;
    }
    live.push_back(std::move(referencing));
    return false;
  });
  return live;
}

Store::BuildClaim Store::BeginIndexBuild(
  duckdb::idx_t index_oid,
  std::shared_ptr<const catalog::InvertedIndexConfig> config) {
  BuildClaim claim{*this, index_oid};
  absl::MutexLock lock{&_layout_mutex};
  SDB_ASSERT(!_layout.building_index);
  RebuildConfig(
    WithConfig(_layout.configs, IndexConfig{index_oid, std::move(config)}),
    index_oid);
  return claim;
}

void Store::FinishIndexBuild(duckdb::idx_t index_oid) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  if (_layout.building_index != index_oid) {
    return;
  }
  _layout.building_index.reset();
  _layout.scan_config = _layout.config;
  ScanConfigChanged();
}

void Store::AbortIndexBuild(duckdb::idx_t index_oid) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  if (_layout.building_index != index_oid) {
    return;
  }
  _layout.building_index.reset();
  std::erase_if(_layout.configs, [&](const IndexConfig& index) {
    return index.oid == index_oid;
  });
  _layout.config = _layout.scan_config;
  _layout.compaction_stale = true;
}

Store::BuildClaim::~BuildClaim() {
  if (_store) {
    _store->AbortIndexBuild(_index_oid);
  }
}

void Store::BuildClaim::Finish() {
  if (auto* store = std::exchange(_store, nullptr)) {
    store->FinishIndexBuild(_index_oid);
  }
}

bool Store::BuildInFlight() const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return static_cast<bool>(_layout.building_index);
}

bool Store::HasIndexes() const {
  absl::ReaderMutexLock lock{&_layout_mutex};
  return absl::c_any_of(_layout.configs, [&](const IndexConfig& index) {
    return index.oid != _id;
  });
}

void Store::RemoveIndexConfig(duckdb::idx_t index_oid) {
  absl::MutexLock lock{&_layout_mutex};
  const auto it = absl::c_find_if(
    _layout.configs,
    [&](const IndexConfig& index) { return index.oid == index_oid; });
  if (it == _layout.configs.end()) {
    return;
  }
  _layout.retired.push_back(*it);
  _layout.configs.erase(it);
  if (_layout.building_index == index_oid) {
    _layout.building_index.reset();
  }
  // only for remove we want removal to stay in case of merge configs failure
  // as this is DROP INDEX / Rollback and catalog already marked index as gone
  // so nobody will repeat this removal. This will leave field config not
  // updated but at least any next DLL will fix this and "stalled" fields will
  // be not emitted anymore. If we leave _configs without deletion this will be
  // not fixed until restart.
  RebuildConfig(_layout.configs, _layout.building_index);
}

void Store::ReleaseRetiredConfig(duckdb::idx_t index_oid) noexcept {
  absl::MutexLock lock{&_layout_mutex};
  if (std::erase_if(_layout.retired, [&](const IndexConfig& index) {
        return index.oid == index_oid;
      }) != 0) {
    _layout.compaction_stale = true;
  }
}

TableReplay* Store::Replay() {
  EnsureOpen();
  return _table ? &_replay : nullptr;
}

void Store::ScheduleRecovery(duckdb::ClientContext& context,
                             duckdb::TaskExecutor& executor) {
  EnsureOpen();
  if (!_table) {
    return;
  }
  _writer->ReserveSegmentId(_replay.Top());
  for (auto& [id, segment] : _replay.Segments()) {
    executor.ScheduleTask(
      duckdb::make_uniq<RecoveryTask>(executor, [this, id, &segment] {
        segment.opened = _table->OpenSegment(id, segment.kinds, segment.rows);
      }));
  }
  for (auto& piece : _replay.Cut()) {
    executor.ScheduleTask(
      duckdb::make_uniq<RecoveryTask>(executor, [this, &context, &piece] {
        piece.reader = ReplayPiece(context, _replay.Chunks(piece));
      }));
  }
}

std::shared_ptr<const irs::SegmentReaderImpl> Store::ReplayPiece(
  duckdb::ClientContext& context, std::span<const TableReplay::Chunk> chunks) {
  auto piece = StartPiece();
  absl::Cleanup abort = [&] { piece.trx.Abort(); };
  uint64_t rows = 0;
  {
    const auto sink = connector::MakeSearchTableInsertSink(
      piece.trx, *this, _replay.RowsCatalog(), context);
    for (const auto& chunk : chunks) {
      connector::WriteChunkToSearchSink(*sink, *chunk.rows, *chunk.columns, _id,
                                        context);
      rows += chunk.rows->size();
    }
  }
  SDB_ENSURE(PieceDocs(piece.trx) == rows, "table ", _id, ": ", rows,
             " rows of its log indexed into ", PieceDocs(piece.trx),
             " documents");
  return piece.trx.SealPiece(piece.id);
}

bool Store::FinishReplay() {
  EnsureOpen();
  if (!_table) {
    return false;
  }
  const bool replayed = _replay.Replayed();
  const auto memory_end = _replay.MemoryEnd();
  auto manifest = _replay.Manifest();
  _table->Recover(std::move(_replay).Units(*_table), memory_end,
                  std::move(manifest));
  _replay = TableReplay{};
  ClaimColumnOids(_table->State()->reader);
  return replayed;
}

}  // namespace sdb::search
