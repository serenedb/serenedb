////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "search/search_table.h"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>

#include <chrono>
#include <duckdb/common/file_system.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <system_error>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/lifecycle.h"
#include "basics/log.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "pg/sql_exception_macro.h"
#include "scheduler/background_scheduler.h"
#include "search/inverted_index_storage.h"
#include "search/task.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

std::filesystem::path SearchTable::GetPath(ObjectId db_id, ObjectId schema_id,
                                           ObjectId table_id) {
  SDB_ASSERT(db_id.isSet());
  SDB_ASSERT(schema_id.isSet());
  SDB_ASSERT(table_id.isSet());
  // Same on-disk layout as an inverted index minus the trailing index level --
  // reuse its path generator with the index unset.
  // TODO(Dronplane): unify as generic SearchStorage with all common stuff
  return InvertedIndexStorage::GetPath(db_id, schema_id, table_id,
                                       /*index_id=*/ObjectId{});
}

std::filesystem::path SearchTable::GetWalPath(ObjectId db_id) {
  SDB_ASSERT(db_id.isSet());
  auto path = GetSearchEngine().GetPersistedPath(db_id);
  path /= "wal";
  return path;
}

std::shared_ptr<SearchTable> SearchTable::Create(
  ObjectId db_id, ObjectId schema_id, ObjectId table_id, bool is_new,
  const catalog::persistence::SearchTableOptions& options,
  std::vector<catalog::ColumnId> pk_columns) {
  return std::make_shared<SearchTable>(db_id, schema_id, table_id, is_new,
                                       options, std::move(pk_columns));
}

namespace {

// Each PRIMARY KEY column is term-indexed under its own column id so PK
// predicates push down. That term field is the column id itself -- distinct
// from the ids user indexes allocate, so it never collides. store_values is
// off: the value is stored under the column id, not this term field.
void BuildPkInto(catalog::InvertedIndex::Entries& entries,
                 SearchTable::TermsByColumn& terms,
                 const std::vector<catalog::ColumnId>& pk_columns) {
  for (auto id : pk_columns) {
    catalog::InvertedIndexEntryInfo info;
    info.store_values = false;
    info.indexed_term_dict = true;
    const auto field_id = static_cast<irs::field_id>(id);
    entries.emplace(field_id, info);
    terms[id].push_back(field_id);
  }
}

// Fold each of `index`'s plain-column entries into the merged config, keyed by
// the index's own allocated term field_id so several indexes on one column get
// independent posting lists. store_values is forced off (value stored under the
// column id). Only genuinely term-indexed entries contribute to `terms`.
void MergeIndexInto(catalog::InvertedIndex::Entries& entries,
                    SearchTable::TermsByColumn& terms,
                    const catalog::InvertedIndex& index) {
  for (auto col_id : index.GetColumns()) {
    const auto* entry = index.FindColumnInfo(col_id);
    if (!entry) {
      continue;
    }
    const auto term_field = index.TermFieldForColumn(col_id);
    auto merged = *entry;
    merged.store_values = false;
    entries.insert_or_assign(term_field, merged);
    if (merged.IsTermDict()) {
      terms[col_id].push_back(term_field);
    }
  }
  // Indexed expressions are synthetic and single-field: value + terms (and any
  // IVF/JSON-leaf/norm sub-fields) live under the expression's own field id, so
  // fold each entry verbatim and add nothing to `terms`.
  for (const auto& key : index.ExpressionKeys()) {
    if (const auto* entry = index.FindEntry(key.field_id)) {
      entries.insert_or_assign(key.field_id, *entry);
    }
  }
}

// The iresearch encoding config the search writer asks for at flush/merge,
// resolved from the merged config.
class MergedFieldOptions final : public irs::IndexFieldOptions {
 public:
  explicit MergedFieldOptions(
    std::shared_ptr<const catalog::InvertedIndex::Entries> entries)
    : _entries{std::move(entries)} {}

  irs::ColumnOptions GetColumnOptions(irs::field_id id) const final {
    const auto it = _entries->find(id);
    if (it == _entries->end()) {
      return {};  // not a merged-config field -> writer baseline
    }
    const auto& entry = it->second;
    return {
      .compression = entry.compression,
      // An IVF entry keys the merged config by its column id (the value
      // column), not a per-index term field, so this attaches the ANN index to
      // that column.
      .ann_info = catalog::AnnInfoForEntry(id, entry),
      .hyperloglog = entry.hyperloglog,
    };
  }

  irs::field_id GetNormColumnId(irs::field_id id) const final {
    const auto it = _entries->find(id);
    SDB_ASSERT(it != _entries->end(),
               "MergedFieldOptions::GetNormColumnId: unknown id ", id);
    const auto& entry = it->second;
    SDB_ASSERT(irs::field_limits::valid(entry.synthetic_column),
               "MergedFieldOptions::GetNormColumnId: no norm reservation for "
               "id ",
               id);
    return entry.synthetic_column;
  }

 private:
  std::shared_ptr<const catalog::InvertedIndex::Entries> _entries;
};

std::shared_ptr<const irs::IndexFieldOptions> MakeFieldOptions(
  std::shared_ptr<const catalog::InvertedIndex::Entries> entries) {
  return std::make_shared<const MergedFieldOptions>(std::move(entries));
}

}  // namespace

SearchTable::SearchTable(
  ObjectId db_id, ObjectId schema_id, ObjectId table_id, bool is_new,
  const catalog::persistence::SearchTableOptions& options,
  std::vector<catalog::ColumnId> pk_columns)
  : _table_id{table_id},
    _db_id{db_id},
    _schema_id{schema_id},
    _is_new{is_new},
    _pk_columns{std::move(pk_columns)},
    _segment_memory_max{options.segment_memory_max} {
  catalog::InvertedIndex::Entries entries;
  TermsByColumn terms;
  BuildPkInto(entries, terms, _pk_columns);
  _entries =
    std::make_shared<const catalog::InvertedIndex::Entries>(std::move(entries));
  _terms_by_column = std::make_shared<const TermsByColumn>(std::move(terms));
  _field_options = MakeFieldOptions(_entries);
  OpenWriter();

  _maint_settings.refresh_interval_msec = options.refresh_interval_ms;
  _maint_settings.compaction_interval_msec = options.compaction_interval_ms;
  _maint_settings.cleanup_interval_step = options.cleanup_interval_step;
}

std::shared_ptr<const catalog::InvertedIndex::Entries>
SearchTable::GetIndexConfig() const noexcept {
  std::shared_lock lock(_table_lock);
  return _entries;
}

std::shared_ptr<const SearchTable::TermsByColumn>
SearchTable::GetTermsByColumn() const noexcept {
  std::shared_lock lock(_table_lock);
  return _terms_by_column;
}

std::shared_ptr<const irs::IndexFieldOptions> SearchTable::GetFieldOptions()
  const noexcept {
  std::shared_lock lock(_table_lock);
  return _field_options;
}

catalog::TokenizerMap ResolveShardTokenizers(const SearchTable& shard,
                                             duckdb::ClientContext* context) {
  catalog::TokenizerMap dicts;
  // Deliberately not the session overload of ResolveTokenizers: that resolves
  // the database off the connection's SereneDB state, which WAL replay's bare
  // duckdb::Connection does not have. The shard knows its own database.
  auto& db_catalog = catalog::DatabaseCatalog(context, shard.GetDbId());
  for (const auto& index : catalog::RelationInvertedIndexes(
         context, shard.GetSchemaId(), shard.GetTableId())) {
    for (const auto id : catalog::InvertedInfo(*index).GetTokenizers()) {
      dicts.try_emplace(id, catalog::FindTokenizerIn(context, db_catalog, id));
    }
  }
  return dicts;
}

catalog::ColumnTokenizer SearchTable::GetTokenizer(
  duckdb::ClientContext& context, irs::field_id field_id) const {
  auto config = GetIndexConfig();
  auto it = config->find(field_id);
  if (it == config->end()) {
    return {};  // not a merged-config field: the default string tokenizer
  }
  return catalog::TokenizerForEntry(ResolveShardTokenizers(*this, &context),
                                    it->second);
}

void SearchTable::MergeIndexConfig(const catalog::InvertedIndex& index) {
  std::unique_lock lock(_table_lock);
  auto merged_entries =
    std::make_shared<catalog::InvertedIndex::Entries>(*_entries);
  auto merged_terms = std::make_shared<TermsByColumn>(*_terms_by_column);
  MergeIndexInto(*merged_entries, *merged_terms, index);
  _entries = std::move(merged_entries);
  _terms_by_column = std::move(merged_terms);
  _field_options = MakeFieldOptions(_entries);
}

void SearchTable::RebuildIndexConfig(duckdb::ClientContext* context) {
  catalog::InvertedIndex::Entries entries;
  TermsByColumn terms;
  BuildPkInto(entries, terms, _pk_columns);
  for (const auto& index :
       catalog::RelationInvertedIndexes(context, _schema_id, _table_id)) {
    MergeIndexInto(entries, terms, catalog::InvertedInfo(*index));
  }
  auto next_entries =
    std::make_shared<const catalog::InvertedIndex::Entries>(std::move(entries));
  auto next_terms = std::make_shared<const TermsByColumn>(std::move(terms));
  std::unique_lock lock(_table_lock);
  _entries = std::move(next_entries);
  _terms_by_column = std::move(next_terms);
  _field_options = MakeFieldOptions(_entries);
}

SearchTable::~SearchTable() {
  _writer.reset();
  _dir.reset();
  if (!_dropped.load(std::memory_order_acquire)) {
    return;
  }
  // Shutdown may already have torn the pool down; the removal then waits for
  // boot's orphan sweep, exactly like a crash between the commit and here.
  if (lifecycle::IsStopping() || BackgroundScheduler::instance().IsStopping()) {
    return;
  }
  GetSearchEngine().GetDbWal(_db_id).DeregisterShard(_table_id);
  BackgroundScheduler::instance()
    .Run([index_dir = GetPath(_db_id, _schema_id, _table_id)] {
      RemoveDroppedStorageDir(index_dir);
    })
    .Detach();
}

void SearchTable::OpenWriter() {
  auto path = GetPath(_db_id, _schema_id, GetTableId());

  std::error_code ec;
  bool path_exists = std::filesystem::exists(path, ec);
  if (ec) {
    THROW_SQL_ERROR(ERR_MSG("Failed to check existence of path '",
                            path.string(),
                            "' while initializing search table for table ",
                            GetTableId().id(), ": ", ec.message()));
  }
  if (!path_exists) {
    std::filesystem::create_directories(path, ec);
    if (ec) {
      THROW_SQL_ERROR(ERR_MSG("Failed to create directory '", path.string(),
                              "' while initializing search table for table ",
                              GetTableId().id(), ": ", ec.message()));
    }
  }

  auto codec = irs::formats::Get("1_5simd");
  const auto open_mode =
    path_exists ? (irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate)
                : irs::OpenMode::kOmCreate;

  irs::ResourceManagementOptions resource_manager;
  _dir = std::make_unique<irs::MMapDirectory>(path, irs::DirectoryAttributes{},
                                              resource_manager);

  irs::IndexWriterOptions writer_options;
  writer_options.segment_memory_max = _segment_memory_max;
  // A shard loaded from disk may hold flushed-but-unpublished segments the WAL
  // references, so Make() must not unlink them; FinishRecovery cleans up once
  // replay is done. A new shard's directory is empty, so it keeps the default.
  writer_options.cleanup_on_open = _is_new;
  // TODO(Dronplane): for now we rely on rocksdb (still present) lock
  // But in future we need own server wide data dir lock.
  writer_options.lock_repository = false;
  writer_options.db = &sdb::DuckDBEngine::Instance().instance();
  writer_options.reader_options.db = writer_options.db;

  writer_options.meta_payload_provider = [this](uint64_t tick,
                                                irs::bstring& out) {
    _last_committed_tick = std::max(_last_committed_tick, tick);
    uint64_t tick_be = absl::big_endian::FromHost(_last_committed_tick);
    out.append(reinterpret_cast<const irs::byte_type*>(&tick_be),
               sizeof(tick_be));
    return true;
  };

  _writer = irs::IndexWriter::Make(*_dir, codec, open_mode, writer_options);

  if (path_exists) {
    // Restore the durable commit tick from the last commit's meta payload.
    auto reader = _writer->GetSnapshot();
    auto payload = irs::GetPayload(reader.Meta().index_meta);
    if (payload.size() >= sizeof(uint64_t)) {
      _last_committed_tick = absl::big_endian::Load64(payload.data());
    }

    // Floor the id allocator (gCurrentTick / NextId) from this store's own
    // field ids: it is in-memory and re-derived at boot only from LIVE catalog
    // ids, so a dropped index's ids -- still occupying slots in this SHARED
    // store -- could be re-issued to a new index and collide at merge. Scan
    // BOTH term-dict field ids AND columnstore ids (one shared allocation
    // pool). Skip reserved system fields (> kMaxRealColumnIdValue): they are
    // not drawn from NextId, so flooring to them would exhaust the allocator.
    const auto floor_from = [](irs::field_id id) {
      if (id <= catalog::kMaxRealColumnIdValue) {
        catalog::RestoreId(id);
      }
    };
    for (const auto& segment : reader) {
      for (const auto field : segment.field_ids()) {
        floor_from(field);
      }
      if (const auto* col_reader = segment.GetColReader()) {
        for (const auto& column : col_reader->Columns()) {
          floor_from(column->Id());
        }
      }
    }
  }

  _wal = &GetSearchEngine().GetDbWal(_db_id);

  if (_is_new) {
    // A brand-new shard has no WAL records, so seed its committed tick at the
    // database WAL's current tick (not 0) -- otherwise an unused table would
    // pin the shared WAL's GC floor.
    _last_committed_tick = _wal->CurrentTick();
  }
  _wal->RegisterShard(GetTableId(), _last_committed_tick);

  if (_is_new) {
    _writer->RefreshCommit();
  }
}

void SearchTable::StartTasks() {
#ifdef SDB_DEV
  const bool already = _tasks_started.exchange(true);
  SDB_ASSERT(!already, "SearchTable::StartTasks called twice for table ",
             GetTableId().id());
#endif
  // Launch this table's refresh + compaction loops on the shared background
  // scheduler. Called only after recovery or CREATE/CTAS finalize, so a
  // background commit's WAL GC never races replay.
  GetSearchEngine().StartTasks(shared_from_this());
}

ResultWithTime SearchTable::RefreshUnsafe(
  bool wait, const irs::ProgressReportCallback& /*progress*/,
  RefreshResult& code) {
  const auto begin = std::chrono::steady_clock::now();
  code = RefreshResult::NoChanges;
  auto result = absl::OkStatus();
  try {
    std::unique_lock<absl::Mutex> lock{_refresh_mutex, std::try_to_lock};
    if (!lock.owns_lock()) {
      if (wait) {
        lock.lock();
      } else {
        code = RefreshResult::InProgress;  // another refresh/VACUUM is running
      }
    }
    if (lock.owns_lock()) {
      // Snapshot the WAL tick before publishing: a RefreshCommit that reports
      // no changes proves this shard has nothing un-published up to that tick,
      // and any later batch lands at a higher tick, so advancing to it never
      // over-claims.
      const auto tick_before = _wal->CurrentTick();
      if (_writer->RefreshCommit()) {
        _wal->OnShardCommit(GetTableId(), _last_committed_tick);
        code = RefreshResult::Done;
      } else {
        _wal->OnShardCommit(GetTableId(), tick_before);
      }
    }
  } catch (const std::exception& e) {
    result = absl::InternalError(absl::StrCat(
      "refresh failed for search table ", GetTableId().id(), ": ", e.what()));
  }
  const uint64_t time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin)
      .count();
  return {std::move(result), time_ms};
}

ResultWithTime SearchTable::CompactUnsafe(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction,
  const irs::IndexFieldOptions* field_options) {
  return irs::GetReady(CompactUnsafeAsync(policy, progress, empty_compaction,
                                          field_options, /*env=*/nullptr));
}

auto SearchTable::CompactUnsafeAsync(
  const irs::CompactionPolicy& policy,
  const irs::MergeWriter::FlushProgress& progress, bool& empty_compaction,
  const irs::IndexFieldOptions* field_options, const irs::AnnBuildEnv* env)
  -> yaclib::Future<ResultWithTime> {
  const auto begin = std::chrono::steady_clock::now();
  empty_compaction = false;
  auto result = absl::OkStatus();
  if (!policy) {
    result = absl::InvalidArgumentError(absl::StrCat(
      "unset compaction policy for search table ", GetTableId().id()));
  } else {
    try {
      // iresearch serializes Compact against refresh/DML internally, so a long
      // merge never blocks the refresh chain.
      const auto res = co_await _writer->CompactAsync(policy, field_options,
                                                      nullptr, progress, env);
      if (!res) {
        result = absl::InternalError(absl::StrCat(
          "compaction failed for search table ", GetTableId().id()));
      } else {
        empty_compaction = (res.size == 0);  // nothing merged -> idle round
      }
    } catch (const std::exception& e) {
      result = absl::InternalError(
        absl::StrCat("consolidation failed for search table ",
                     GetTableId().id(), ": ", e.what()));
    }
  }
  const uint64_t time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin)
      .count();
  co_return ResultWithTime{std::move(result), time_ms};
}

ResultWithTime SearchTable::CleanupUnsafe() {
  const auto begin = std::chrono::steady_clock::now();
  auto result = absl::OkStatus();
  try {
    irs::directory_utils::RemoveAllUnreferenced(*_dir);
  } catch (const std::exception& e) {
    result = absl::InternalError(absl::StrCat(
      "cleanup failed for search table ", GetTableId().id(), ": ", e.what()));
  }
  const uint64_t time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - begin)
      .count();
  return {std::move(result), time_ms};
}

void SearchTable::VacuumRefresh() {
  RefreshResult code = RefreshResult::Undefined;
  RefreshUnsafe(/*wait=*/true, nullptr, code);
  CleanupUnsafe();
}

void SearchTable::VacuumCompact() {
  static const auto kFullMerge = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  static const irs::MergeWriter::FlushProgress kProgress = [] { return true; };
  RefreshResult code = RefreshResult::Undefined;
  RefreshUnsafe(/*wait=*/true, nullptr, code);
  bool empty = false;
  CompactUnsafe(kFullMerge, kProgress, empty, /*field_options=*/nullptr);
  if (!empty) {
    RefreshUnsafe(/*wait=*/true, nullptr, code);
  }
  CleanupUnsafe();
}

}  // namespace sdb::search
