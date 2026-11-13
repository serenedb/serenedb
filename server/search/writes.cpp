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

#include "search/writes.h"

#include <absl/algorithm/container.h>
#include <absl/functional/any_invocable.h>

#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/sql_identifier.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/base_catalog.h"
#include "catalog/transaction.h"
#include "connector/constraint_keys.h"
#include "connector/full_scanner.h"
#include "connector/search_sink_writer.hpp"
#include "search/log_entry.h"
#include "search/task.h"

namespace sdb::search {
namespace {

void ReleaseWriter(StoreWrites& w) noexcept { w.registration.reset(); }

[[noreturn]] void ThrowDuplicate(duckdb::ClientContext& context,
                                 const KeySpec& spec, std::string_view key) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
                  ERR_MSG("duplicate key value violates unique constraint \"",
                          spec.constraint, "\""),
                  ERR_DETAIL("Key ",
                             connector::ConstraintKeyText(
                               context, key, spec.names, spec.types),
                             " already exists."));
}

void AddChanged(StoreWrites& w, Tick snapshot,
                std::shared_ptr<const irs::DocRemoval> rows) {
  if (w.changed.empty() || w.changed.back().snapshot != snapshot) {
    w.changed.push_back({.snapshot = snapshot});
  }
  w.changed.back().rows.push_back(std::move(rows));
}

[[noreturn]] void ThrowMissingReference(duckdb::ClientContext& context,
                                        const catalog::SearchTableEntry& table,
                                        const KeySpec& spec,
                                        std::string_view key) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FOREIGN_KEY_VIOLATION),
    ERR_MSG("insert or update on table \"", table.name.GetIdentifierName(),
            "\" violates foreign key constraint \"", spec.constraint, "\""),
    ERR_DETAIL(
      "Key ",
      connector::ConstraintKeyText(context, key, spec.names, spec.types),
      " is not present in table \"", spec.referenced.GetIdentifierName(),
      "\"."));
}

[[noreturn]] void ThrowStillReferenced(duckdb::ClientContext& context,
                                       const catalog::SearchTableEntry& table,
                                       const KeySpec& unique,
                                       const ReferencingKey& referencing,
                                       std::string_view key) {
  const auto& child = referencing.table->name.GetIdentifierName();
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FOREIGN_KEY_VIOLATION),
    ERR_MSG("update or delete on table \"", table.name.GetIdentifierName(),
            "\" violates foreign key constraint \"",
            referencing.spec->constraint, "\" on table \"", child, "\""),
    ERR_DETAIL(
      "Key ",
      connector::ConstraintKeyText(context, key, unique.names, unique.types),
      " is still referenced from table \"", child, "\"."));
}

bool HasUniqueKeys(const StoreWrites& w) noexcept {
  return absl::c_any_of(
    w.keys, [](const KeySpec& spec) { return spec.kind == KeyKind::Unique; });
}

const KeySpec* UniqueSpec(const StoreWrites& w, irs::field_id field) {
  const auto it = absl::c_find_if(w.keys, [&](const KeySpec& spec) {
    return spec.kind == KeyKind::Unique && spec.key.field == field;
  });
  return it == w.keys.end() ? nullptr : &*it;
}

// Visits the keys under unique key `spec` of rows `docs` of `segment`.
void ForEachRowKey(duckdb::ClientContext& context, duckdb::idx_t table_id,
                   const KeySpec& spec, const irs::SubReader& segment,
                   const irs::DocumentMask& docs,
                   absl::FunctionRef<void(std::string_view key)> visit) {
  const auto* columns = segment.GetColReader();
  if (!columns || docs.Empty()) {
    return;
  }
  const connector::ConstraintKeyEncoder encoder{
    context,
    table_id,
    {.field = spec.key.field,
     .columns = spec.key.columns,
     .expressions = spec.key.expressions}};
  std::vector<connector::ColumnId> column_ids;
  duckdb::vector<duckdb::LogicalType> types;
  for (size_t i = 0; i != spec.key.columns.size(); ++i) {
    if (i >= spec.key.expressions.size() || spec.key.expressions[i].empty()) {
      column_ids.push_back(spec.key.columns[i]);
      types.push_back(spec.types[i]);
    }
  }
  for (auto& read : encoder.ExpressionReads()) {
    if (!absl::c_linear_search(column_ids, read.column)) {
      column_ids.push_back(read.column);
      types.push_back(std::move(read.type));
    }
  }
  std::vector<irs::ColumnstoreProjection> projections;
  for (size_t i = 0; i != column_ids.size(); ++i) {
    projections.push_back({.output_slot = i, .column_id = column_ids[i]});
  }
  irs::ColFilterStateCache filter_states;
  connector::FullScanner scanner{
    *columns, projections, {}, &context, filter_states};
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::Get(context), types);
  duckdb::SelectionVector rows{STANDARD_VECTOR_SIZE};
  const uint64_t docs_count = segment.Meta().docs_count;
  uint64_t window = 0;
  duckdb::idx_t count = 0;
  const auto flush = [&] {
    if (count == 0) {
      return;
    }
    chunk.Reset();
    const auto scanned = scanner.Scan(
      window, std::min<uint64_t>(STANDARD_VECTOR_SIZE, docs_count - window),
      chunk, &rows, count);
    chunk.SetCardinality(scanned);
    duckdb::Vector keys{duckdb::LogicalType::BLOB, scanned};
    encoder.Encode(chunk, column_ids, keys);
    auto values = keys.Values<duckdb::string_t>();
    for (duckdb::idx_t row = 0; row != scanned; ++row) {
      if (const auto entry = values[row]; entry.IsValid()) {
        const auto& value = entry.GetValue();
        visit({value.GetData(), value.GetSize()});
      }
    }
    count = 0;
  };
  irs::DocumentMask::Cursor cursor{docs};
  std::array<irs::doc_id_t, STANDARD_VECTOR_SIZE> batch;
  while (const auto read = cursor.Read(batch)) {
    for (const auto doc : std::span{batch}.first(read)) {
      const uint64_t row = doc - irs::doc_limits::min();
      const auto start = row - row % STANDARD_VECTOR_SIZE;
      if (count != 0 && start != window) {
        flush();
      }
      window = start;
      rows.set_index(count++, row - start);
    }
  }
  flush();
}

// Feeds `visit` the key under `key` of every row of `chunk` holding one, with
// the document each row is: `first` for the first row.
void ForEachChunkKey(
  duckdb::DataChunk& chunk, std::span<const connector::ColumnId> column_ids,
  const connector::ConstraintKeyEncoder& key, irs::doc_id_t first,
  absl::FunctionRef<void(std::string_view key, irs::doc_id_t doc)> visit) {
  const auto count = chunk.size();
  duckdb::Vector keys{duckdb::LogicalType::BLOB, count};
  key.Encode(chunk, column_ids, keys);
  auto values = keys.Values<duckdb::string_t>();
  for (duckdb::idx_t row = 0; row != count; ++row) {
    const auto entry = values[row];
    if (!entry.IsValid()) {
      continue;
    }
    const auto& value = entry.GetValue();
    visit({value.GetData(), value.GetSize()},
          static_cast<irs::doc_id_t>(first + row));
  }
}

struct KeyedRow {
  irs::field_id field = 0;
  std::string key;
  irs::doc_id_t doc = 0;
};

// Every key the rows of `w` hold more than once, or that a live row of
// `existing` holds, for unique key `spec`: the rows are `segments`, read
// through their term dictionaries, and `keys`, sorted.
std::optional<std::string> FindDuplicateIn(
  const KeySpec& spec, std::span<const KeyProbe::Segment> segments,
  std::span<const std::string> keys, const KeyProbe& existing, KeyRange range) {
  std::optional<std::string> found;
  size_t next = 0;
  const auto settle = [&](std::string_view key, size_t rows) {
    if (rows > 1 || existing.Exists(key)) {
      found.emplace(key);
      return false;
    }
    return true;
  };
  const auto take_run = [&] {
    size_t rows = 1;
    while (next + rows != keys.size() && keys[next + rows] == keys[next]) {
      ++rows;
    }
    return rows;
  };
  ForEachSegmentKey(
    segments, spec.key.field,
    [&](std::string_view key, size_t rows) {
      while (next != keys.size() && keys[next] < key) {
        const auto run = take_run();
        if (!settle(keys[next], run)) {
          return false;
        }
        next += run;
      }
      while (next != keys.size() && keys[next] == key) {
        ++rows;
        ++next;
      }
      return settle(key, rows);
    },
    range);
  while (!found && next != keys.size()) {
    const auto run = take_run();
    if (settle(keys[next], run)) {
      next += run;
    }
  }
  return found;
}

struct DuplicateCheck final : duckdb::BaseExecutorTask {
  DuplicateCheck(duckdb::TaskExecutor& executor,
                 absl::AnyInvocable<void()> check)
    : BaseExecutorTask{executor}, check{std::move(check)} {}

  void ExecuteTask() final { check(); }

  absl::AnyInvocable<void()> check;
};

std::optional<std::string> FindDuplicate(
  duckdb::ClientContext& context, const KeySpec& spec,
  std::span<const KeyProbe::Segment> segments,
  std::span<const std::string> keys, const KeyProbe& existing) {
  constexpr uint64_t kMinKeysPerCheck = 1 << 16;
  constexpr uint64_t kChecksPerThread = 4;
  const auto total = SegmentKeyCount(segments, spec.key.field) + keys.size();
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(context);
  const auto parts = std::min<uint64_t>(
    kChecksPerThread * static_cast<uint64_t>(scheduler.NumberOfThreads()),
    total / kMinKeysPerCheck);
  if (parts < 2) {
    return FindDuplicateIn(spec, segments, keys, existing, {});
  }
  const auto bounds = SplitSegmentKeys(segments, spec.key.field, keys, parts);
  std::vector<std::optional<std::string>> found(bounds.size() + 1);
  duckdb::TaskExecutor executor{scheduler, context.GetSchedulerAccount()};
  for (size_t part = 0; part != found.size(); ++part) {
    const KeyRange range{
      .from = part == 0 ? std::string_view{} : bounds[part - 1],
      .to = part == bounds.size() ? std::string_view{} : bounds[part]};
    const auto begin =
      range.from.empty() ? keys.begin() : absl::c_lower_bound(keys, range.from);
    const auto end =
      range.to.empty() ? keys.end() : absl::c_lower_bound(keys, range.to);
    executor.ScheduleTask(duckdb::make_uniq<DuplicateCheck>(
      executor,
      [&, range, slice = std::span{begin, end}, &result = found[part]] {
        result = FindDuplicateIn(spec, segments, slice, existing, range);
      }));
  }
  executor.WorkOnTasks();
  for (auto& duplicate : found) {
    if (duplicate) {
      return std::move(duplicate);
    }
  }
  return std::nullopt;
}

void TakeRemovals(std::vector<std::shared_ptr<const irs::DocRemoval>>& removals,
                  absl::FunctionRef<bool(uint64_t segment)> taken,
                  absl::FunctionRef<void(const irs::DocumentMask& docs)> take) {
  for (auto& removal : removals) {
    if (absl::c_none_of(removal->groups, [&](const auto& group) {
          return taken(group.segment);
        })) {
      continue;
    }
    auto rest = std::make_shared<irs::DocRemoval>();
    for (const auto& group : removal->groups) {
      if (taken(group.segment)) {
        take(group.docs);
        continue;
      }
      rest->groups.push_back(
        {.segment = group.segment,
         .docs = irs::DocumentMaskBuilder{group.docs}.Finish(
           irs::DocumentMaskBuilder::kCanonical)});
    }
    removal = std::move(rest);
  }
  std::erase_if(removals,
                [](const auto& removal) { return removal->groups.empty(); });
}

std::shared_ptr<const irs::DocumentMask> TakeRowRemovals(
  std::vector<std::shared_ptr<const irs::DocRemoval>>& removals,
  uint64_t rows_id) {
  irs::DocumentMaskBuilder taken;
  TakeRemovals(
    removals, [&](uint64_t segment) { return segment == rows_id; },
    [&](const irs::DocumentMask& docs) { taken.Merge(docs); });
  if (taken.Empty()) {
    return nullptr;
  }
  return std::make_shared<const irs::DocumentMask>(std::move(taken).Finish());
}

void ForEachLiveChunk(duckdb::ColumnDataCollection& rows,
                      const irs::DocumentMask* removed, const auto& visit) {
  duckdb::SelectionVector live{STANDARD_VECTOR_SIZE};
  std::array<uint64_t, STANDARD_VECTOR_SIZE / 64 + 1> words;
  auto doc = irs::doc_limits::min();
  for (auto& chunk : rows.Chunks()) {
    const auto count = chunk.size();
    const auto first = doc;
    doc += static_cast<irs::doc_id_t>(count);
    if (removed) {
      SDB_ASSERT(0 < count && count <= STANDARD_VECTOR_SIZE);
      const auto first_word = first / 64;
      removed->Words(first_word, std::span{words}.first(
                                   (first + count - 1) / 64 - first_word + 1));
      duckdb::idx_t kept = 0;
      for (duckdb::idx_t i = 0; i < count; ++i) {
        const auto bit = first + i - uint64_t{first_word} * 64;
        if (((words[bit / 64] >> (bit % 64)) & 1) == 0) {
          live.set_index(kept++, i);
        }
      }
      if (kept == 0) {
        continue;
      }
      if (kept != count) {
        chunk.Slice(live, kept);
      }
    }
    visit(chunk);
  }
}

bool Logged(const StoreWrites& w) {
  return w.owner->ParentCatalog().Cast<catalog::BaseCatalog>().Durable();
}

OwnWrites OwnOf(const StoreWrites& w) {
  return {.rows = w.changes.collection.get(),
          .rows_id = OwnRowsId(w.rows_generation),
          .row_columns = w.changes.column_ids,
          .segments = w.own_segments,
          .removals = w.changes.removals,
          .patches = w.changes.patches};
}

void FlushRows(duckdb::ClientContext& context, StoreWrites& w) {
  auto& store = *w.store;
  auto& changes = w.changes;
  SDB_ASSERT(changes.HasBufferedRows());
  const auto rows_id = OwnRowsId(w.rows_generation);
  const auto mask = TakeRowRemovals(changes.removals, rows_id);
  auto trx =
    std::make_unique<irs::IndexWriter::Transaction>(store.GetTransaction());
  {
    const auto sink = connector::MakeSearchTableInsertSink(
      *trx, store, *changes.catalog, context);
    ForEachLiveChunk(
      *changes.collection, mask.get(), [&](duckdb::DataChunk& chunk) {
        connector::WriteChunkToSearchSink(*sink, chunk, changes.column_ids,
                                          store.GetId(), context);
      });
  }
  auto segments = store.OpenFlushed(trx->FlushAndFsync());
  irs::doc_id_t rank = 0;
  const auto first_map = w.own_maps.size();
  for (auto& segment : segments) {
    const auto docs = segment.reader->Meta().docs_count;
    w.own_maps.push_back({.source = rows_id,
                          .target = segment.reader->Id(),
                          .mask = mask,
                          .rank_begin = rank,
                          .rank_end = static_cast<irs::doc_id_t>(rank + docs)});
    rank += docs;
    w.own_segments.push_back(std::move(segment));
  }
  CarryUnitPatches(changes.patches, rows_id,
                   std::span{w.own_maps}.subspan(first_map));
  w.transactions.push_back(std::move(trx));
  changes.ClearBufferedRows();
  ++w.rows_generation;
}

}  // namespace

const WriterGenerations::Writer* Writes::Registration(
  duckdb::idx_t store_id) const {
  absl::ReaderMutexLock lock{&_mutex};
  const auto it = _writes.find(store_id);
  return it == _writes.end() ? nullptr : &it->second.registration;
}

std::vector<const WriterGenerations*> Writes::Registrations() const {
  absl::ReaderMutexLock lock{&_mutex};
  std::vector<const WriterGenerations*> registrations;
  for (const auto& [id, w] : _writes) {
    if (w.registration) {
      registrations.push_back(w.registration.get());
    }
  }
  return registrations;
}

StoreWrites& Writes::EnsureStoreWrites(const std::shared_ptr<Store>& store) {
  absl::MutexLock lock{&_mutex};
  auto& w = _writes[store->GetId()];
  if (!w.store) {
    w.store = store;
    if (store->HasTable()) {
      const auto* state =
        store->GetTable().VersionAt(Snapshot(), _walks).state.get();
      w.pin.emplace(
        StoreWrites::Pin{.state = state, .manifest = state->manifest.get()});
      w.pinned.store(&*w.pin, std::memory_order_relaxed);
    }
    w.listed = _listed.load(std::memory_order_relaxed);
    _listed.store(&w, std::memory_order_release);
  }
  return w;
}

StoreWrites* Writes::FindStoreWrites(duckdb::idx_t store_id) const {
  for (auto* w = _listed.load(std::memory_order_acquire); w; w = w->listed) {
    if (w->store->GetId() == store_id) {
      return w;
    }
  }
  return nullptr;
}

void Writes::Adopt(StoreWrites& w, const catalog::SearchTableEntry& table) {
  w.keys = table.KeySpecs();
  for (const auto& key : w.store->Config()->constraint_keys) {
    if (absl::c_none_of(w.keys, [&](const KeySpec& spec) {
          return spec.key.field == key.field;
        })) {
      if (auto spec = IndexKeySpec(table, key, Latest(table))) {
        w.keys.push_back(std::move(*spec));
      }
    }
  }
  w.table = &table;
  w.owner = &table;
}

void Writes::AdoptTable(const std::shared_ptr<Store>& store,
                        const catalog::SearchTableEntry& table) {
  if (auto* w = FindStoreWrites(store->GetId())) {
    absl::MutexLock lock{&w->mutex};
    Adopt(*w, table);
  }
}

void Writes::RegisterWriter(const std::shared_ptr<Store>& store,
                            const catalog::SearchTableEntry& table) {
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  if (w.table != &table) {
    Adopt(w, table);
  }
  const auto refuse = [&](std::string_view activity) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("Attempting to write to table ", table.name.GetIdentifierName(),
              " but another transaction ", activity, " it"));
  };
  if (!w.dropping && store->Dropped()) {
    refuse("dropped");
  }
  if (!w.dropping && store->ExclusiveClaim() == Store::Exclusive::Drop) {
    refuse("is dropping");
  }
  if (!w.registration) {
    w.registration = store->RegisterWriter();
  }
}

void Writes::AddOwnSegments(
  const std::shared_ptr<Store>& store,
  std::unique_ptr<irs::IndexWriter::Transaction> trx,
  std::span<const irs::IndexWriter::FlushedSegment> flushed) {
  auto segments = store->OpenFlushed(flushed);
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  for (auto& segment : segments) {
    w.own_segments.push_back(std::move(segment));
  }
  w.transactions.push_back(std::move(trx));
  ++w.revision;
}

void Writes::AddInlineRows(duckdb::ClientContext& context,
                           const std::shared_ptr<Store>& store,
                           std::span<const connector::ColumnId> column_ids,
                           duckdb::Catalog& catalog,
                           std::unique_ptr<duckdb::ColumnDataCollection> rows,
                           uint64_t bytes, uint64_t flush_above) {
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  w.changes.AppendInsertRows(std::move(rows), bytes, column_ids, catalog);
  ++w.revision;
  if (w.changes.BufferedBytes() > flush_above ||
      w.changes.collection->Count() > irs::doc_limits::kMaxDocs) {
    FlushRows(context, w);
  }
}

void Writes::RewriteOwn(
  duckdb::ClientContext& context, const std::shared_ptr<Store>& store,
  duckdb::Catalog& catalog,
  absl::FunctionRef<void(const irs::SubReader&,
                         connector::SearchSinkInsertBaseImpl&)>
    feed) {
  auto* found = FindStoreWrites(store->GetId());
  if (!found) {
    return;
  }
  auto& w = *found;
  {
    absl::MutexLock lock{&w.mutex};
    if (!w.changes.HasBufferedRows() && w.own_segments.empty()) {
      return;
    }
    std::vector<uint64_t> own{OwnRowsId(w.rows_generation)};
    for (const auto& segment : w.own_segments) {
      own.push_back(segment.reader->Id());
    }
    const auto read = store->Read(OwnOf(w));
    auto trx =
      std::make_unique<irs::IndexWriter::Transaction>(store->GetTransaction());
    {
      const auto sink =
        connector::MakeSearchTableInsertSink(*trx, *store, catalog, context);
      for (const auto& sub : read.Reader()) {
        if (absl::c_linear_search(
              own,
              irs::utils::downCast<irs::SegmentReader>(sub).GetImpl()->Id())) {
          feed(sub, *sink);
        }
      }
    }
    w.own_segments = store->OpenFlushed(trx->FlushAndFsync());
    w.transactions.clear();
    w.transactions.push_back(std::move(trx));
    w.own_maps.clear();
    TakeRemovals(
      w.changes.removals,
      [&](uint64_t segment) { return absl::c_linear_search(own, segment); },
      [](const irs::DocumentMask&) {});
    w.changes.ClearBufferedRows();
    w.changes.column_ids.clear();
    ++w.rows_generation;
    w.checked_segments = 0;
    w.buffered_keys.Clear();
    w.keyed_rows = 0;
    ++w.revision;
    w.pinned.store(nullptr, std::memory_order_relaxed);
    w.pin.reset();
  }
}

void Writes::Adopting(const std::shared_ptr<Store>& store) {
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  w.adopting = true;
  w.pinned.store(nullptr, std::memory_order_relaxed);
  w.pin.reset();
}

void Writes::Rebuilt(const std::shared_ptr<Store>& store,
                     std::shared_ptr<const TableState> state, bool newer) {
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  if (state) {
    w.rebuilt = std::move(state);
  }
  w.rebuilt_newer = w.rebuilt_newer || newer;
  ++w.revision;
  w.pinned.store(nullptr, std::memory_order_relaxed);
  w.pin.reset();
}

const StoreWrites::Pin& Writes::EnsurePin(duckdb::ClientContext& context,
                                          StoreWrites& w) {
  if (const auto* pin = w.pinned.load(std::memory_order_acquire)) {
    return *pin;
  }
  absl::MutexLock lock{&w.mutex};
  if (w.pin) {
    return *w.pin;
  }
  if (w.rebuilt_newer) {
    ThrowConcurrentUpdate();
  }
  const auto& table = w.store->GetTable();
  const auto snapshot = Snapshot();
  const TableState* state = nullptr;
  if (w.rebuilt && w.rebuilt->tick > snapshot) {
    state = w.rebuilt.get();
  } else {
    SDB_ASSERT(!w.adopting || w.rebuilt);
    state = table.VersionAt(snapshot, _walks).state.get();
  }
  if (w.store->HasIndexes() && w.changes.HasBufferedRows()) {
    FlushRows(context, w);
  }
  auto& pin = w.pin.emplace(StoreWrites::Pin{
    .state = state,
    .manifest = w.manifest ? w.manifest.get() : state->manifest.get(),
    .revision = w.revision});
  if (const auto own = OwnOf(w); HasOwn(own)) {
    pin.own.emplace(table.View(*state, own));
  }
  w.pinned.store(&pin, std::memory_order_release);
  return pin;
}

ReadView Writes::Read(duckdb::ClientContext& context, Store& store) {
  const auto& table = store.GetTable();
  if (auto* w = FindStoreWrites(store.GetId())) {
    const auto& pin = EnsurePin(context, *w);
    return {
      .reader = &pin.Reader(), .state = pin.state, .manifest = pin.manifest};
  }
  const auto& version = table.VersionAt(Snapshot(), _walks);
  return {.reader = &version.state->reader,
          .state = version.state.get(),
          .manifest = version.state->manifest.get()};
}

Tick Writes::PlanVersion(const Store& store) {
  bool own = false;
  if (auto* w = FindStoreWrites(store.GetId())) {
    absl::MutexLock lock{&w->mutex};
    own = w->rebuilt || HasOwn(OwnOf(*w));
  }
  const auto last = store.GetTable().LastWrite();
  return own || last > Snapshot() ? catalog::Transactions::Get().NextId()
                                  : last;
}

const irs::DirectoryReader& Writes::PinnedReader(duckdb::ClientContext& context,
                                                 Store& store) {
  auto* w = FindStoreWrites(store.GetId());
  SDB_ENSURE(w, "table ", store.GetId(),
             ": deletes without the reader their positions come from");
  return EnsurePin(context, *w).Reader();
}

void Writes::ResetPins(bool all) noexcept {
  absl::MutexLock lock{&_mutex};
  for (auto& [store_id, w] : _writes) {
    if (all || (w.pin && w.pin->revision != w.revision)) {
      w.pinned.store(nullptr, std::memory_order_relaxed);
      w.pin.reset();
    }
  }
}

void Writes::AddRemoval(duckdb::ClientContext& context,
                        const std::shared_ptr<Store>& store,
                        irs::DocRemovalBuilder&& positions) {
  const auto& reader = PinnedReader(context, *store);
  auto removal = std::move(positions).Finish(reader);
  auto& w = EnsureStoreWrites(store);
  std::vector<StoreWrites::RemovedKeys> removed;
  if (const auto referencing = store->ReferencingKeys(); !referencing.empty()) {
    std::vector<KeySpec> specs;
    {
      absl::MutexLock lock{&w.mutex};
      for (const auto& reference : referencing) {
        if (const auto* spec = UniqueSpec(w, reference->referenced);
            spec && absl::c_none_of(specs, [&](const KeySpec& taken) {
              return taken.key.field == spec->key.field;
            })) {
          specs.push_back(*spec);
        }
      }
    }
    absl::flat_hash_map<uint64_t, const irs::SubReader*> segments;
    for (const auto& segment : reader) {
      segments.emplace(
        irs::utils::downCast<irs::SegmentReader>(segment).GetImpl()->Id(),
        &segment);
    }
    for (const auto& spec : specs) {
      auto& keys =
        removed.emplace_back(StoreWrites::RemovedKeys{.field = spec.key.field});
      for (const auto& group : removal->groups) {
        if (const auto it = segments.find(group.segment);
            it != segments.end()) {
          ForEachRowKey(
            context, store->GetId(), spec, *it->second, group.docs,
            [&](std::string_view key) { keys.keys.emplace_back(key); });
        }
      }
    }
  }
  absl::MutexLock lock{&w.mutex};
  for (auto& keys : removed) {
    const auto it =
      absl::c_find_if(w.removed_keys, [&](const StoreWrites::RemovedKeys& own) {
        return own.field == keys.field;
      });
    if (it == w.removed_keys.end()) {
      keys.complete = !w.removed_rows;
      w.removed_keys.push_back(std::move(keys));
    } else {
      absl::c_move(keys.keys, std::back_inserter(it->keys));
    }
  }
  w.removed_rows = true;
  ++w.revision;
  const auto flushed_rows = [&](uint64_t id) {
    return id > OwnRowsId(w.rows_generation);
  };
  if (absl::c_any_of(removal->groups, [&](const auto& group) {
        return flushed_rows(group.segment);
      })) {
    absl::flat_hash_map<uint64_t, irs::DocumentMaskBuilder> mapped;
    for (const auto& group : removal->groups) {
      if (!flushed_rows(group.segment)) {
        mapped[group.segment].Merge(group.docs);
        continue;
      }
      for (const auto& map : w.own_maps) {
        if (map.source == group.segment) {
          irs::MapDocs(map, group.docs, mapped[map.target]);
        }
      }
    }
    auto translated = std::make_shared<irs::DocRemoval>();
    for (auto& [id, docs] : mapped) {
      if (!docs.Empty()) {
        translated->groups.push_back({.segment = id,
                                      .docs = std::move(docs).Finish(
                                        irs::DocumentMaskBuilder::kCanonical)});
      }
    }
    removal = std::move(translated);
  }
  if (!removal->groups.empty()) {
    AddChanged(w, Snapshot(), removal);
    w.changes.removals.push_back(std::move(removal));
  }
}

void Writes::AddPatches(const std::shared_ptr<Store>& store,
                        std::vector<UnitPatch>&& patches) {
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  ++w.revision;
  const auto rows_id = OwnRowsId(w.rows_generation);
  auto changed = std::make_shared<irs::DocRemoval>();
  for (auto& patch : patches) {
    if (patch.unit < rows_id) {
      changed->groups.push_back(
        {.segment = patch.unit, .docs = PatchedDocs(patch)});
    }
    std::vector<UnitPatch> added;
    added.push_back(std::move(patch));
    if (added.front().unit > rows_id) {
      CarryUnitPatches(added, added.front().unit, w.own_maps);
    }
    MergeUnitPatches(w.changes.patches, std::move(added));
  }
  if (!changed->groups.empty()) {
    AddChanged(w, Snapshot(), std::move(changed));
  }
}

void Writes::CheckChanged(const StoreWrites& w,
                          std::span<const StoreWrites::ChangedRows> changed) {
  for (const auto& [snapshot, rows] : changed) {
    for (const auto& removal : rows) {
      if (const auto tick = w.store->GetTable().Conflict(*removal, snapshot)) {
        _conflict = std::max(_conflict, tick);
        ThrowConcurrentUpdate();
      }
    }
  }
}

void Writes::ThrowDuplicateOrConflict(duckdb::ClientContext& context,
                                      const StoreWrites& w, const KeySpec& spec,
                                      std::string_view key, bool merge_insert) {
  CheckChanged(w, w.changed);
  if (merge_insert) {
    const auto& table = w.store->GetTable();
    const auto committed = [&](std::shared_ptr<const TableState> state) {
      const LiveUnits live{std::move(state), w.changes.removals};
      KeyProbe probe{spec.key.field};
      probe.AddLive(live);
      return probe.Exists(key);
    };
    if (committed(table.State()) &&
        !committed(table.VersionAt(Snapshot(), _walks).state)) {
      ThrowConcurrentUpdate();
    }
  }
  ThrowDuplicate(context, spec, key);
}

void Writes::CheckConflicts(const Store& store) {
  if (const auto* w = FindStoreWrites(store.GetId())) {
    absl::MutexLock lock{&w->mutex};
    if (!w->changed.empty() && w->changed.back().snapshot == Snapshot()) {
      CheckChanged(*w, std::span{w->changed}.last(1));
    }
  }
}

void Writes::AddSearchTruncate(duckdb::ClientContext& context,
                               const std::shared_ptr<Store>& store) {
  auto& w = EnsureStoreWrites(store);
  const auto& visible = EnsurePin(context, w).state->reader;
  irs::DocRemovalBuilder all;
  for (uint32_t i = 0; i < visible.size(); ++i) {
    const auto& segment = visible[i];
    auto& docs = all.Group(i);
    docs.AddRange(irs::doc_limits::min(),
                  static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                             segment.docs_count()));
    if (const auto* deleted = segment.docs_mask()) {
      docs.Subtract(*deleted);
    }
  }
  auto removal = std::move(all).Finish(visible);
  absl::MutexLock lock{&w.mutex};
  w.transactions.clear();
  w.own_segments.clear();
  w.own_maps.clear();
  w.checked_segments = 0;
  ++w.rows_generation;
  if (!removal->groups.empty()) {
    AddChanged(w, Snapshot(), removal);
  }
  w.changes.AppendTruncate(std::move(removal));
  ++w.revision;
}

void Writes::ClaimExclusive(const std::shared_ptr<Store>& store,
                            Store::Exclusive kind) {
  SDB_ASSERT(kind == Store::Exclusive::Drop);
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  w.dropping = true;
}

void Writes::RegisterIndex(const std::shared_ptr<Store>& store,
                           const duckdb::CatalogEntry& index) {
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  w.owner = &index;
}

void Writes::SetManifest(const std::shared_ptr<Store>& store,
                         std::shared_ptr<const FileManifest> manifest) {
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  w.manifest = std::move(manifest);
  ++w.revision;
  w.pinned.store(nullptr, std::memory_order_relaxed);
  w.pin.reset();
}

namespace {

KeyProbe::Segment OwnSegmentSource(
  const std::shared_ptr<const irs::SegmentReaderImpl>& reader,
  const LiveUnits& live) {
  const auto id = reader->Id();
  return {.unit = id, .sub = reader.get(), .removed = live.Removed(id)};
}

KeyProbe::Segment OwnSegmentSource(const OwnSegment& segment,
                                   const LiveUnits& live) {
  return OwnSegmentSource(segment.reader, live);
}

// The rows of `w` that are not committed yet: its own segments, and the rows
// it buffers or, once its commit is prepared, sealed into its piece.
void AddOwnRows(KeyProbe& probe, const StoreWrites& w, const LiveUnits& live) {
  for (const auto& segment : w.own_segments) {
    probe.Add(OwnSegmentSource(segment, live));
  }
  if (w.prepared) {
    if (w.piece) {
      probe.Add(OwnSegmentSource(w.piece, live));
    }
    return;
  }
  if (w.keyed_rows != 0 && w.keyed_generation == w.rows_generation) {
    const auto rows_id = OwnRowsId(w.rows_generation);
    probe.Add(KeyProbe::Hot{.unit = rows_id,
                            .keys = &w.buffered_keys,
                            .docs = static_cast<irs::doc_id_t>(w.keyed_rows),
                            .removed = live.Removed(rows_id)});
  }
}

class LockedWrites {
 public:
  LockedWrites(irs::containers::NodeHashMap<duckdb::idx_t, StoreWrites>& writes,
               const StoreWrites& own) ABSL_NO_THREAD_SAFETY_ANALYSIS
    : _own{&own} {
    _locked.reserve(writes.size());
    for (auto& [store_id, w] : writes) {
      _locked.push_back({.store_id = store_id, .writes = &w});
    }
    absl::c_sort(_locked, [](const Locked& lhs, const Locked& rhs) {
      return lhs.store_id < rhs.store_id;
    });
    for (const auto& locked : _locked) {
      if (locked.writes == _own) {
        locked.writes->mutex.Lock();
      } else {
        locked.writes->mutex.ReaderLock();
      }
    }
  }

  ~LockedWrites() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    for (auto it = _locked.rbegin(); it != _locked.rend(); ++it) {
      if (it->writes == _own) {
        it->writes->mutex.Unlock();
      } else {
        it->writes->mutex.ReaderUnlock();
      }
    }
  }

  LockedWrites(const LockedWrites&) = delete;
  LockedWrites& operator=(const LockedWrites&) = delete;

 private:
  struct Locked {
    duckdb::idx_t store_id = 0;
    StoreWrites* writes = nullptr;
  };

  const StoreWrites* _own;
  std::vector<Locked> _locked;
};

// The rows of a table holding keys, as one transaction sees them: the latest
// committed rows but the ones it removed, and the rows it wrote.
class TableRows {
 public:
  TableRows(const Store& store, const StoreWrites* own)
    : _own{own},
      _live{
        store.GetTable().State(),
        own
          ? std::span<const std::shared_ptr<const irs::DocRemoval>>{own->changes
                                                                      .removals}
          : std::span<const std::shared_ptr<const irs::DocRemoval>>{}} {}

  const LiveUnits& Live() const noexcept { return _live; }

  KeyProbe Probe(irs::field_id field) const {
    KeyProbe probe{field};
    probe.AddLive(_live);
    if (_own) {
      AddOwnRows(probe, *_own, _live);
    }
    return probe;
  }

 private:
  const StoreWrites* _own;
  LiveUnits _live;
};

// The keys of the rows of `w` under `field` that are not committed yet.
std::vector<std::string> OwnKeys(const StoreWrites& w, irs::field_id field,
                                 const LiveUnits& live) {
  std::vector<std::string> keys;
  std::vector<KeyProbe::Segment> segments;
  segments.reserve(w.own_segments.size() + 1);
  for (const auto& segment : w.own_segments) {
    segments.push_back(OwnSegmentSource(segment, live));
  }
  if (w.piece) {
    segments.push_back(OwnSegmentSource(w.piece, live));
  }
  ForEachSegmentKey(segments, field, [&](std::string_view key, size_t) {
    keys.emplace_back(key);
    return true;
  });
  absl::c_sort(keys);
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  return keys;
}

}  // namespace

StoreWrites* Writes::Related(const Store& store, StoreWrites& w) {
  if (w.store.get() == &store) {
    return &w;
  }
  const auto it = _writes.find(store.GetId());
  return it == _writes.end() ? nullptr : &it->second;
}

void Writes::CheckReferences(duckdb::ClientContext& context, StoreWrites& w,
                             const KeySpec& spec,
                             std::span<const std::string> keys) {
  const auto* parent = ReferencedTable(*w.table, spec, Latest(*w.table));
  if (!parent || spec.referenced_field == 0) {
    ThrowMissingReference(context, *w.table, spec, keys.front());
  }
  const auto& store = *parent->Storage();
  const TableRows rows{store, Related(store, w)};
  auto probe = rows.Probe(spec.referenced_field);
  for (const auto& key : keys) {
    if (!probe.Exists(key)) {
      ThrowMissingReference(context, *w.table, spec, key);
    }
  }
  w.related_checked.try_emplace(store.GetId(), rows.Live().State());
}

void Writes::CheckRemovedKeys(duckdb::ClientContext& context, StoreWrites& w,
                              bool commit) {
  if (w.removed_keys.empty()) {
    return;
  }
  const auto referencing = w.store->ReferencingKeys();
  const auto latest = Latest(*w.table);
  std::optional<TableRows> own;
  for (auto& removed : w.removed_keys) {
    const auto* unique = UniqueSpec(w, removed.field);
    const auto children =
      unique ? LiveReferencing(*w.table, referencing, removed.field, latest)
             : std::vector<ReferencingKey>{};
    std::vector<std::string_view> keys;
    for (auto& child : children) {
      const auto& store = *child.table->Storage();
      const auto changed = [&] {
        const auto it = w.related_checked.find(store.GetId());
        return it == w.related_checked.end() ||
               it->second != store.GetTable().State();
      };
      const size_t begin = commit && changed() ? 0 : removed.checked;
      if (begin == removed.keys.size()) {
        continue;
      }
      keys.assign(removed.keys.begin() + begin, removed.keys.end());
      absl::c_sort(keys);
      keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
      const TableRows rows{store, Related(store, w)};
      auto probe = rows.Probe(child.spec->key.field);
      std::optional<KeyProbe> held;
      for (const auto key : keys) {
        if (!probe.Exists(key)) {
          continue;
        }
        if (!own) {
          own.emplace(*w.store, &w);
        }
        if (!held) {
          held.emplace(own->Probe(removed.field));
        }
        if (!held->Exists(key)) {
          ThrowStillReferenced(context, *w.table, *unique, child, key);
        }
      }
      w.related_checked.try_emplace(store.GetId(), rows.Live().State());
    }
    removed.checked = removed.keys.size();
  }
}

void Writes::CheckKeys(duckdb::ClientContext& context, const Store& store,
                       bool merge_insert) {
  absl::MutexLock all{&_mutex};
  const auto found = _writes.find(store.GetId());
  if (found == _writes.end()) {
    return;
  }
  auto& w = found->second;
  {
    absl::ReaderMutexLock lock{&w.mutex};
    if (!w.table || (w.keys.empty() && w.removed_keys.empty())) {
      return;
    }
  }
  const LockedWrites locked{_writes, w};
  if (w.keyed_generation != w.rows_generation) {
    w.buffered_keys.Clear();
    w.keyed_rows = 0;
    w.keyed_generation = w.rows_generation;
  }
  const bool checks_all = w.keyed_rows == 0 && w.checked_segments == 0;
  std::vector<std::vector<std::string>> fresh(w.keys.size());
  std::vector<KeyedRow> keyed;
  const duckdb::idx_t rows =
    w.changes.HasBufferedRows() ? w.changes.collection->Count() : 0;
  if (merge_insert &&
      (rows > w.keyed_rows || w.own_segments.size() > w.checked_segments)) {
    w.merge_inserted = true;
  }
  if (rows > w.keyed_rows) {
    const auto& collection = *w.changes.collection;
    std::vector<connector::ConstraintKeyEncoder> encoders;
    encoders.reserve(w.keys.size());
    for (const auto& spec : w.keys) {
      encoders.emplace_back(context, w.store->GetId(), spec.key);
    }
    duckdb::ColumnDataScanState scan;
    collection.InitializeScan(scan);
    duckdb::DataChunk chunk;
    collection.InitializeScanChunk(scan, chunk);
    for (bool more = collection.Seek(w.keyed_rows, scan, chunk); more;
         more = collection.Scan(scan, chunk)) {
      const auto first = scan.current_row_index;
      for (size_t spec = 0; spec != w.keys.size(); ++spec) {
        ForEachChunkKey(
          chunk, w.changes.column_ids, encoders[spec],
          static_cast<irs::doc_id_t>(irs::doc_limits::min() + first),
          [&](std::string_view key, irs::doc_id_t doc) {
            if (doc - irs::doc_limits::min() < w.keyed_rows) {
              return;
            }
            fresh[spec].emplace_back(key);
            keyed.push_back({.field = w.keys[spec].key.field,
                             .key = std::string{key},
                             .doc = doc});
          });
      }
    }
  }
  LiveUnits live{store.GetTable().State(), w.changes.removals};
  const auto rows_id = OwnRowsId(w.rows_generation);
  std::vector<KeyProbe::Segment> segments;
  for (size_t i = w.checked_segments; i < w.own_segments.size(); ++i) {
    segments.push_back(OwnSegmentSource(w.own_segments[i], live));
  }
  for (size_t spec = 0; spec != w.keys.size(); ++spec) {
    const auto& key_spec = w.keys[spec];
    if (key_spec.kind != KeyKind::Unique || key_spec.deferred) {
      continue;
    }
    auto& keys = fresh[spec];
    if (keys.empty() && segments.empty()) {
      continue;
    }
    KeyProbe existing{key_spec.key.field};
    existing.AddLive(live);
    for (size_t i = 0; i < w.checked_segments; ++i) {
      existing.Add(OwnSegmentSource(w.own_segments[i], live));
    }
    if (w.keyed_rows != 0) {
      existing.Add(
        KeyProbe::Hot{.unit = rows_id,
                      .keys = &w.buffered_keys,
                      .docs = static_cast<irs::doc_id_t>(w.keyed_rows),
                      .removed = live.Removed(rows_id)});
    }
    absl::c_sort(keys);
    if (auto duplicate =
          FindDuplicate(context, key_spec, segments, keys, existing)) {
      ThrowDuplicateOrConflict(context, w, key_spec, *duplicate, merge_insert);
    }
  }
  for (const auto& row : keyed) {
    w.buffered_keys.Add(row.field, row.key, row.doc);
  }
  w.keyed_rows = rows;
  w.checked_segments = w.own_segments.size();
  if (duckdb::GetReplicationRole(context) == duckdb::ReplicationRole::REPLICA) {
    w.removed_keys.clear();
    if (checks_all) {
      w.checked.emplace(std::move(live));
    }
    return;
  }
  for (size_t spec = 0; spec != w.keys.size(); ++spec) {
    const auto& key_spec = w.keys[spec];
    if (key_spec.kind != KeyKind::Reference) {
      continue;
    }
    auto& keys = fresh[spec];
    ForEachSegmentKey(segments, key_spec.key.field,
                      [&](std::string_view key, size_t) {
                        keys.emplace_back(key);
                        return true;
                      });
    if (keys.empty()) {
      continue;
    }
    absl::c_sort(keys);
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    CheckReferences(context, w, key_spec, keys);
  }
  CheckRemovedKeys(context, w, false);
  if (checks_all) {
    w.checked.emplace(std::move(live));
  }
}

std::shared_ptr<const TableState> Writes::VerifyAllReferences(
  duckdb::ClientContext& context, const catalog::SearchTableEntry& table,
  const KeySpec& spec) {
  const auto* parent = ReferencedTable(table, spec, Latest(table));
  const auto own = _writes.find(table.Storage()->GetId());
  const auto read = table.Storage()->Read(
    own == _writes.end() ? OwnWrites{} : OwnOf(own->second));
  std::vector<KeyProbe::Segment> segments;
  for (const auto& segment : read.Reader()) {
    segments.push_back({.sub = &segment});
  }
  if (!parent || spec.referenced_field == 0) {
    ForEachSegmentKey(segments, spec.key.field,
                      [&](std::string_view key, size_t) -> bool {
                        ThrowMissingReference(context, table, spec, key);
                      });
    return {};
  }
  const auto& store = *parent->Storage();
  const auto it = _writes.find(store.GetId());
  const auto* related = it == _writes.end() ? nullptr : &it->second;
  std::optional<absl::ReaderMutexLock> lock;
  if (related) {
    lock.emplace(&related->mutex);
  }
  const TableRows rows{store, related};
  auto probe = rows.Probe(spec.referenced_field);
  ForEachSegmentKey(segments, spec.key.field,
                    [&](std::string_view key, size_t) {
                      if (!probe.Exists(key)) {
                        ThrowMissingReference(context, table, spec, key);
                      }
                      return true;
                    });
  return rows.Live().State();
}

void Writes::VerifyReferences(duckdb::ClientContext& context,
                              const catalog::SearchTableEntry& table,
                              const KeySpec& spec) {
  absl::MutexLock all{&_mutex};
  auto referenced = VerifyAllReferences(context, table, spec);
  _reference_checks.push_back(
    {.table = &table, .spec = spec, .referenced = std::move(referenced)});
}

void Writes::CatchUpKeys(duckdb::ClientContext& context, StoreWrites& w) {
  if (!w.table || (w.keys.empty() && !w.removed_rows && !w.changes.truncate)) {
    return;
  }
  LiveUnits now{w.store->GetTable().State(), w.changes.removals};
  if (HasUniqueKeys(w) && (!w.own_segments.empty() || w.piece)) {
    std::vector<KeyProbe::Segment> segments;
    segments.reserve(w.own_segments.size() + 1);
    for (const auto& segment : w.own_segments) {
      segments.push_back(OwnSegmentSource(segment, now));
    }
    if (w.piece) {
      segments.push_back(OwnSegmentSource(w.piece, now));
    }
    for (const auto& spec : w.keys) {
      if (spec.kind != KeyKind::Unique) {
        continue;
      }
      KeyProbe fresh{spec.key.field};
      fresh.AddLive(now, w.checked && !spec.deferred ? &*w.checked : nullptr);
      if (fresh.Empty() && !spec.deferred) {
        continue;
      }
      if (auto duplicate = FindDuplicate(context, spec, segments, {}, fresh)) {
        ThrowDuplicateOrConflict(context, w, spec, *duplicate,
                                 w.merge_inserted);
      }
    }
  }
  const auto latest = Latest(*w.table);
  const bool replica =
    duckdb::GetReplicationRole(context) == duckdb::ReplicationRole::REPLICA;
  for (const auto& spec : w.keys) {
    if (replica || spec.kind != KeyKind::Reference) {
      continue;
    }
    const auto* parent = ReferencedTable(*w.table, spec, latest);
    if (parent) {
      const auto& store = *parent->Storage();
      const auto it = w.related_checked.find(store.GetId());
      if (it != w.related_checked.end() &&
          it->second == store.GetTable().State()) {
        continue;
      }
    }
    const auto keys = OwnKeys(w, spec.key.field, now);
    if (keys.empty()) {
      continue;
    }
    if (parent) {
      w.related_checked.erase(parent->Storage()->GetId());
    }
    CheckReferences(context, w, spec, keys);
  }
  const auto referencing = w.store->ReferencingKeys();
  if (w.changes.truncate) {
    for (const auto& spec : w.keys) {
      if (spec.kind != KeyKind::Unique) {
        continue;
      }
      for (const auto& child :
           LiveReferencing(*w.table, referencing, spec.key.field, latest)) {
        const auto* related = Related(*child.table->Storage(), w);
        if (related != &w && (!related || !related->changes.truncate)) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("cannot truncate a table referenced in a foreign key "
                    "constraint"),
            ERR_DETAIL("Table \"", child.table->name.GetIdentifierName(),
                       "\" references \"", w.table->name.GetIdentifierName(),
                       "\"."),
            ERR_HINT("Truncate table \"", child.table->name.GetIdentifierName(),
                     "\" at the same time, or use TRUNCATE ... CASCADE."));
        }
      }
    }
  } else if (w.removed_rows && !replica) {
    for (const auto& reference : referencing) {
      auto it = absl::c_find_if(w.removed_keys, [&](const auto& removed) {
        return removed.field == reference->referenced;
      });
      if (it != w.removed_keys.end() && it->complete) {
        continue;
      }
      const auto* spec = UniqueSpec(w, reference->referenced);
      if (!spec) {
        continue;
      }
      if (it == w.removed_keys.end()) {
        it = w.removed_keys.insert(
          w.removed_keys.end(),
          StoreWrites::RemovedKeys{.field = reference->referenced});
      }
      it->keys.clear();
      it->checked = 0;
      it->complete = true;
      const auto collect = [&](const irs::SubReader& reader,
                               const irs::DocumentMask* removed) {
        if (removed) {
          ForEachRowKey(
            context, w.store->GetId(), *spec, reader, *removed,
            [&](std::string_view key) { it->keys.emplace_back(key); });
        }
      };
      for (const auto& unit : now.Units()) {
        collect(*unit.unit->reader, unit.removed);
      }
      for (const auto& segment : w.own_segments) {
        collect(*segment.reader, now.Removed(segment.reader->Id()));
      }
      for (auto& child : w.related_checked) {
        child.second.reset();
      }
    }
    CheckRemovedKeys(context, w, true);
  }
  w.checked.emplace(std::move(now));
}

TableRead Writes::LatestView(duckdb::ClientContext& context, Store& store) {
  const auto& table = store.GetTable();
  auto* w = FindStoreWrites(store.GetId());
  if (!w) {
    return table.Read({});
  }
  absl::MutexLock lock{&w->mutex};
  if (w->rebuilt_newer) {
    ThrowConcurrentUpdate();
  }
  if (store.HasIndexes() && w->changes.HasBufferedRows()) {
    FlushRows(context, *w);
  }
  return table.Read(OwnOf(*w));
}

void Writes::PrepareCommit(duckdb::ClientContext& context) {
  for (const auto& [store_id, w] : _writes) {
    CheckChanged(w, w.changed);
  }
  for (auto& [store_id, w] : _writes) {
    auto& changes = w.changes;
    if (!w.owner ||
        (changes.Empty() && w.own_segments.empty() && !w.manifest)) {
      continue;
    }
    if (changes.HasBufferedRows() || !changes.removals.empty()) {
      SDB_IF_FAILURE("search_feed_pending_fails") {
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
    }
    if (!w.table) {
      SDB_IF_FAILURE("Search::FailOnCommit") {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                        ERR_MSG("debug failure point"));
      }
    }
    if (changes.HasBufferedRows() && w.store->OverBudget()) {
      FlushRows(context, w);
    }
    if (changes.HasBufferedRows()) {
      const auto rows_id = OwnRowsId(w.rows_generation);
      auto removed = TakeRowRemovals(changes.removals, rows_id);
      auto piece = w.store->StartPiece();
      const bool logged = Logged(w);
      if (logged) {
        w.rows.columns = changes.column_ids;
      }
      {
        const auto sink = connector::MakeSearchTableInsertSink(
          piece.trx, *w.store, *changes.catalog, context);
        ForEachLiveChunk(
          *changes.collection, removed.get(), [&](duckdb::DataChunk& chunk) {
            connector::WriteChunkToSearchSink(*sink, chunk, changes.column_ids,
                                              w.store->GetId(), context);
            if (logged) {
              w.rows.chunks.push_back(LogChunk(chunk));
            }
          });
      }
      const auto docs = PieceDocs(piece.trx);
      if (docs != 0) {
        w.piece = piece.trx.SealPiece(piece.id);
      }
      const std::array<irs::DocMap, 1> sealed{
        irs::DocMap{.source = rows_id,
                    .target = piece.id,
                    .mask = std::move(removed),
                    .rank_end = docs}};
      CarryUnitPatches(changes.patches, rows_id, sealed);
      changes.ClearBufferedRows();
    }
    w.buffered_keys.Clear();
    w.prepared = true;
  }
  for (auto& [store_id, w] : _writes) {
    if (w.prepared) {
      CatchUpKeys(context, w);
      SDB_PARK_ONCE_ON_FAILURE("search_commit_prepared");
    }
  }
}

std::vector<LogSection> Writes::TakeLog(duckdb::ClientContext& context) {
  for (auto& [store_id, w] : _writes) {
    if (!w.prepared) {
      continue;
    }
    if (!w.dropping && (w.store->Dropped() ||
                        w.store->ExclusiveClaim() == Store::Exclusive::Drop)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
        ERR_MSG("Attempting to write to table ",
                w.owner->name.GetIdentifierName(), " but another transaction ",
                w.store->Dropped() ? "dropped" : "is dropping", " it"));
    }
    auto& table = w.store->GetTable();
    CheckChanged(w, w.changed);
    for (auto& removal : w.changes.removals) {
      removal = table.Resolve(removal);
    }
    if (!w.changes.patches.empty()) {
      w.changes.patches = table.Resolve(std::move(w.changes.patches));
    }
  }
  for (auto& [store_id, w] : _writes) {
    if (w.prepared) {
      CatchUpKeys(context, w);
    }
  }
  const auto latest = [&](const ReferenceCheck& check) {
    const auto* parent =
      ReferencedTable(*check.table, check.spec, Latest(*check.table));
    return parent ? parent->Storage()->GetTable().State() : nullptr;
  };
  for (const auto& check : _reference_checks) {
    if (latest(check) != check.referenced) {
      VerifyAllReferences(context, *check.table, check.spec);
    }
  }
  std::vector<LogSection> sections;
  for (auto& [store_id, w] : _writes) {
    if (!w.prepared) {
      continue;
    }
    w.commit = TableCommit{.piece = std::move(w.piece),
                           .segments = std::move(w.own_segments),
                           .removals = std::move(w.changes.removals),
                           .patches = std::move(w.changes.patches),
                           .manifest = std::move(w.manifest)};
    if (Logged(w)) {
      sections.push_back({.owner = w.owner,
                          .delta = w.store->GetTable().CommitDelta(
                            w.commit, std::move(w.rows))});
    }
  }
  return sections;
}

bool Writes::CrossTable() const {
  if (!_reference_checks.empty()) {
    return true;
  }
  return absl::c_any_of(_writes, [](const auto& entry) {
    const auto& w = entry.second;
    return w.prepared &&
           (absl::c_any_of(w.keys,
                           [](const KeySpec& spec) {
                             return spec.kind == KeyKind::Reference;
                           }) ||
            w.store->HasReferencing());
  });
}

std::vector<absl::Mutex*> Writes::CommitMutexes(
  std::span<const std::shared_ptr<Store>> layouts) const {
  std::vector<absl::Mutex*> mutexes;
  for (const auto& [store_id, w] : _writes) {
    if (w.prepared) {
      mutexes.push_back(&w.store->GetTable().CommitMutex());
    }
  }
  for (const auto& store : layouts) {
    if (store->HasTable()) {
      mutexes.push_back(&store->GetTable().CommitMutex());
    }
  }
  absl::c_sort(mutexes);
  mutexes.erase(std::ranges::unique(mutexes).begin(), mutexes.end());
  return mutexes;
}

void Writes::Apply(Tick tick,
                   std::span<const std::shared_ptr<Store>> layouts) noexcept {
  for (const auto& store : layouts) {
    store->CommitLayout(_session);
  }
  for (auto& [store_id, w] : _writes) {
    if (!w.owner) {
      ReleaseWriter(w);
      continue;
    }
    if (w.prepared) {
      try {
        auto& table = w.store->GetTable();
        if (!w.commit.segments.empty()) {
          w.store->NudgeCompaction();
        }
        if (table.Apply(tick, std::move(w.commit))) {
          QueueReleaseCleanup(w.store, tick);
        }
      } catch (const std::exception& e) {
        SDB_FATAL(
          SEARCH, "commit: applying to table ", store_id,
          " failed after the log took it: ", duckdb::ErrorData{e}.RawMessage());
      }
      w.transactions.clear();
    }
  }
  for (const auto& store : layouts) {
    try {
      store->PublishLayout(tick);
    } catch (const std::exception& e) {
      SDB_FATAL(
        SEARCH, "commit: publishing the layout of table ", store->GetId(),
        " failed after the log took it: ", duckdb::ErrorData{e}.RawMessage());
    }
  }
}

void Writes::BeforePublish() noexcept {
  if (absl::c_any_of(_writes,
                     [](const auto& entry) { return entry.second.prepared; })) {
    SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_before_irs");
  }
}

void Writes::Finish() noexcept {
  for (auto& [store_id, w] : _writes) {
    if (!w.owner) {
      continue;
    }
    if (w.prepared) {
      SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_before_build_truncate");
      SDB_WAIT_ON_FAILURE("pause_search_commit_after_irs");
      if (!w.table) {
        SDB_IF_FAILURE("Search::CrashAfterCommit") { SDB_IMMEDIATE_ABORT(); }
      }
      w.store->PiecesAdded();
    }
    ReleaseWriter(w);
  }
}

void Writes::Abort() noexcept {
  for (auto& [store_id, w] : _writes) {
    w.piece.reset();
    w.transactions.clear();
    ReleaseWriter(w);
  }
  _listed.store(nullptr, std::memory_order_relaxed);
  _writes.clear();
  _reference_checks.clear();
  if (const auto conflict = std::exchange(_conflict, 0)) {
    catalog::Transactions::Get().AwaitPublished(conflict);
  }
}

}  // namespace sdb::search
