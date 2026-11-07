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

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/sql_identifier.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "connector/constraint_keys.h"
#include "connector/full_scanner.h"
#include "connector/search_sink_writer.hpp"
#include "search/log_entry.h"

namespace sdb::search {
namespace {

void ReleaseWriter(StoreWrites& w) noexcept {
  w.truncate_claim.reset();
  w.registration.reset();
}

[[noreturn]] void ThrowDuplicate(const KeySpec& spec, std::string_view key) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
    ERR_MSG("duplicate key value violates unique constraint \"",
            spec.constraint, "\""),
    ERR_DETAIL("Key ",
               connector::ConstraintKeyText(key, spec.names, spec.types),
               " already exists."));
}

[[noreturn]] void ThrowMissingReference(const catalog::SearchTableEntry& table,
                                        const KeySpec& spec,
                                        std::string_view key) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FOREIGN_KEY_VIOLATION),
    ERR_MSG("insert or update on table \"", table.name.GetIdentifierName(),
            "\" violates foreign key constraint \"", spec.constraint, "\""),
    ERR_DETAIL("Key ",
               connector::ConstraintKeyText(key, spec.names, spec.types),
               " is not present in table \"",
               spec.referenced.GetIdentifierName(), "\"."));
}

[[noreturn]] void ThrowStillReferenced(const catalog::SearchTableEntry& table,
                                       const KeySpec& unique,
                                       const ReferencingKey& referencing,
                                       std::string_view key) {
  const auto& child = referencing.table->name.GetIdentifierName();
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FOREIGN_KEY_VIOLATION),
    ERR_MSG("update or delete on table \"", table.name.GetIdentifierName(),
            "\" violates foreign key constraint \"",
            referencing.spec->constraint, "\" on table \"", child, "\""),
    ERR_DETAIL("Key ",
               connector::ConstraintKeyText(key, unique.names, unique.types),
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
void ForEachRowKey(duckdb::ClientContext& context, const KeySpec& spec,
                   const irs::SubReader& segment, const irs::DocumentMask& docs,
                   absl::FunctionRef<void(std::string_view key)> visit) {
  const auto* columns = segment.GetColReader();
  if (!columns || docs.Empty()) {
    return;
  }
  std::vector<irs::ColumnstoreProjection> projections;
  for (size_t i = 0; i != spec.key.columns.size(); ++i) {
    projections.push_back({.output_slot = i, .column_id = spec.key.columns[i]});
  }
  irs::ColFilterStateCache filter_states;
  connector::FullScanner scanner{
    *columns, projections, {}, &context, filter_states};
  duckdb::DataChunk chunk;
  chunk.Initialize(
    duckdb::Allocator::Get(context),
    duckdb::vector<duckdb::LogicalType>{spec.types.begin(), spec.types.end()});
  const catalog::ConstraintKey key{.field = spec.key.field,
                                   .columns = spec.key.columns};
  duckdb::Vector keys{duckdb::LogicalType::BLOB, STANDARD_VECTOR_SIZE};
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
    connector::EncodeConstraintKeys(chunk, spec.key.columns, key, keys);
    auto values = keys.Values<duckdb::string_t>();
    for (duckdb::idx_t row = 0; row != scanned; ++row) {
      if (const auto entry = values[row]; entry.IsValid()) {
        const auto& value = entry.GetValue();
        visit({value.GetData(), value.GetSize()});
      }
    }
    count = 0;
  };
  docs.ForEachRun(nullptr, [&](irs::doc_id_t first, irs::doc_id_t last) {
    for (auto doc = first; doc < last; ++doc) {
      const uint64_t row = doc - irs::doc_limits::min();
      const auto start = row - row % STANDARD_VECTOR_SIZE;
      if (count != 0 && start != window) {
        flush();
      }
      window = start;
      rows.set_index(count++, row - start);
    }
    return true;
  });
  flush();
}

// Feeds `visit` the key under `key` of every row of `chunk` holding one, with
// the document each row is: `first` for the first row.
void ForEachChunkKey(
  duckdb::DataChunk& chunk, std::span<const connector::ColumnId> column_ids,
  const catalog::ConstraintKey& key, irs::doc_id_t first,
  absl::FunctionRef<void(std::string_view key, irs::doc_id_t doc)> visit) {
  const auto count = chunk.size();
  duckdb::Vector keys{duckdb::LogicalType::BLOB, count};
  connector::EncodeConstraintKeys(chunk, column_ids, key, keys);
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

// Every key the rows of `w` hold more than once, or that a live row of
// `existing` holds, for unique key `spec`: the rows are `segments`, read
// through their term dictionaries, and `keys`, sorted.
std::optional<std::string> FindDuplicate(
  const KeySpec& spec, std::span<const KeyProbe::Segment> segments,
  std::span<const std::string> keys, KeyProbe& existing) {
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
  ForEachSegmentKey(segments, spec.key.field,
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
                    });
  while (!found && next != keys.size()) {
    const auto run = take_run();
    if (settle(keys[next], run)) {
      next += run;
    }
  }
  return found;
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
           irs::DocumentMaskBuilder::kCanonical),
         .maps = group.maps});
    }
    removal = std::move(rest);
  }
  std::erase_if(removals,
                [](const auto& removal) { return removal->groups.empty(); });
}

irs::DocumentMaskBuilder TakeRowRemovals(
  std::vector<std::shared_ptr<const irs::DocRemoval>>& removals,
  uint64_t rows_id) {
  irs::DocumentMaskBuilder taken;
  TakeRemovals(
    removals, [&](uint64_t segment) { return segment == rows_id; },
    [&](const irs::DocumentMask& docs) { taken.Merge(docs); });
  return taken;
}

void ForEachLiveChunk(duckdb::ColumnDataCollection& rows,
                      const irs::DocumentMaskBuilder& removed,
                      const auto& visit) {
  duckdb::SelectionVector live{STANDARD_VECTOR_SIZE};
  auto doc = irs::doc_limits::min();
  for (auto& chunk : rows.Chunks()) {
    const auto count = chunk.size();
    const auto first = doc;
    doc += static_cast<irs::doc_id_t>(count);
    if (!removed.Empty()) {
      duckdb::idx_t kept = 0;
      for (duckdb::idx_t i = 0; i < count; ++i) {
        if (!removed.Contains(static_cast<irs::doc_id_t>(first + i))) {
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

std::string_view ClaimActivity(Store::Exclusive kind) {
  return kind == Store::Exclusive::Drop ? "is dropping" : "is truncating";
}

OwnWrites OwnOf(const StoreWrites& w) {
  return {.rows = w.changes.collection.get(),
          .rows_id = OwnRowsId(w.rows_generation),
          .row_columns = w.changes.column_ids,
          .segments = w.own_segments,
          .removals = w.changes.removals,
          .truncated = w.changes.truncate};
}

void FlushRows(duckdb::ClientContext& context, StoreWrites& w) {
  auto& store = *w.store;
  auto& changes = w.changes;
  SDB_ASSERT(changes.HasBufferedRows());
  const auto rows_id = OwnRowsId(w.rows_generation);
  auto removed = TakeRowRemovals(changes.removals, rows_id);
  auto trx = std::make_unique<irs::IndexWriter::Transaction>(
    store.GetTransaction(/*exclusive_segment=*/true));
  {
    const auto sink = connector::MakeSearchTableInsertSink(
      *trx, store, *changes.catalog, context);
    ForEachLiveChunk(
      *changes.collection, removed, [&](duckdb::DataChunk& chunk) {
        connector::WriteChunkToSearchSink(*sink, chunk, changes.column_ids,
                                          store.GetId(), context);
      });
  }
  auto segments = store.OpenFlushed(trx->FlushAndFsync());
  std::shared_ptr<const irs::DocumentMask> mask;
  if (!removed.Empty()) {
    mask =
      std::make_shared<const irs::DocumentMask>(std::move(removed).Finish());
  }
  irs::doc_id_t rank = 0;
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

StoreWrites& Writes::EnsureStoreWrites(const std::shared_ptr<Store>& store) {
  absl::MutexLock lock{&_mutex};
  auto& w = _writes[store->GetId()];
  if (!w.store) {
    w.store = store;
  }
  return w;
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
}

void Writes::AdoptTable(const std::shared_ptr<Store>& store,
                        const catalog::SearchTableEntry& table) {
  StoreWrites* w = nullptr;
  {
    absl::ReaderMutexLock lock{&_mutex};
    if (const auto it = _writes.find(store->GetId()); it != _writes.end()) {
      w = &it->second;
    }
  }
  if (w) {
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
    refuse(ClaimActivity(Store::Exclusive::Drop));
  }
  if (!w.registration) {
    w.registration = store->RegisterWriter();
    if (!w.registration) {
      refuse(ClaimActivity(store->ExclusiveClaim()));
    }
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
}

void Writes::AddInlineRows(duckdb::ClientContext& context,
                           const std::shared_ptr<Store>& store,
                           std::span<const connector::ColumnId> column_ids,
                           duckdb::Catalog& catalog,
                           std::unique_ptr<duckdb::ColumnDataCollection> rows,
                           uint64_t flush_above) {
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  w.changes.AppendInsertRows(std::move(rows), column_ids, catalog);
  if (w.changes.BufferedBytes() > flush_above) {
    FlushRows(context, w);
  }
}

void Writes::RewriteOwn(
  duckdb::ClientContext& context, const std::shared_ptr<Store>& store,
  duckdb::Catalog& catalog,
  absl::FunctionRef<void(const irs::SubReader&,
                         connector::SearchSinkInsertBaseImpl&)>
    feed) {
  StoreWrites* found = nullptr;
  {
    absl::ReaderMutexLock lock{&_mutex};
    if (const auto it = _writes.find(store->GetId()); it != _writes.end()) {
      found = &it->second;
    }
  }
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
    const auto reader = store->View(kAllApplied, OwnOf(w));
    auto trx = std::make_unique<irs::IndexWriter::Transaction>(
      store->GetTransaction(/*exclusive_segment=*/true));
    {
      const auto sink =
        connector::MakeSearchTableInsertSink(*trx, *store, catalog, context);
      for (const auto& sub : reader) {
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
  }
  ResetReader(store->GetDatabaseId(), store->GetId());
}

void Writes::Rebuilt(const std::shared_ptr<Store>& store, Tick tick,
                     bool newer) {
  {
    auto& w = EnsureStoreWrites(store);
    absl::MutexLock lock{&w.mutex};
    w.rebuilt = std::max(w.rebuilt, tick);
    w.rebuilt_newer = w.rebuilt_newer || newer;
  }
  ResetReader(store->GetDatabaseId(), store->GetId());
}

std::shared_ptr<irs::DirectoryReader> Writes::PinnedReader(
  const Store& store) const {
  absl::ReaderMutexLock lock{&_mutex};
  const auto pinned =
    _readers.find(std::pair{store.GetDatabaseId(), store.GetId()});
  SDB_ENSURE(pinned != _readers.end(), "search table ", store.GetId(),
             ": deletes without the reader their positions come from");
  return pinned->second.reader;
}

void Writes::AddRemoval(duckdb::ClientContext& context,
                        const std::shared_ptr<Store>& store,
                        irs::DocRemovalBuilder&& positions) {
  const auto reader = PinnedReader(*store);
  auto removal = std::move(positions).Finish(*reader);
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
    for (const auto& segment : *reader) {
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
            context, spec, *it->second, group.docs,
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
    w.changes.removals.push_back(std::move(removal));
  }
}

void Writes::AddSearchTruncate(const std::shared_ptr<Store>& store,
                               const catalog::SearchTableEntry& table,
                               Tick snapshot) {
  const auto conflict = [&](bool writing) {
    if (writing) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
        ERR_MSG("Attempting to truncate table ", table.name.GetIdentifierName(),
                " but another transaction is writing to this table"));
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("Attempting to truncate table ",
              duckdb::SQLQuotedIdentifier::ToString(table.name),
              " but another transaction has added rows to this table"));
  };
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  if (!w.truncate_claim) {
    w.truncate_claim = store->ClaimTruncate();
    if (!w.truncate_claim) {
      conflict(true);
    }
    if (store->GetTable().LastAppend() > snapshot) {
      w.truncate_claim.reset();
      conflict(false);
    }
  }
  w.transactions.clear();
  w.own_segments.clear();
  w.own_maps.clear();
  w.checked_segments = 0;
  ++w.rows_generation;
  w.changes.AppendTruncate(false);
}

void Writes::ClaimExclusive(const std::shared_ptr<Store>& store,
                            Store::Exclusive kind) {
  SDB_ASSERT(kind == Store::Exclusive::Drop);
  auto& w = EnsureStoreWrites(store);
  absl::MutexLock lock{&w.mutex};
  w.dropping = true;
}

namespace {

KeyProbe::Segment OwnSegmentSource(const OwnSegment& segment,
                                   const LiveUnits& live) {
  const auto id = segment.reader->Id();
  return {.unit = id,
          .sub = segment.reader.get(),
          .masks = {.removed = live.Removed(id)}};
}

// The rows of `w` that are not committed yet: its own segments, and the rows
// it buffers or, once its commit is prepared, wrote into its hot unit.
void AddOwnRows(KeyProbe& probe, const StoreWrites& w, const LiveUnits& live) {
  for (const auto& segment : w.own_segments) {
    probe.Add(OwnSegmentSource(segment, live));
  }
  if (w.prepared) {
    if (w.hot_end != w.hot_first) {
      probe.Add(KeyProbe::Hot{.unit = w.hot_id,
                              .keys = &w.buffered_keys,
                              .first = w.hot_first,
                              .docs = w.hot_end});
    }
    return;
  }
  if (w.keyed_rows != 0 && w.keyed_generation == w.rows_generation) {
    const auto rows_id = OwnRowsId(w.rows_generation);
    probe.Add(KeyProbe::Hot{.unit = rows_id,
                            .keys = &w.buffered_keys,
                            .docs = static_cast<irs::doc_id_t>(w.keyed_rows),
                            .masks = {.removed = live.Removed(rows_id)}});
  }
}

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
    if (!_own || !_own->changes.truncate) {
      probe.AddLive(_live);
    }
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
  segments.reserve(w.own_segments.size());
  for (const auto& segment : w.own_segments) {
    segments.push_back(OwnSegmentSource(segment, live));
  }
  ForEachSegmentKey(segments, field, [&](std::string_view key, size_t) {
    keys.emplace_back(key);
    return true;
  });
  for (const auto& row : w.hot_keys) {
    if (row.field == field) {
      keys.push_back(row.key);
    }
  }
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

void Writes::CheckReferences(StoreWrites& w, const KeySpec& spec,
                             std::span<const std::string> keys) {
  const auto* parent = ReferencedTable(*w.table, spec, Latest(*w.table));
  if (!parent || spec.referenced_field == 0) {
    ThrowMissingReference(*w.table, spec, keys.front());
  }
  const auto& store = *parent->Storage();
  auto* related = Related(store, w);
  std::optional<absl::ReaderMutexLock> lock;
  if (related && related != &w) {
    lock.emplace(&related->mutex);
  }
  const TableRows rows{store, related};
  auto probe = rows.Probe(spec.referenced_field);
  for (const auto& key : keys) {
    if (!probe.Exists(key)) {
      ThrowMissingReference(*w.table, spec, key);
    }
  }
  w.related_checked.try_emplace(store.GetId(), rows.Live().State());
}

void Writes::CheckRemovedKeys(StoreWrites& w, bool commit) {
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
      auto* related = Related(store, w);
      std::optional<absl::ReaderMutexLock> lock;
      if (related && related != &w) {
        lock.emplace(&related->mutex);
      }
      const TableRows rows{store, related};
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
          ThrowStillReferenced(*w.table, *unique, child, key);
        }
      }
      w.related_checked.try_emplace(store.GetId(), rows.Live().State());
    }
    removed.checked = removed.keys.size();
  }
}

void Writes::CheckKeys(duckdb::ClientContext& context, const Store& store) {
  absl::MutexLock all{&_mutex};
  const auto found = _writes.find(store.GetId());
  if (found == _writes.end()) {
    return;
  }
  auto& w = found->second;
  absl::MutexLock lock{&w.mutex};
  if (!w.table || (w.keys.empty() && w.removed_keys.empty())) {
    return;
  }
  if (w.keyed_generation != w.rows_generation) {
    w.buffered_keys.Clear();
    w.keyed_rows = 0;
    w.keyed_generation = w.rows_generation;
  }
  std::vector<std::vector<std::string>> fresh(w.keys.size());
  std::vector<StoreWrites::HotKey> keyed;
  duckdb::idx_t rows = 0;
  if (w.changes.HasBufferedRows()) {
    for (auto& chunk : w.changes.collection->Chunks()) {
      const auto first = rows;
      rows += chunk.size();
      if (rows <= w.keyed_rows) {
        continue;
      }
      for (size_t spec = 0; spec != w.keys.size(); ++spec) {
        ForEachChunkKey(
          chunk, w.changes.column_ids, w.keys[spec].key,
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
    if (key_spec.kind != KeyKind::Unique) {
      continue;
    }
    KeyProbe existing{key_spec.key.field};
    if (!w.changes.truncate) {
      existing.AddLive(live);
    }
    for (size_t i = 0; i < w.checked_segments; ++i) {
      existing.Add(OwnSegmentSource(w.own_segments[i], live));
    }
    if (w.keyed_rows != 0) {
      existing.Add(
        KeyProbe::Hot{.unit = rows_id,
                      .keys = &w.buffered_keys,
                      .docs = static_cast<irs::doc_id_t>(w.keyed_rows),
                      .masks = {.removed = live.Removed(rows_id)}});
    }
    auto& keys = fresh[spec];
    absl::c_sort(keys);
    if (auto duplicate = FindDuplicate(key_spec, segments, keys, existing)) {
      ThrowDuplicate(key_spec, *duplicate);
    }
  }
  for (const auto& row : keyed) {
    w.buffered_keys.Add(row.field, row.key, row.doc);
  }
  w.keyed_rows = rows;
  w.checked_segments = w.own_segments.size();
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
    CheckReferences(w, key_spec, keys);
  }
  CheckRemovedKeys(w, false);
  w.checked.emplace(std::move(live));
}

std::shared_ptr<const TableState> Writes::VerifyAllReferences(
  const catalog::SearchTableEntry& table, const KeySpec& spec) {
  const auto* parent = ReferencedTable(table, spec, Latest(table));
  const auto own = _writes.find(table.Storage()->GetId());
  const auto reader =
    own == _writes.end()
      ? table.Storage()->View(kAllApplied, {})
      : table.Storage()->View(kAllApplied, OwnOf(own->second));
  std::vector<KeyProbe::Segment> segments;
  for (const auto& segment : reader) {
    segments.push_back({.sub = &segment});
  }
  if (!parent || spec.referenced_field == 0) {
    ForEachSegmentKey(segments, spec.key.field,
                      [&](std::string_view key, size_t) -> bool {
                        ThrowMissingReference(table, spec, key);
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
                        ThrowMissingReference(table, spec, key);
                      }
                      return true;
                    });
  return rows.Live().State();
}

void Writes::VerifyReferences(const catalog::SearchTableEntry& table,
                              const KeySpec& spec) {
  absl::MutexLock all{&_mutex};
  auto referenced = VerifyAllReferences(table, spec);
  _reference_checks.push_back(
    {.table = &table, .spec = spec, .referenced = std::move(referenced)});
}

void Writes::CatchUpKeys(duckdb::ClientContext& context, StoreWrites& w) {
  if (w.keys.empty() && !w.removed_rows && !w.changes.truncate) {
    return;
  }
  LiveUnits now{w.store->GetTable().State(), w.changes.removals};
  if (HasUniqueKeys(w) && !w.changes.truncate &&
      (!w.own_segments.empty() || !w.hot_keys.empty())) {
    std::vector<KeyProbe::Segment> segments;
    segments.reserve(w.own_segments.size());
    for (const auto& segment : w.own_segments) {
      segments.push_back(OwnSegmentSource(segment, now));
    }
    std::vector<std::string> keys;
    for (const auto& spec : w.keys) {
      if (spec.kind != KeyKind::Unique) {
        continue;
      }
      KeyProbe fresh{spec.key.field};
      fresh.AddLive(now, w.checked ? &*w.checked : nullptr);
      if (fresh.Empty()) {
        continue;
      }
      keys.clear();
      for (const auto& row : w.hot_keys) {
        if (row.field == spec.key.field) {
          keys.push_back(row.key);
        }
      }
      absl::c_sort(keys);
      if (auto duplicate = FindDuplicate(spec, segments, keys, fresh)) {
        ThrowDuplicate(spec, *duplicate);
      }
    }
  }
  const auto latest = Latest(*w.table);
  for (const auto& spec : w.keys) {
    if (spec.kind != KeyKind::Reference) {
      continue;
    }
    const auto keys = OwnKeys(w, spec.key.field, now);
    if (keys.empty()) {
      continue;
    }
    if (const auto* parent = ReferencedTable(*w.table, spec, latest)) {
      const auto& store = *parent->Storage();
      const auto it = w.related_checked.find(store.GetId());
      if (it != w.related_checked.end() &&
          it->second == store.GetTable().State()) {
        continue;
      }
      w.related_checked.erase(store.GetId());
    }
    CheckReferences(w, spec, keys);
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
  } else if (w.removed_rows) {
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
                               const irs::DocumentMaskBuilder* removed) {
        if (removed) {
          ForEachRowKey(
            context, *spec, reader, irs::DocumentMaskBuilder{*removed}.Finish(),
            [&](std::string_view key) { it->keys.emplace_back(key); });
        }
      };
      for (const auto& unit : now.Units()) {
        collect(*w.store->GetTable().UnitReader(*unit.unit), unit.removed);
      }
      for (const auto& segment : w.own_segments) {
        collect(*segment.reader, now.Removed(segment.reader->Id()));
      }
      for (auto& child : w.related_checked) {
        child.second.reset();
      }
    }
    CheckRemovedKeys(w, true);
  }
  w.checked.emplace(std::move(now));
}

irs::DirectoryReader Writes::View(duckdb::ClientContext& context, Store& store,
                                  Tick snapshot) {
  StoreWrites* w = nullptr;
  {
    absl::ReaderMutexLock lock{&_mutex};
    if (const auto it = _writes.find(store.GetId()); it != _writes.end()) {
      w = &it->second;
    }
  }
  const bool indexed = store.HasIndexes();
  if (indexed) {
    store.CoolAll();
  }
  if (!w) {
    return store.View(snapshot, {});
  }
  absl::MutexLock lock{&w->mutex};
  if (w->rebuilt_newer) {
    ThrowConcurrentUpdate();
  }
  if (indexed && w->changes.HasBufferedRows()) {
    FlushRows(context, *w);
  }
  return store.View(std::max(snapshot, w->rebuilt), OwnOf(*w));
}

void Writes::NoteConflicts(Tick snapshot) {
  absl::MutexLock all{&_mutex};
  for (const auto& [store_id, w] : _writes) {
    absl::ReaderMutexLock lock{&w.mutex};
    for (const auto& removal : w.changes.removals) {
      _conflict =
        _conflict || w.store->GetTable().Conflicts(snapshot, *removal);
    }
  }
}

void Writes::PrepareCommit(duckdb::ClientContext& context, Tick snapshot) {
  if (_conflict) {
    ThrowConcurrentUpdate();
  }
  for (const auto& [store_id, w] : _writes) {
    for (const auto& removal : w.changes.removals) {
      w.store->GetTable().CheckConflicts(snapshot, *removal);
    }
  }
  for (auto& [store_id, w] : _writes) {
    auto& changes = w.changes;
    if (!w.table || (changes.Empty() && w.own_segments.empty())) {
      continue;
    }
    if (changes.HasBufferedRows() || !changes.removals.empty()) {
      SDB_IF_FAILURE("search_feed_pending_fails") {
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
    }
    if (changes.truncate) {
      w.log.push_back(LogTruncate());
    }
    if (!w.own_segments.empty()) {
      w.log.push_back(LogAddSegments(w.own_segments));
    }
    if (changes.HasBufferedRows()) {
      auto removed =
        TakeRowRemovals(changes.removals, OwnRowsId(w.rows_generation));
      w.hot = w.store->GetTable().AcquireHot(w.store->Writer());
      absl::MutexLock lock{&w.hot->Mutex()};
      auto& trx = w.hot->Trx();
      w.hot_first = HotDocs(trx);
      {
        const auto sink = connector::MakeSearchTableInsertSink(
          trx, *w.store, *changes.catalog, context);
        ForEachLiveChunk(
          *changes.collection, removed, [&](duckdb::DataChunk& chunk) {
            const auto first = HotDocs(trx);
            for (const auto& key : sink->ConstraintKeys()) {
              ForEachChunkKey(
                chunk, changes.column_ids, key,
                static_cast<irs::doc_id_t>(irs::doc_limits::min() + first),
                [&](std::string_view value, irs::doc_id_t doc) {
                  w.hot_keys.push_back({.field = key.field,
                                        .key = std::string{value},
                                        .doc = doc});
                });
            }
            connector::WriteChunkToSearchSink(*sink, chunk, changes.column_ids,
                                              w.store->GetId(), context);
            w.log.push_back(LogHotRows(HotSegmentId(trx), first, chunk));
          });
      }
      w.hot_end = HotDocs(trx);
      if (w.hot_end != w.hot_first) {
        w.hot_id = HotSegmentId(trx);
      }
      changes.ClearBufferedRows();
    }
    w.buffered_keys.Clear();
    for (const auto& row : w.hot_keys) {
      w.buffered_keys.Add(row.field, row.key, row.doc);
    }
    w.prepared = true;
  }
  for (auto& [store_id, w] : _writes) {
    if (w.prepared) {
      CatchUpKeys(context, w);
      SDB_PARK_ONCE_ON_FAILURE("search_commit_prepared");
    }
  }
}

std::vector<LogSection> Writes::TakeLog(duckdb::ClientContext& context,
                                        Tick snapshot) {
  if (_conflict) {
    ThrowConcurrentUpdate();
  }
  std::vector<LogSection> sections;
  for (auto& [store_id, w] : _writes) {
    if (!w.prepared) {
      continue;
    }
    if (!w.dropping && (w.store->Dropped() ||
                        w.store->ExclusiveClaim() == Store::Exclusive::Drop)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
        ERR_MSG("Attempting to write to table ",
                w.table->name.GetIdentifierName(), " but another transaction ",
                w.store->Dropped() ? "dropped" : "is dropping", " it"));
    }
    auto& table = w.store->GetTable();
    for (auto& removal : w.changes.removals) {
      table.CheckConflicts(snapshot, *removal);
      removal = table.Resolve(removal);
      if (!removal->groups.empty()) {
        w.log.push_back(LogRemoval(*removal));
      }
    }
  }
  for (auto& [store_id, w] : _writes) {
    if (w.prepared) {
      CatchUpKeys(context, w);
      sections.push_back({.table = w.table, .entries = std::move(w.log)});
    }
  }
  const auto latest = [&](const ReferenceCheck& check) {
    const auto* parent =
      ReferencedTable(*check.table, check.spec, Latest(*check.table));
    return parent ? parent->Storage()->GetTable().State() : nullptr;
  };
  for (const auto& check : _reference_checks) {
    if (latest(check) != check.referenced) {
      VerifyAllReferences(*check.table, check.spec);
    }
  }
  return sections;
}

void Writes::Apply(Tick tick, Tick oldest, Lsn lsn) noexcept {
  for (auto& [store_id, w] : _writes) {
    if (!w.table) {
      ReleaseWriter(w);
      continue;
    }
    if (w.prepared) {
      try {
        auto& table = w.store->GetTable();
        const bool appended = w.hot && w.hot_end != w.hot_first;
        if (!w.own_segments.empty()) {
          w.store->NudgeCompaction();
        }
        if (appended) {
          for (const auto& row : w.hot_keys) {
            w.hot->Keys().Add(row.field, row.key, row.doc);
          }
        }
        table.Apply(tick, oldest,
                    TableCommit{.hot = appended ? w.hot : nullptr,
                                .hot_id = w.hot_id,
                                .hot_end = w.hot_end,
                                .segments = std::move(w.own_segments),
                                .removals = std::move(w.changes.removals),
                                .truncate = w.changes.truncate,
                                .lsn = lsn});
        if (w.hot) {
          w.store->ReleaseHot(w.hot);
          w.hot.reset();
        }
      } catch (const std::exception& e) {
        SDB_FATAL(
          SEARCH, "search-table commit: applying to table ", store_id,
          " failed after the log took it: ", duckdb::ErrorData{e}.RawMessage());
      }
      w.transactions.clear();
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
    if (!w.table) {
      continue;
    }
    if (w.prepared) {
      SDB_PARK_ONCE_ON_FAILURE("pause_search_commit_before_build_truncate");
      SDB_WAIT_ON_FAILURE("pause_search_commit_after_irs");
    }
    ReleaseWriter(w);
  }
}

void Writes::Abort() noexcept {
  for (auto& [store_id, w] : _writes) {
    if (w.hot) {
      bool inverted = false;
      {
        absl::MutexLock lock{&w.hot->Mutex()};
        inverted = w.hot->Docs() != w.hot_first;
        if (inverted) {
          w.hot->Trx().DiscardDocsFrom(
            static_cast<irs::doc_id_t>(irs::doc_limits::min() + w.hot_first));
        }
      }
      w.store->GetTable().ReleaseHot(w.hot, inverted);
      w.hot.reset();
    }
    w.transactions.clear();
    ReleaseWriter(w);
  }
  _writes.clear();
  _readers.clear();
  _reference_checks.clear();
  _conflict = false;
}

}  // namespace sdb::search
