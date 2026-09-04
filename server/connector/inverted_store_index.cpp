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

#include "connector/inverted_store_index.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/container/inlined_vector.h>

#include <atomic>
#include <deque>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/execution/index/unbound_index.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/append_state.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <duckdb/storage/table/row_group_collection.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iterator>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "basics/assert.h"
#include "basics/log.h"
#include "basics/primary_key.hpp"
#include "catalog1/entry/inverted_index.h"
#include "catalog1/entry/tokenizer.h"
#include "catalog1/lookup.h"
#include "catalog1/scorer_options.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/index_opclass.h"
#include "connector/search_sink_writer.hpp"
#include "connector/term_dict.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "query/config_variable_names.h"
#include "search/inverted_index_storage.h"
#include "search/tick_domain.h"

namespace sdb::connector {
namespace {

// The index entry one id names in the database holding it, or null when no
// entry there carries it -- an online CREATE INDEX feeds a concurrent writer
// before its own transaction has committed, so a miss is ordinary.
duckdb::optional_ptr<const duckdb::IndexCatalogEntry> FindIndexEntry(
  duckdb::ClientContext* context, duckdb::AttachedDatabase& db,
  duckdb::idx_t id) {
  const auto found =
    catalog::FindIn<duckdb::DuckIndexEntry>(context, db.GetCatalog(), id);
  return found ? &found->Cast<duckdb::IndexCatalogEntry>() : nullptr;
}

duckdb::idx_t IdOption(const duckdb::case_insensitive_map_t<duckdb::Value>& o,
                       const char* key) {
  const auto it = o.find(key);
  if (it == o.end() || it->second.IsNull()) {
    return 0;
  }
  return it->second.GetValue<uint64_t>();
}

// Field ids of the keys that are not bare columns. A bare column indexes under
// its own column id; an expression has none, so it takes a slot from the
// synthetic range, which no relation column can ever collide with. Stride 8
// covers the per-kind JSON leaves and the synthetic geo column allocated with
// it -- allocated together, so either all valid or all invalid.
constexpr irs::field_id kExpressionFieldBase = kFirstSyntheticColumnId + 0x100;
constexpr irs::field_id kExpressionFieldStride = 8;

constexpr irs::field_id ExpressionFieldId(size_t key) noexcept {
  return kExpressionFieldBase + key * kExpressionFieldStride;
}

// A key that is a bare column indexes under that column's own id, which
// duckdb hands us indirectly: a BoundColumnRefExpression's column_index is a
// position in the index's column_ids, not a table column.
duckdb::optional_ptr<const duckdb::BoundColumnRefExpression> AsColumnRef(
  const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundColumnRefExpression>();
}

duckdb::CatalogTransaction ConfigTransaction(duckdb::ClientContext* context,
                                             duckdb::AttachedDatabase& db) {
  return context != nullptr
           ? db.GetCatalog().GetCatalogTransaction(*context)
           : duckdb::CatalogTransaction::GetSystemTransaction(db.GetDatabase());
}

// The text search dictionary an opclass names, or null when the opclass is one
// of the two built-ins or was not given at all. A bare opclass resolves
// against the index's own schema, so a dictionary of either built-in name
// shadows it.
duckdb::optional_ptr<catalog::TokenizerCatalogEntry> ResolveOpclassDict(
  duckdb::ClientContext* context, duckdb::AttachedDatabase& db,
  duckdb::SchemaCatalogEntry& schema, std::string_view opclass) {
  if (opclass.empty()) {
    return nullptr;
  }
  auto found = schema.GetEntry(ConfigTransaction(context, db),
                               duckdb::CatalogType::TOKENIZER_ENTRY,
                               duckdb::Identifier{std::string{opclass}});
  if (!found) {
    return nullptr;
  }
  return &found->Cast<catalog::TokenizerCatalogEntry>();
}

}  // namespace

void WritePkPolicy(PkPolicy policy,
                   duckdb::case_insensitive_map_t<duckdb::Value>& into) {
  into[InvertedStoreIndex::kPkTermOption] =
    duckdb::Value::BOOLEAN(policy.index_term);
  into[InvertedStoreIndex::kPkColumnOption] =
    duckdb::Value::UTINYINT(static_cast<uint8_t>(policy.column));
}

PkPolicy ReadPkPolicy(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  PkPolicy policy;
  if (auto it = options.find(InvertedStoreIndex::kPkTermOption);
      it != options.end()) {
    policy.index_term = it->second.GetValue<bool>();
  }
  if (auto it = options.find(InvertedStoreIndex::kPkColumnOption);
      it != options.end()) {
    policy.column = static_cast<PkColumnKind>(it->second.GetValue<uint8_t>());
  }
  return policy;
}

// The dictionaries the persisted key records name, resolved against the
// caller's transaction. Kept off the entry's config deliberately: these are
// non-owning pointers into MVCC versions, so freezing them there would pin one
// snapshot for every later reader.
catalog::TokenizerMap ResolveKeyTokenizers(
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  duckdb::SchemaCatalogEntry& schema,
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  catalog::TokenizerMap dicts;
  for (const auto& key : catalog::DecodeInvertedKeys(options)) {
    if (key.kind != catalog::OpclassKind::Dictionary ||
        key.dictionary.empty()) {
      continue;
    }
    if (auto dict = ResolveOpclassDict(&context, db, schema, key.dictionary)) {
      dicts.emplace(dict->name, &*dict);
    }
  }
  return dicts;
}

std::optional<catalog::ScorerOptions> ParseTopKScorer(
  duckdb::ClientContext& context,
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  const auto it = options.find("optimize_top_k");
  if (it == options.end()) {
    return std::nullopt;
  }
  return catalog::ParseScorerExpression(
    context, it->second.DefaultCastAs(duckdb::LogicalType::VARCHAR)
               .GetValue<std::string>());
}

catalog::TokenizerMap ResolveKeyTokenizers(
  duckdb::ClientContext& context, const duckdb::IndexCatalogEntry& entry) {
  auto& catalog =
    duckdb::Catalog::GetCatalog(context, entry.ParentCatalog().GetName());
  auto& schema = catalog.GetSchema(context, entry.ParentSchema().name);
  return ResolveKeyTokenizers(context, catalog.GetAttached(), schema,
                              entry.options);
}

void ResolveAndPersistInvertedKeys(
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  duckdb::SchemaCatalogEntry& schema, duckdb::CreateIndexInfo& info,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs,
  duckdb::idx_t table_id, std::span<const ColumnId> column_ids) {
  const size_t keys = info.parsed_expressions.size();
  std::vector<catalog::InvertedIndexKey> records;
  records.reserve(keys);
  // Per field, not per key: a column listed twice -- `inverted(a, a
  // included(...))` -- is two keys contributing to one field's config.
  catalog::InvertedIndexFields entries;
  containers::FlatHashSet<irs::field_id> tokenized_columns;
  containers::FlatHashSet<std::string> tokenized_exprs;

  for (size_t i = 0; i < keys; ++i) {
    catalog::InvertedIndexKey record;
    const auto block = ExpressionFieldId(i);
    record.block = block;
    const auto colref = i < exprs.size() ? AsColumnRef(*exprs[i]) : nullptr;
    duckdb::LogicalType value_type;
    std::string label;
    if (colref) {
      const auto pos = colref->Binding().column_index.GetIndex();
      SDB_ENSURE(pos < info.column_ids.size(),
                 "inverted index key references a column outside its own set");
      record.field_id = static_cast<irs::field_id>(info.column_ids[pos]);
      value_type = exprs[i]->GetReturnType();
      label = colref->GetName().GetIdentifierName();
    } else {
      record.field_id = block;
      value_type = exprs[i]->GetReturnType();
      record.return_type = value_type.ToString();
      record.return_type_id = static_cast<uint8_t>(value_type.id());
      label = info.parsed_expressions[i]->ToString();
      // Frozen here in the same normalized form a query will produce, so the
      // match later needs nothing but the entry.
      auto normalized =
        NormalizeBoundExpression(*exprs[i], table_id, column_ids, context);
      record.serialized = SerializeBoundExpression(*normalized);
    }

    KeyOpclass opclass;
    if (i < info.column_opclasses.size()) {
      opclass.name = info.column_opclasses[i];
    }
    if (i < info.column_opclass_options.size()) {
      opclass.options = &info.column_opclass_options[i];
    }

    ValidateInvertedIndexKey(label, value_type, opclass);

    // A built-in is only a built-in in the parenthesised form, so a bare name
    // is looked up as a dictionary first and may shadow one.
    duckdb::optional_ptr<catalog::TokenizerCatalogEntry> dict;
    if (opclass.IsTokenizer()) {
      dict = ResolveOpclassDict(&context, db, schema, opclass.name);
      if (dict) {
        record.kind = catalog::OpclassKind::Dictionary;
        // The opclass exactly as written, which is what the resolution below
        // looks up with -- the entry's own name has already been through
        // Identifier folding and does not necessarily round-trip.
        record.dictionary = std::string{opclass.name};
        record.dictionary_oid = dict->oid;
        record.features = dict->GetFeatures().GetIndexFeatures();
      }
      // One tokenizer per key. A column keys on its id; an expression has no
      // id of its own -- each gets a fresh block -- so it keys on its text,
      // which is what makes two spellings of the same expression collide.
      if (colref) {
        if (!tokenized_columns.insert(record.field_id).second) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("Column '", label,
                    "' is listed more than once with a tokenizer opclass; the "
                    "catalog stores a single tokenizer per indexed column. "
                    "Stack `included(...)` on the same column instead, or "
                    "remove the duplicate."));
        }
      } else if (!tokenized_exprs.insert(label).second) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("Expression '", label,
                  "' is listed more than once with a tokenizer opclass; the "
                  "catalog stores a single tokenizer per indexed expression. "
                  "Stack `included(...)` on the same expression instead, or "
                  "remove the duplicate."));
      }
    } else if (opclass.IsBuiltin(catalog::kIVFKind)) {
      record.kind = catalog::OpclassKind::Ivf;
    } else {
      record.kind = catalog::OpclassKind::Included;
    }

    // Sub-fields are drawn from this key's own 8-wide block, so two keys
    // merging into one field never collide -- their blocks differ.
    auto next_sub_id = static_cast<irs::field_id>(block + 1);
    const auto [slot, fresh] = entries.try_emplace(record.field_id);
    auto& info_out = slot->second;
    if (fresh) {
      // Every field carries a null leaf; only the typed leaves are
      // conditional on what the analyzer reads.
      info_out.null_field_id = next_sub_id++;
    }
    if (opclass.IsTokenizer()) {
      info_out.indexed_term_dict = true;
    }
    ApplyOpclassToEntry(context, schema.name.GetIdentifierName(), label,
                        value_type, opclass, dict, next_sub_id, info_out);

    records.push_back(std::move(record));
  }

  catalog::EncodeInvertedKeys(records, info.options);
  catalog::EncodeInvertedEntries(entries, info.options);
}

namespace {

// Per-worker evaluation of an index's expressions. The bound expressions and
// their chunk offsets come from duckdb (BoundIndex), so this owns nothing but
// an executor and the result chunk: no binding, no context, no shared state --
// every feed worker constructs one and runs it on its own batch in parallel.
class IndexExpressions {
 public:
  IndexExpressions(duckdb::ClientContext& context,
                   const InvertedStoreIndex& index)
    : _executor{context},
      _config{index.SharedConfig()},
      _has_predicate{index.HasPredicate()} {
    const auto& exprs = index.Expressions();
    if (exprs.empty()) {
      return;
    }
    duckdb::vector<duckdb::LogicalType> types;
    types.reserve(exprs.size());
    for (const auto& expr : exprs) {
      _executor.AddExpression(*expr);
      types.push_back(expr->GetReturnType());
    }
    _results.Initialize(duckdb::Allocator::DefaultAllocator(), types);
  }

  bool Empty() const noexcept { return _results.ColumnCount() == 0; }

  // Evaluates every expression over `chunk` once.
  void Execute(duckdb::DataChunk& chunk) {
    _results.Reset();
    _executor.Execute(chunk, _results);
    for (size_t i = 0; i < _config->keys.size(); ++i) {
      // A whole-value analyzer is handed the object as it stands, so the
      // leaf rule does not apply to its key.
      const auto* entry = _config->FindEntry(_config->keys[i].field_id);
      if (entry && entry->whole_value) {
        continue;
      }
      RejectJsonObjectArrayLeaves(_results.data[i], chunk.size());
    }
  }

  // The value vector feeding `field_ids()[i]`.
  duckdb::Vector& Value(size_t i) noexcept { return _results.data[i]; }
  std::span<const catalog::InvertedIndexKey> Fields() const noexcept {
    return _config->keys;
  }

  // The partial-index predicate's result (null when the index is not partial).
  duckdb::Vector* Predicate() noexcept {
    return _has_predicate ? &_results.data[_results.ColumnCount() - 1]
                          : nullptr;
  }

 private:
  duckdb::ExpressionExecutor _executor;
  duckdb::DataChunk _results;
  std::shared_ptr<const catalog::InvertedIndexFieldOptions> _config;
  bool _has_predicate = false;
};

// Per-worker scratch for the feed body. Held by the pooled Bundle so a bulk
// commit reuses the buffers (and the key strings' capacity) across batches
// instead of reallocating them per batch per index.
struct FeedScratch {
  // Rowids of a scanned range, which run contiguously from the range's first
  // row -- generated in place rather than materialized into a side buffer.
  duckdb::Vector scan_rowids{duckdb::LogicalType::ROW_TYPE};
  std::vector<std::string> keys;
  std::vector<std::string_view> key_views;
  std::string delete_key;
  std::vector<duckdb::Vector> sliced;
  std::vector<ExpressionValue> values;
  // Partial-index selection, reused: the vector keeps its capacity and the
  // chunk its vector array, so a filtered batch costs no allocation.
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  duckdb::DataChunk filtered_chunk;
  duckdb::Vector filtered_rowids{duckdb::LogicalType::ROW_TYPE};
  // A worker's view of its slice of the borrowed chunk, reused across chunks.
  duckdb::DataChunk slice_chunk;
  duckdb::Vector slice_rowids{duckdb::LogicalType::ROW_TYPE};
};

// Rows the partial-index predicate keeps, selected into `scratch.sel`. Returns
// `total` when everything passes (or the index is not partial), so the caller
// feeds the batch untouched.
duckdb::idx_t SelectRows(duckdb::Vector* predicate, duckdb::idx_t total,
                         FeedScratch& scratch) {
  if (!predicate) {
    return total;
  }
  duckdb::UnifiedVectorFormat fmt;
  predicate->ToUnifiedFormat(total, fmt);
  const auto* values = duckdb::UnifiedVectorFormat::GetData<bool>(fmt);
  duckdb::idx_t kept = 0;
  for (duckdb::idx_t i = 0; i < total; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (fmt.validity.RowIsValid(idx) && values[idx]) {
      scratch.sel.set_index(kept++, i);
    }
  }
  return kept;
}

// Feed one batch through the predicate: evaluate the index's expressions,
// drop the rows the predicate rejects, and tokenize the rest. Shared by the
// parallel feed and the inline commit path so the filtering, key building and
// value slicing exist once. Returns the number of rows fed (0 when the
// predicate rejected the whole batch).
duckdb::idx_t FeedFilteredChunk(
  DuckDBSinkIndexWriter& writer, IndexExpressions& exprs,
  duckdb::DataChunk& chunk, duckdb::Vector& rowid_vec, duckdb::idx_t scanned,
  std::span<const FeedColumn> columns, FeedScratch& scratch) {
  if (!exprs.Empty()) {
    exprs.Execute(chunk);
  }
  const auto count = SelectRows(exprs.Predicate(), scanned, scratch);
  if (count == 0) {
    return 0;
  }
  const bool filtered = count != scanned;
  auto* feed_chunk = &chunk;
  auto* feed_rows = &rowid_vec;
  if (filtered) {
    // Sized once: the scratch is pooled and reused across every batch this
    // writer feeds, and InitializeEmpty only accepts an empty chunk.
    if (scratch.filtered_chunk.ColumnCount() == 0) {
      scratch.filtered_chunk.InitializeEmpty(chunk.GetTypes());
    }
    scratch.filtered_chunk.Reference(chunk);
    scratch.filtered_chunk.Slice(scratch.sel, count);
    scratch.filtered_rowids.Slice(rowid_vec, scratch.sel, count);
    feed_chunk = &scratch.filtered_chunk;
    feed_rows = &scratch.filtered_rowids;
  }

  duckdb::UnifiedVectorFormat row_fmt;
  feed_rows->ToUnifiedFormat(count, row_fmt);
  const auto* row_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(row_fmt);
  auto& keys = scratch.keys;
  auto& key_views = scratch.key_views;
  keys.resize(count);
  key_views.resize(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    keys[i].clear();
    primary_key::AppendSigned(keys[i], row_data[row_fmt.sel->get_index(i)]);
    key_views[i] = keys[i];
  }

  // Expression values were computed over the unfiltered batch, so a filtered
  // feed slices them the same way its columns were sliced.
  const auto fields = exprs.Fields();
  auto& sliced = scratch.sliced;
  auto& values = scratch.values;
  sliced.clear();
  values.clear();
  values.reserve(fields.size());
  if (filtered) {
    sliced.reserve(fields.size());
  }
  for (size_t i = 0; i < fields.size(); ++i) {
    if (!fields[i].feeds) {
      continue;
    }
    auto& raw = exprs.Value(i);
    if (!filtered) {
      values.push_back({fields[i].field_id, &raw});
      continue;
    }
    sliced.emplace_back(raw.GetType(), nullptr, 0);
    sliced.back().Slice(raw, scratch.sel, count);
    values.push_back({fields[i].field_id, &sliced.back()});
  }

  FeedChunk(writer, count, PkChunk{.keys = key_views, .column = feed_rows},
            *feed_chunk, columns, values);
  return count;
}

}  // namespace

// Bounds replay's prefetch window: a job counts as done once it retires.
struct InFlight {
  std::atomic<uint64_t> dispatched{0};
  std::atomic<uint64_t> completed{0};

  uint64_t Count() const {
    return dispatched.load(std::memory_order_relaxed) -
           completed.load(std::memory_order_acquire);
  }
};

struct ReplayQueue;

// The per-index worker machinery both feeds share: the task executor, the
// pooled writer/expression kits, and the tokenize bodies. Knows nothing about
// WAL order or commit ticks -- those belong to whoever owns the entries.
struct FeedPool {
  // Per-worker feed kit, pooled because neither the writers nor the executor
  // are per-chunk cheap. The bound expressions are shared and read-only; only
  // the executor and its result chunk are per-worker.
  struct Bundle {
    Bundle(FeedPool& pool, irs::IndexWriter::Transaction& trx)
      : insert_writer{pool.MakeInsertWriter(trx)},
        delete_writer{std::make_unique<DuckDBSearchSinkDeleteWriter>(trx)},
        exprs{
          std::make_unique<IndexExpressions>(*pool.expr_context, pool.owner)} {}

    std::unique_ptr<DuckDBSearchSinkInsertWriter> insert_writer;
    std::unique_ptr<DuckDBSearchSinkDeleteWriter> delete_writer;
    std::unique_ptr<IndexExpressions> exprs;
    FeedScratch scratch;
  };

  std::unique_ptr<DuckDBSearchSinkInsertWriter> MakeInsertWriter(
    irs::IndexWriter::Transaction& trx) {
    return std::make_unique<DuckDBSearchSinkInsertWriter>(
      trx, MakeTokenizerProvider(dicts, *config), owner.IndexedColumns(),
      MakeEntryInfoProvider(*config), config->pk);
  }

  irs::IndexWriter::Transaction NewTransaction() {
    auto trx = storage->GetTransaction();
    trx.SetFieldOptions(config);
    return trx;
  }

  // Flush cadence of every feed through FeedEvaluated (replay and live
  // commits alike): keeps file writes on the workers and the serial flush
  // residue at the final writer commit small, without ending segments (ticks
  // are stamped later, at retirement).
  static constexpr size_t kReplayFlushBytes = size_t{32} << 20;

  std::shared_ptr<search::InvertedIndexStorage> storage;
  // Resolved where the committing transaction was still in scope: a worker has
  // no ClientContext of its own to read a dictionary entry through.
  catalog::TokenizerMap dicts;
  // The configuration this pool's writers encode against, held by shared
  // ownership: it IS the irs::IndexFieldOptions iresearch takes, and an ALTER
  // landing mid-commit swaps the index's copy without disturbing this one.
  std::shared_ptr<const InvertedIndexFieldOptions> config;
  duckdb::DatabaseInstance& instance;
  // duckdb has no context-free ExpressionExecutor, and a feed worker has no
  // connection of its own -- replay runs before any exists. One transaction-
  // less context per pool, shared by every worker's executor: it is read for
  // its allocator and settings only, never to reach the catalog.
  duckdb::shared_ptr<duckdb::ClientContext> expr_context;
  // The index owning this feed; workers build their executors from its
  // duckdb-bound expressions. It outlives the session.
  const InvertedStoreIndex& owner;
  duckdb::TaskExecutor executor;

  std::mutex bundle_mu;
  std::vector<std::unique_ptr<Bundle>> bundles;

  FeedPool(std::shared_ptr<search::InvertedIndexStorage> storage_in,
           catalog::TokenizerMap dicts_in,
           std::shared_ptr<const InvertedIndexFieldOptions> config_in,
           duckdb::AttachedDatabase& attached_in,
           const InvertedStoreIndex& owner_in)
    : storage{std::move(storage_in)},
      dicts{std::move(dicts_in)},
      config{std::move(config_in)},
      instance{attached_in.GetDatabase()},
      expr_context{duckdb::make_shared_ptr<duckdb::ClientContext>(
        instance.shared_from_this())},
      owner{owner_in},
      executor{duckdb::TaskScheduler::GetScheduler(instance)} {}

  // Only replay bounds a prefetch window, and only replay pays for reading the
  // setting -- an index that never replays does not construct the queue at all.
  static size_t ConfiguredReplayDepth(duckdb::DatabaseInstance& db) {
    auto& config = duckdb::DBConfig::GetConfig(db);
    duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
    const auto index = config.TryGetSettingIndex(
      std::string{kRecoveryReplayDepthSetting}, option);
    if (!index.IsValid()) {
      return 0;
    }
    duckdb::Value value;
    if (!config.user_settings.TryGetSetting(index.GetIndex(), value) ||
        value.IsNull()) {
      return 0;
    }
    return value.GetValue<uint32_t>();
  }

  // When the window is full, help run scheduled tasks instead of sleeping.
  void Backpressure(const InFlight& flight, size_t depth) {
    while (flight.Count() >= depth) {
      if (executor.HasError()) {
        return;
      }
      duckdb::shared_ptr<duckdb::Task> help;
      if (executor.GetTask(help)) {
        help->Execute(duckdb::TaskExecutionMode::PROCESS_ALL);
        help.reset();
      } else {
        std::this_thread::yield();
      }
    }
  }

  // The feed body every worker runs. Binds nothing and touches no context.
  void FeedEvaluated(DuckDBSearchSinkInsertWriter& writer,
                     IndexExpressions& exprs, FeedScratch& scratch,
                     irs::IndexWriter::Transaction& trx,
                     duckdb::DataChunk& chunk, duckdb::Vector& rowid_vec,
                     duckdb::idx_t scanned) {
    FeedFilteredChunk(writer, exprs, chunk, rowid_vec, scanned, {}, scratch);
    trx.AdvanceQueries(1);
    if (trx.ActiveMemory() >= kReplayFlushBytes) {
      trx.Flush();
    }
  }

  // A scan chunk whose rowids run contiguously from `first_row`.
  void FeedScan(DuckDBSearchSinkInsertWriter& writer, IndexExpressions& exprs,
                FeedScratch& scratch, irs::IndexWriter::Transaction& trx,
                duckdb::DataChunk& chunk, int64_t first_row) {
    const auto scanned = chunk.size();
    duckdb::VectorOperations::GenerateSequence(scratch.scan_rowids, scanned,
                                               first_row, 1);
    FeedEvaluated(writer, exprs, scratch, trx, chunk, scratch.scan_rowids,
                  scanned);
  }

  // Borrows a kit for the duration of a scope. The pool hands them back on
  // destruction, so a throwing feed cannot leak one.
  class BundleLease {
   public:
    BundleLease(FeedPool& pool, irs::IndexWriter::Transaction& trx)
      : _pool{pool}, _bundle{pool.AcquireBundle(trx)} {}
    ~BundleLease() { _pool.ReleaseBundle(std::move(_bundle)); }
    BundleLease(const BundleLease&) = delete;
    BundleLease& operator=(const BundleLease&) = delete;

    Bundle& operator*() const noexcept { return *_bundle; }
    Bundle* operator->() const noexcept { return _bundle.get(); }

   private:
    FeedPool& _pool;
    std::unique_ptr<Bundle> _bundle;
  };

  std::unique_ptr<Bundle> AcquireBundle(irs::IndexWriter::Transaction& trx) {
    std::unique_ptr<Bundle> bundle;
    {
      std::lock_guard lock{bundle_mu};
      if (!bundles.empty()) {
        bundle = std::move(bundles.back());
        bundles.pop_back();
      }
    }
    if (!bundle) {
      bundle = std::make_unique<Bundle>(*this, trx);
    }
    bundle->insert_writer->SetTransaction(trx);
    bundle->delete_writer->SetTransaction(trx);
    return bundle;
  }

  void ReleaseBundle(std::unique_ptr<Bundle> bundle) {
    std::lock_guard lock{bundle_mu};
    bundles.push_back(std::move(bundle));
  }

  // Takes the rows through an accessor rather than a span: the live path reads
  // them straight out of duckdb's (possibly selection-vectored) rowid vector,
  // so only replay -- which must outlive the chunk -- ever materializes them.
  template<typename RowAt>
  void FeedDeletes(Bundle& bundle, size_t count, RowAt&& row_at) {
    connector::FeedDeletes(*bundle.delete_writer, bundle.scratch.delete_key,
                           count, std::forward<RowAt>(row_at));
  }
};

// What a worker does with an entry, and what it carries. One struct per body
// rather than a class per body: the insert and delete forms differed by a
// single call, and the range form differed only by having no body at all.
struct InsertPayload {
  // Adopts the producer's batch whole -- rows and row ids -- without copying.
  std::shared_ptr<ReplayBatch> batch;
};

struct DeletePayload {
  std::vector<int64_t> rowids;
};

using Payload = std::variant<InsertPayload, DeletePayload>;

struct Entry {
 private:
  Entry(FeedPool& pool_in, ReplayQueue* queue_in, uint64_t wal_offset_in,
        size_t trx_count)
    : pool{pool_in}, queue{queue_in}, wal_offset{wal_offset_in} {
    trxs.reserve(trx_count);
    for (size_t i = 0; i < trx_count; ++i) {
      trxs.emplace_back(pool_in.NewTransaction());
    }
  }

 public:
  // A worker entry: one transaction, tokenized by a RunTask from `payload`.
  Entry(FeedPool& pool_in, ReplayQueue* queue_in, uint64_t wal_offset_in,
        Payload payload_in)
    : Entry{pool_in, queue_in, wal_offset_in, 1} {
    payload = std::move(payload_in);
  }

  // A range entry: one transaction per sub-range, filled by the scan jobs
  // themselves and committed together at this entry's tick. No payload and no
  // queue, because no worker task ever runs it.
  Entry(FeedPool& pool_in, uint64_t wal_offset_in, size_t subranges)
    : Entry{pool_in, nullptr, wal_offset_in, subranges} {}

  Entry(const Entry&) = delete;
  Entry& operator=(const Entry&) = delete;

  // Tokenizes this entry's payload into its transaction, on a worker.
  void Feed(FeedPool::Bundle& bundle) {
    SDB_ASSERT(payload);
    std::visit(
      [&](auto& p) {
        using T = std::decay_t<decltype(p)>;
        if constexpr (std::is_same_v<T, InsertPayload>) {
          pool.FeedEvaluated(*bundle.insert_writer, *bundle.exprs,
                             bundle.scratch, trxs[0], p.batch->data,
                             p.batch->row_ids, p.batch->data.size());
        } else {
          static_assert(std::is_same_v<T, DeletePayload>);
          pool.FeedDeletes(bundle, p.rowids.size(),
                           [&](size_t i) { return p.rowids[i]; });
        }
      },
      *payload);
  }

  FeedPool& pool;
  // Where completion reports. Null for a range entry: the coordinator
  // completes it inline, so nothing notifies.
  ReplayQueue* queue;
  const uint64_t wal_offset;
  // Empty for a range entry: its transactions are filled by the scan jobs.
  std::optional<Payload> payload;
  std::vector<irs::IndexWriter::Transaction> trxs;
  std::atomic<bool> done{false};
};

// The committing thread owns the scan and feeds every native index inline, so a
// chunk is only ours for the duration of the call. Tokenizing it is the slow
// part, so split it across workers and join before returning: the work goes
// parallel without the chunk ever outliving the call that lent it. Slicing
// takes the source by const reference, so every worker may view the same chunk
// at once.
struct SliceTask final : duckdb::BaseExecutorTask {
  SliceTask(duckdb::TaskExecutor& executor_in, FeedPool& pool_in,
            irs::IndexWriter::Transaction& trx_in,
            const duckdb::DataChunk& chunk_in, const duckdb::Vector& row_ids_in,
            duckdb::idx_t begin_in, duckdb::idx_t end_in)
    : BaseExecutorTask{executor_in},
      pool{pool_in},
      trx{trx_in},
      chunk{chunk_in},
      row_ids{row_ids_in},
      begin{begin_in},
      end{end_in} {}

  void ExecuteTask() override {
    const FeedPool::BundleLease bundle{pool, trx};
    auto& scratch = bundle->scratch;
    // Slice() writes into this chunk's existing vectors, so shape it once.
    if (scratch.slice_chunk.ColumnCount() == 0) {
      scratch.slice_chunk.InitializeEmpty(chunk.GetTypes());
    }
    // Per column rather than DataChunk::Slice: when a native index shares the
    // table, duckdb hands us a table-shaped chunk in which only the indexed
    // columns are referenced and the rest are placeholders with no buffer at
    // all. Slicing one of those dereferences nothing.
    for (duckdb::idx_t c = 0; c < chunk.ColumnCount(); ++c) {
      if (chunk.data[c].GetBufferRef()) {
        scratch.slice_chunk.data[c].Slice(chunk.data[c], begin, end);
      }
    }
    scratch.slice_chunk.SetCardinality(end - begin);
    scratch.slice_rowids.Slice(row_ids, begin, end);
    pool.FeedEvaluated(*bundle->insert_writer, *bundle->exprs, scratch, trx,
                       scratch.slice_chunk, scratch.slice_rowids, end - begin);
  }

  std::string TaskType() const override { return "InvertedChunkSlice"; }

  FeedPool& pool;
  irs::IndexWriter::Transaction& trx;
  const duckdb::DataChunk& chunk;
  const duckdb::Vector& row_ids;
  const duckdb::idx_t begin;
  const duckdb::idx_t end;
};

struct RunTask final : duckdb::BaseExecutorTask {
  RunTask(duckdb::TaskExecutor& executor_in, Entry& entry_in)
    : BaseExecutorTask{executor_in}, entry{entry_in} {}

  // Defined below ReplayQueue, whose Retire() it calls.
  void ExecuteTask() override;

  std::string TaskType() const override { return "InvertedReplayChunk"; }

  Entry& entry;
};

struct ReplayQueue {
  explicit ReplayQueue(FeedPool& pool_in) : pool{pool_in} {
    depth = FeedPool::ConfiguredReplayDepth(pool.instance);
    if (depth == 0) {
      const auto threads = static_cast<size_t>(
        duckdb::TaskScheduler::GetScheduler(pool.instance).NumberOfThreads());
      depth = 4 * std::max<size_t>(1, threads);
    }
    depth = std::clamp<size_t>(depth, 1, 1024);
  }
  ReplayQueue(const ReplayQueue&) = delete;
  ReplayQueue& operator=(const ReplayQueue&) = delete;

  FeedPool& pool;
  uint64_t generation = 0;
  uint64_t durable_offset = 0;

  std::mutex retire_mu;
  std::deque<std::unique_ptr<Entry>> window;
  std::deque<std::pair<uint64_t, uint64_t>> pending_cursors;
  uint64_t committed_below = 0;
  InFlight flight;
  size_t depth = 1;

  void Dispatch(std::unique_ptr<Entry> job, uint64_t wal_offset) {
    if (pool.executor.HasError()) {
      return;  // FinishReplay rethrows
    }
    auto* raw = job.get();
    Enqueue(std::move(job), wal_offset);
    pool.executor.ScheduleTask(duckdb::make_uniq<RunTask>(pool.executor, *raw));
    pool.Backpressure(flight, depth);
  }

  // Reserve a ROW_GROUP_DATA range entry in WAL order: a window slot holding
  // one transaction per sub-range, filled by the coordinator's scan jobs (see
  // ReplayExternalRange) and completed once they join. It retires
  // synchronously, so it only transiently occupies the async window bound.
  Entry* CreateRangeEntry(uint64_t wal_offset, size_t subranges) {
    auto job = std::make_unique<Entry>(pool, wal_offset, subranges);
    auto* raw = job.get();
    Enqueue(std::move(job), wal_offset);
    return raw;
  }

  // The scan jobs have joined: mark the entry done and retire in WAL order.
  // Its disjoint sub-range segments all commit at this entry's single tick.
  void CompleteRangeEntry(Entry& job) {
    job.done.store(true, std::memory_order_release);
    Retire();
  }

  void Enqueue(std::unique_ptr<Entry> job, uint64_t wal_offset) {
    {
      std::lock_guard lock{retire_mu};
      // Entries replay in ascending offset order and each transaction commits
      // before the next entry is read, so everything strictly below the entry
      // being dispatched is committed.
      committed_below = std::max(committed_below, wal_offset);
      FlushCursorsLocked();
      window.push_back(std::move(job));
    }
    flight.dispatched.fetch_add(1, std::memory_order_relaxed);
  }

  // Commit finished tasks strictly in dispatch order: ticks allocated here
  // are WAL-ordered, per-segment ticks stay monotone, and the pending state
  // is always a WAL prefix. Retirement is eager (a WAL v2/v3 entry is a
  // checksummed whole-transaction block, so a torn tail throws before any of
  // its chunks dispatch), but cursor points -- and with them the frontier a
  // mid-replay refresh may commit durably -- only advance once the entry's
  // transaction has committed.
  void Retire() {
    std::lock_guard lock{retire_mu};
    while (!window.empty()) {
      auto& head = *window.front();
      if (!head.done.load(std::memory_order_acquire)) {
        break;
      }
      // Reserve the widest transaction's query count, not one tick: a
      // tick-bound Remove bumps _queries per row, and Commit lays a
      // transaction's documents out below the tick it is given. Reserving less
      // would put them on ticks already handed to earlier entries, so a
      // multi-row delete could order at or before the inserts it must mask.
      uint64_t queries = 0;
      for (auto& trx : head.trxs) {
        queries = std::max<uint64_t>(queries, trx.GetQueries());
      }
      const auto tick = search::TickDomain::Instance().Advance(queries + 1);
      // A small entry commits its one transaction; a range entry commits every
      // sub-range's transaction at this same tick (disjoint rowids, so equal
      // ticks are fine -- a later delete still masks them at a higher tick).
      for (auto& trx : head.trxs) {
        SDB_ENSURE(trx.Commit(tick),
                   "inverted index replay: commit failed for index ",
                   pool.owner.IndexId());
      }
      pending_cursors.emplace_back(tick, head.wal_offset);
      window.pop_front();
      flight.completed.fetch_add(1, std::memory_order_release);
    }
    FlushCursorsLocked();
  }

  void FlushCursorsLocked() {
    while (!pending_cursors.empty() &&
           pending_cursors.front().second < committed_below) {
      const auto [tick, offset] = pending_cursors.front();
      pending_cursors.pop_front();
      pool.storage->RecordFlushCursor(
        tick, search::WalCursor{generation, offset + 1});
      pool.storage->SetRecoveryFrontierTick(tick);
    }
  }

  // End of replay: everything below the success offset committed; flush the
  // remaining cursor points so the final refresh commits the whole feed.
  void FinishRetire(uint64_t success_offset) {
    Retire();
    std::lock_guard lock{retire_mu};
    SDB_ASSERT(window.empty());
    committed_below = std::max(committed_below, success_offset + 1);
    FlushCursorsLocked();
    SDB_ASSERT(pending_cursors.empty());
  }
};

void RunTask::ExecuteTask() {
  auto& pool = entry.pool;
  const FeedPool::BundleLease bundle{pool, entry.trxs[0]};
  entry.Feed(*bundle);
  // Deliberately not flushed here. Serializing a segment pays off when the unit
  // is big enough to be worth a parallel write -- a replay sub-range is, a
  // single WAL entry is not: flushing each one writes a segment per entry and
  // measured ~2x slower than letting the refresh write them together.
  entry.done.store(true, std::memory_order_release);
  SDB_ASSERT(entry.queue);
  entry.queue->Retire();
}

// The commit-window feed (persistent, reused after recovery): entries tokenize
// in parallel into their own segments and CommitSearch commits them all at the
// commit's tick. No WAL ordering to keep -- one tick covers the whole set.
// Only touched on the committing thread and its workers; commits are
// serialized DB-wide by the store WAL lock.
struct LiveFeed {
  explicit LiveFeed(FeedPool& pool_in) : pool{pool_in} {}

  FeedPool& pool;
  // The transactions this commit staged. Every one is filled synchronously --
  // slices join before the append returns -- so there is nothing in flight by
  // the time the commit asks for them.
  std::vector<std::unique_ptr<Entry>> jobs;

  // The committing thread's own leg. Two operations cannot go to a worker: a
  // chunk append (duckdb hands over a buffer it recycles, so nothing may
  // outlive the call) and a delete (one filter to build, cheaper than the task
  // that would carry it). Both stage into this one transaction, which commits
  // with the parallel jobs at the same tick -- so there is a single commit
  // protocol, not an inline one and a parallel one.
  std::optional<irs::IndexWriter::Transaction> inline_trx;
  std::unique_ptr<FeedPool::Bundle> inline_bundle;

  FeedPool::Bundle& Inline() {
    if (!inline_trx) {
      inline_trx.emplace(pool.NewTransaction());
      inline_bundle = pool.AcquireBundle(*inline_trx);
    }
    return *inline_bundle;
  }

  // Rows per slice. 64 puts a 2048-row chunk on every core (the count is capped
  // at the thread count); larger slices measurably leave cores idle, and a
  // chunk too small to fill one slice is fed whole on the calling thread
  // instead.
  static constexpr duckdb::idx_t kMinSliceRows = 64;

  // One transaction per slice, for the whole commit window: the entry is
  // created once and every chunk feeds slice k into the same transaction k.
  Entry* slice_entry = nullptr;
  size_t slice_count = 0;

  // Tokenize one borrowed chunk in parallel and join before returning, so the
  // producer may reuse it the moment this call ends. RegisterFlush happens here
  // rather than at Prepare because this runs before the store WAL bytes are
  // written, and a background refresh computing its durable cursor in between
  // must already see these segments.
  void FeedChunkParallel(duckdb::DataChunk& chunk, duckdb::Vector& row_ids) {
    const auto count = chunk.size();
    if (slice_count == 0) {
      const auto threads = static_cast<size_t>(
        duckdb::TaskScheduler::GetScheduler(pool.instance).NumberOfThreads());
      slice_count = std::clamp<size_t>(count / kMinSliceRows, 1,
                                       std::max<size_t>(1, threads));
      slice_entry = CreateRangeEntry(slice_count);
    }
    // The tail chunk can be far smaller than the first: use only as many slices
    // as it can fill, always slice k into transaction k.
    const auto slices =
      std::clamp<size_t>(count / kMinSliceRows, 1, slice_count);
    if (slices == 1) {
      auto& trx = slice_entry->trxs[0];
      const FeedPool::BundleLease bundle{pool, trx};
      pool.FeedEvaluated(*bundle->insert_writer, *bundle->exprs,
                         bundle->scratch, trx, chunk, row_ids, count);
      trx.RegisterFlush();
      return;
    }
    // Its own executor, not the pool's: a TaskExecutor never clears its error
    // state, so a shared one would make a single failed chunk bail out every
    // later commit's slices without tokenizing and rethrow the stale error.
    // Replay keeps the pool's, where poisoning is what should happen.
    duckdb::TaskExecutor executor{
      duckdb::TaskScheduler::GetScheduler(pool.instance)};
    for (size_t k = 0; k < slices; ++k) {
      const auto begin = (k * count) / slices;
      const auto end = ((k + 1) * count) / slices;
      if (end <= begin) {
        continue;
      }
      executor.ScheduleTask(duckdb::make_uniq<SliceTask>(
        executor, pool, slice_entry->trxs[k], chunk, row_ids, begin, end));
    }
    executor.WorkOnTasks();
    for (size_t k = 0; k < slices; ++k) {
      slice_entry->trxs[k].RegisterFlush();
    }
  }

  template<typename RowAt>
  void FeedDeletesInline(size_t count, RowAt&& row_at) {
    auto& bundle = Inline();
    pool.FeedDeletes(bundle, count, std::forward<RowAt>(row_at));
    inline_trx->RegisterFlush();
  }

  void ReleaseInline() {
    if (inline_bundle) {
      pool.ReleaseBundle(std::move(inline_bundle));
    }
    inline_trx.reset();
  }

  // Phase 1 of the live commit, BEFORE the tick is allocated: finish
  // tokenization and pin every segment onto the flush context. RegisterFlush
  // must precede TickDomain::Advance -- otherwise a refresh whose tick
  // snapshot lands between the Advance and the pin could advance its committed
  // tick past an unpinned segment (lost insert / FlushPending assert).
  // Returns the max per-segment query count for tick-range sizing.
  uint64_t Prepare() {
    uint64_t queries = 0;
    ForEachTransaction([&](irs::IndexWriter::Transaction& trx) {
      trx.RegisterFlush();
      queries = std::max(queries, trx.GetQueries());
    });
    return queries;
  }

  // Phase 2, AFTER the tick is allocated: record the durable cursor (before
  // the segments become flushable) then commit every segment at the commit
  // tick. A commit that fails leaves the index marked out-of-sync (rebuilt on
  // boot) rather than throwing: the store commit is already durable, and this
  // runs inside a noexcept commit, so throwing would take the process down.
  void Finish(uint64_t last_tick, std::optional<search::WalCursor> cursor) {
    auto& storage = pool.storage;
    if (storage && cursor) {
      storage->RecordFlushCursor(last_tick, *cursor);
    }
    ForEachTransaction([&](irs::IndexWriter::Transaction& trx) {
      if (trx.Commit(last_tick)) {
        return;
      }
      SDB_ERROR(SEARCH, "inverted index live feed: commit failed for index '",
                pool.owner.IndexId(), "' at tick ", last_tick,
                "; the index will be rebuilt from the store on next boot");
      if (storage) {
        storage->MarkOutOfSync();
      }
    });
    Reset();
  }

  void Abort() {
    ForEachTransaction([](irs::IndexWriter::Transaction& trx) { trx.Abort(); });
    Reset();
  }

  // Every transaction this commit staged: the sliced ones and, when the
  // committing thread fed a whole chunk or a delete itself, the inline one.
  template<typename Fn>
  void ForEachTransaction(Fn&& fn) {
    for (auto& job : jobs) {
      for (auto& trx : job->trxs) {
        fn(trx);
      }
    }
    if (inline_trx) {
      fn(*inline_trx);
    }
  }

  // One transaction per sub-range of a parallel append scan, filled by the scan
  // jobs themselves (see FeedChunkParallel) rather than by a worker task, and
  // committed with everything else at the commit tick.
  Entry* CreateRangeEntry(size_t subranges) {
    auto job = std::make_unique<Entry>(pool, /*wal_offset=*/0, subranges);
    auto* raw = job.get();
    jobs.push_back(std::move(job));
    return raw;
  }

  void Reset() {
    jobs.clear();
    slice_entry = nullptr;
    slice_count = 0;
    ReleaseInline();
  }

  // Nothing staged, so the pool behind it may be rebuilt. True between commits:
  // Commit and Abort both Reset.
  bool Idle() const noexcept { return jobs.empty() && !inline_trx; }
};

// One index's feed: the shared worker pool, plus the two things that drive it.
struct InvertedFeedSession {
  InvertedFeedSession(std::shared_ptr<search::InvertedIndexStorage> storage,
                      catalog::TokenizerMap dicts,
                      std::shared_ptr<const InvertedIndexFieldOptions> config,
                      duckdb::AttachedDatabase& attached,
                      const InvertedStoreIndex& owner)
    : pool{std::move(storage), std::move(dicts), std::move(config), attached,
           owner},
      live{pool} {}

  // Any teardown path (attach failure destroying the catalog under a live
  // session) must wait out in-flight tasks before the entries they reference
  // die -- so it happens here, ahead of member destruction, not in ~FeedPool.
  ~InvertedFeedSession() {
    try {
      pool.executor.WorkOnTasks();
    } catch (...) {
    }
  }

  // Replay's half is built on first use and never built at all for an index
  // that only ever feeds live commits -- which is every index after boot. It
  // is the heavy one (two deques and a mutex), and the live session would
  // otherwise carry it for the life of the index.
  ReplayQueue& Replay() {
    if (!replay_slot) {
      auto& queue = replay_slot.emplace(pool);
      queue.durable_offset = replay_durable_offset;
      queue.generation = replay_generation;
    }
    return *replay_slot;
  }

  FeedPool pool;
  // Where replay should resume, recorded cheaply at session construction so
  // Replay() can seed the queue if it is ever built.
  uint64_t replay_durable_offset = 0;
  uint64_t replay_generation = 0;
  std::optional<ReplayQueue> replay_slot;

  bool HasReplay() const noexcept { return replay_slot.has_value(); }
  LiveFeed live;
};

// The commit-time entry points query::Transaction drives. Free functions rather
// than a virtual interface: the session is a connector type the transaction can
// name but not complete, and there was only ever one implementation.
uint64_t PrepareInvertedFeed(InvertedFeedSession& feed) {
  return feed.live.Prepare();
}

void FinishInvertedFeed(InvertedFeedSession& feed, uint64_t last_tick,
                        std::optional<search::WalCursor> cursor) {
  feed.live.Finish(last_tick, cursor);
}

void AbortInvertedFeed(InvertedFeedSession& feed) { feed.live.Abort(); }

InvertedStoreIndex::InvertedStoreIndex(
  duckdb::CreateIndexInput& input,
  std::shared_ptr<search::InvertedIndexStorage> storage,
  std::shared_ptr<const InvertedIndexFieldOptions> config,
  catalog::TokenizerMap tokenizers,
  std::optional<catalog::ScorerOptions> top_k_scorer)
  : BoundIndex(input.name, kTypeName, input.constraint_type, input.column_ids,
               input.table_io_manager, input.unbound_expressions, input.db),
    _index_id{IdOption(input.options, kIndexIdOption)},
    _table_id{IdOption(input.options, kTableIdOption)},
    _storage{std::move(storage)},
    _config{std::move(config)},
    _tokenizers{std::move(tokenizers)},
    _top_k_scorer{std::move(top_k_scorer)} {
  SDB_ASSERT(_config);
}

InvertedStoreIndex::~InvertedStoreIndex() = default;

std::shared_ptr<InvertedFeedSession>
InvertedStoreIndex::EnsureInvertedFeedSession() {
  // The configuration comes from the committing transaction's own snapshot, so
  // a commit indexes with the entry version it saw and an ALTER landing
  // mid-flight takes effect for the next transaction. Replay has no committing
  // context and takes the boot snapshot.
  auto* committing = CurrentCommittingContext();
  if (committing != nullptr) {
    // A commit feeds one session per index. The entry can be republished under
    // it -- an online build finishing, an ALTER INDEX committing -- and
    // rebuilding the session then would strand the segments this commit has
    // already staged in the one it is replacing.
    if (auto engaged = committing->InvertedFeed(_index_id)) {
      return engaged;
    }
  }
  // The configuration is decoded once, when the index object is built, and a
  // reshape replaces the whole object -- so a pool that is still here was
  // built against the shape that is still there and never needs rebuilding.
  // ALTER INDEX ... SET applies through InvertedIndexStorage::ApplyOptions
  // instead, which reconfigures the live writer and the maintenance loops.
  if (_feed) {
    return _feed;
  }
  SDB_ENSURE(_storage, "inverted index feed: storage ", _index_id, " missing");
  const search::WalCursor cursor = _storage->GetRecoveryWalCursor();
  uint64_t durable_offset = 0;
  auto& block_manager = db.GetStorageManager().GetBlockManager();
  if (cursor.generation == block_manager.GetCheckpointIteration()) {
    durable_offset = cursor.offset;
  }
  _feed = std::make_shared<InvertedFeedSession>(_storage, _tokenizers, _config,
                                                db, *this);
  _feed->replay_durable_offset = durable_offset;
  _feed->replay_generation = block_manager.GetCheckpointIteration();
  return _feed;
}

namespace {

std::vector<int64_t> ExtractRowIds(duckdb::Vector& row_ids,
                                   duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat fmt;
  row_ids.ToUnifiedFormat(count, fmt);
  const auto* rows = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(fmt);
  std::vector<int64_t> out;
  out.reserve(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    out.push_back(static_cast<int64_t>(rows[fmt.sel->get_index(i)]));
  }
  return out;
}

}  // namespace

// The store-WAL byte offset of the entry currently replaying (stamped by the
// replayer per WAL entry). Operations strictly below the storage's durable
// cursor are already in the segments and are skipped; the op exactly at the
// cursor is the first un-durable one and is streamed. 0 = unknown, don't skip.
duckdb::idx_t InvertedStoreIndex::ReplayCommitOffset() const {
  return duckdb::DuckTransactionManager::Get(db).GetReplayCommitOffset();
}

void InvertedStoreIndex::ReplayAppend(
  const std::shared_ptr<ReplayBatch>& batch) {
  if (batch->data.size() == 0) {
    return;
  }
  auto& session = *EnsureInvertedFeedSession();
  const auto commit_offset = ReplayCommitOffset();
  if (commit_offset != 0 && commit_offset < session.Replay().durable_offset) {
    return;
  }
  session.Replay().Dispatch(
    std::make_unique<Entry>(session.pool, &session.Replay(), commit_offset,
                            InsertPayload{.batch = batch}),
    commit_offset);
}

void InvertedStoreIndex::ReplayDelete(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto& session = *EnsureInvertedFeedSession();
  const auto commit_offset = ReplayCommitOffset();
  if (commit_offset != 0 && commit_offset < session.Replay().durable_offset) {
    return;
  }
  session.Replay().Dispatch(
    std::make_unique<Entry>(
      session.pool, &session.Replay(), commit_offset,
      DeletePayload{.rowids = ExtractRowIds(row_ids, count)}),
    commit_offset);
}

namespace {

// One index taking part in a range replay, and the WAL-ordered entry holding
// its per-sub-range transactions.
struct Participant {
  InvertedFeedSession* session;
  Entry* job;
};

// Every external index on a store table is an InvertedStoreIndex. Inlined: this
// runs per commit and per replay range, and one inverted index is the common
// case.
using ExternalIndexes = absl::InlinedVector<InvertedStoreIndex*, 2>;

ExternalIndexes ExternalIndexesOf(duckdb::TableIndexList& index_list) {
  ExternalIndexes indexes;
  for (auto& index : index_list.Indexes()) {
    if (!index.IsBound()) {
      continue;
    }
    auto& bound = index.Cast<duckdb::BoundIndex>();
    if (bound.GetIndexType() == InvertedStoreIndex::kTypeName) {
      indexes.push_back(&bound.Cast<InvertedStoreIndex>());
    }
  }
  return indexes;
}

ExternalIndexes ExternalIndexesOf(duckdb::DataTable& table) {
  return ExternalIndexesOf(table.GetDataTableInfo()->GetIndexes());
}

// Split the range so several workers decompress it in parallel (one scan feeds
// every index). Sub-range size is the dial between two costs: every sub-range
// is a transaction, and concurrent transactions are distinct segments, so a
// small sub-range buys parallelism now and pays for it at every later refresh.
// Total time is flat across 4k..32k rows; segment count is not (a 5x100k load
// leaves 24 segments at 4k and 6 at 16k), so it is sized for the segments.
size_t SubrangeCount(duckdb::TaskScheduler& scheduler, duckdb::idx_t count) {
  constexpr duckdb::idx_t kRowsPerSubrange = 16384;
  const auto threads = std::max<size_t>(1, scheduler.NumberOfThreads());
  const auto subranges = std::max<duckdb::idx_t>(1, count / kRowsPerSubrange);
  return std::min<size_t>(subranges, threads);
}

// Scans one slice once and feeds it into every index's slot-`k` transaction,
// borrowing each index's pooled writer/expression kit rather than building one:
// constructing a tokenizer provider and an ExpressionExecutor per slice is the
// whole cost of a small append.
template<typename Scanner>
void FeedRange(std::vector<Participant>& parts, size_t slot,
               const Scanner& scan, duckdb::idx_t scan_begin, int64_t row_begin,
               duckdb::idx_t length, bool flush) {
  // deque: a lease is immovable, so the container has to build in place.
  std::deque<FeedPool::BundleLease> bundles;
  for (auto& part : parts) {
    bundles.emplace_back(part.session->pool, part.job->trxs[slot]);
  }
  int64_t row = row_begin;
  scan(scan_begin, length, [&](duckdb::DataChunk& chunk) {
    for (size_t p = 0; p < parts.size(); ++p) {
      parts[p].session->pool.FeedScan(*bundles[p]->insert_writer,
                                      *bundles[p]->exprs, bundles[p]->scratch,
                                      parts[p].job->trxs[slot], chunk, row);
    }
    row += static_cast<int64_t>(chunk.size());
  });
  if (flush) {
    // Recovery refreshes as soon as replay ends, so serialize this sub-range's
    // segment here: otherwise every segment is written one at a time under the
    // writer's commit lock, which is the whole cost of a cheap-tokenizer
    // replay.
    for (auto& part : parts) {
      part.job->trxs[slot].Flush();
    }
  }
}

// Scans one slice once and feeds it into every index's slot-`k` transaction in
// place. `scan` produces table-layout chunks for a row range and is the only
// difference between recovery (a committed table range) and a commit (the local
// row groups being appended): either way the chunk never leaves the callback,
// so it can borrow whatever the scan lends it.
template<typename Scanner>
struct ScanTask final : duckdb::BaseExecutorTask {
  ScanTask(duckdb::TaskExecutor& executor_in,
           std::vector<Participant>& parts_in, size_t slot_in,
           const Scanner& scan_in, duckdb::idx_t scan_begin_in,
           int64_t row_begin_in, duckdb::idx_t length_in, bool flush_in)
    : BaseExecutorTask{executor_in},
      parts{parts_in},
      slot{slot_in},
      scan{scan_in},
      scan_begin{scan_begin_in},
      row_begin{row_begin_in},
      length{length_in},
      flush{flush_in} {}

  void ExecuteTask() override {
    FeedRange(parts, slot, scan, scan_begin, row_begin, length, flush);
  }

  std::string TaskType() const override { return "InvertedRangeScan"; }

  std::vector<Participant>& parts;
  const size_t slot;
  const Scanner& scan;
  const duckdb::idx_t scan_begin;
  const int64_t row_begin;
  const duckdb::idx_t length;
  const bool flush;
};

// Sub-ranges are vector-aligned so each scan starts on a vector boundary and
// its row ids stay exact.
template<typename Scanner>
void RunRangeScans(duckdb::TaskScheduler& scheduler,
                   std::vector<Participant>& parts, size_t subranges,
                   duckdb::idx_t scan_start, duckdb::row_t row_start,
                   duckdb::idx_t count, const Scanner& scan, bool flush) {
  if (subranges == 1) {
    // A small append pays nothing for the machinery: no task, no executor.
    FeedRange(parts, 0, scan, scan_start, row_start, count, flush);
    return;
  }
  duckdb::TaskExecutor executor{scheduler};
  for (size_t k = 0; k < subranges; ++k) {
    auto sub_begin = (k * count) / subranges;
    auto sub_end = ((k + 1) * count) / subranges;
    sub_begin -= sub_begin % STANDARD_VECTOR_SIZE;
    if (k + 1 != subranges) {
      sub_end -= sub_end % STANDARD_VECTOR_SIZE;
    }
    if (sub_end <= sub_begin) {
      continue;
    }
    executor.ScheduleTask(duckdb::make_uniq<ScanTask<Scanner>>(
      executor, parts, k, scan, scan_start + sub_begin,
      row_start + static_cast<int64_t>(sub_begin), sub_end - sub_begin, flush));
  }
  executor.WorkOnTasks();
}

// The body both range feeds share: size the partition, let every index
// contribute the entry it wants, then scan each sub-range once and feed it into
// all of them in place. `make_entry` is where the two differ -- a live commit
// stages into the commit window, a replay stages into a WAL-ordered entry and
// may decline an index already durable past the offset. Returns the
// participants, for the caller that has to close them.
template<typename MakeEntry, typename Scanner>
std::vector<Participant> RunRangeFeed(duckdb::TaskScheduler& scheduler,
                                      const ExternalIndexes& indexes,
                                      duckdb::idx_t scan_start,
                                      duckdb::row_t row_start,
                                      duckdb::idx_t count, const Scanner& scan,
                                      bool flush, MakeEntry&& make_entry) {
  const auto subranges = SubrangeCount(scheduler, count);
  std::vector<Participant> parts;
  parts.reserve(indexes.size());
  for (auto* inverted : indexes) {
    if (auto part = make_entry(*inverted, subranges)) {
      parts.push_back(*part);
    }
  }
  if (!parts.empty()) {
    RunRangeScans(scheduler, parts, subranges, scan_start, row_start, count,
                  scan, flush);
  }
  return parts;
}

// Scans [begin, begin + length) of the row groups a commit is about to append,
// in table layout, with a scan state of its own -- so nothing it borrows
// outlives the callback and no row is scanned twice.
template<typename Fn>
void ScanLocalRange(duckdb::DuckTransaction& transaction,
                    duckdb::RowGroupCollection& source,
                    const duckdb::vector<duckdb::StorageIndex>& columns,
                    const duckdb::vector<duckdb::LogicalType>& scan_types,
                    duckdb::idx_t begin, duckdb::idx_t length, Fn&& fn) {
  const auto& table_types = source.GetTypes();
  duckdb::TableScanState state;
  state.Initialize(columns, nullptr);
  source.InitializeScanWithOffset(duckdb::QueryContext(), state.local_state,
                                  columns, begin, begin + length);

  duckdb::DataChunk scanned;
  scanned.Initialize(source.GetAllocator(), scan_types);
  // The feed reads columns at their table positions; the scan produces them in
  // `columns` order, so hand it a table-shaped view referencing the scan chunk.
  duckdb::DataChunk view;
  view.InitializeEmpty(table_types);
  for (duckdb::idx_t produced = 0; produced < length;) {
    scanned.Reset();
    state.local_state.Scan(transaction, scanned);
    if (scanned.size() == 0) {
      break;
    }
    if (produced + scanned.size() > length) {
      scanned.SetCardinality(length - produced);
    }
    for (duckdb::idx_t i = 0; i < columns.size(); ++i) {
      view.data[columns[i].GetPrimaryIndex()].Reference(scanned.data[i]);
    }
    view.SetCardinality(scanned.size());
    fn(view);
    produced += scanned.size();
  }
}

}  // namespace

// DBConfig::external_local_append target: feed every inverted index of a store
// table with the rows a commit is about to append. Same shape as
// ReplayExternalRange -- partition the range, one scan per worker, feed in
// place
// -- so a chunk is never handed to a thread that did not scan it and nothing is
// copied or scanned twice. Runs on the committing thread inside the storage
// commit; the transactions it fills commit at CommitSearch's tick.
duckdb::ErrorData InvertedStoreIndex::AppendLocalRange(
  duckdb::DuckTransaction& transaction, duckdb::TableIndexList& index_list,
  duckdb::RowGroupCollection& source,
  const duckdb::vector<duckdb::StorageIndex>& mapped_column_ids,
  duckdb::row_t row_start) {
  const auto count = source.GetTotalRows();
  if (count == 0) {
    return {};
  }
  auto* conn = CurrentCommittingContext();
  SDB_ENSURE(conn, "inverted index append: no committing context");

  auto& scheduler =
    duckdb::TaskScheduler::GetScheduler(source.GetAttached().GetDatabase());
  const auto indexes = ExternalIndexesOf(index_list);
  if (indexes.empty()) {
    return {};
  }

  const auto& table_types = source.GetTypes();
  duckdb::vector<duckdb::LogicalType> scan_types;
  scan_types.reserve(mapped_column_ids.size());
  for (const auto& id : mapped_column_ids) {
    scan_types.push_back(table_types[id.GetPrimaryIndex()]);
  }
  const auto scan = [&transaction, &source, &mapped_column_ids, &scan_types](
                      duckdb::idx_t begin, duckdb::idx_t length, auto&& fn) {
    ScanLocalRange(transaction, source, mapped_column_ids, scan_types, begin,
                   length, fn);
  };
  // Uncommitted row groups carry transaction-local row ids, which start at
  // MAX_ROW_ID; the final ids the index keys on start at `row_start`.
  const auto scan_start = static_cast<duckdb::idx_t>(duckdb::MAX_ROW_ID);
  try {
    RunRangeFeed(
      scheduler, indexes, scan_start, row_start, count, scan, /*flush=*/false,
      [&](InvertedStoreIndex& inverted,
          size_t subranges) -> std::optional<Participant> {
        const auto& engaged = inverted.EnsureInvertedFeedSession();
        conn->EngageInvertedFeed(inverted._index_id, engaged);
        auto& session = *engaged;
        return Participant{&session, session.live.CreateRangeEntry(subranges)};
      });
  } catch (const std::exception& e) {
    return duckdb::ErrorData{e};
  }
  return {};
}

void InvertedStoreIndex::ReplayExternalRange(duckdb::ClientContext& context,
                                             duckdb::DataTable& table,
                                             duckdb::row_t row_start,
                                             duckdb::idx_t count) {
  if (count == 0) {
    return;
  }
  const auto indexes = ExternalIndexesOf(table);
  if (indexes.empty()) {
    return;
  }

  auto& db = table.db;
  auto& transaction = duckdb::DuckTransaction::Get(context, db);
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(db.GetDatabase());
  const auto wal_offset =
    duckdb::DuckTransactionManager::Get(db).GetReplayCommitOffset();

  // duckdb's scan takes a std::function, so this one boundary keeps it.
  const auto scan = [&table, &transaction](duckdb::idx_t begin,
                                           duckdb::idx_t length, auto&& fn) {
    table.ScanTableSegment(transaction, begin, length, fn);
  };
  // One WAL-ordered entry per index not already durable past this offset. The
  // lambda is here rather than in the shared body because it reaches this
  // class's private session accessor.
  const auto parts = RunRangeFeed(
    scheduler, indexes, static_cast<duckdb::idx_t>(row_start), row_start, count,
    scan, /*flush=*/true,
    [&](InvertedStoreIndex& inverted,
        size_t subranges) -> std::optional<Participant> {
      auto& session = *inverted.EnsureInvertedFeedSession();
      if (wal_offset != 0 && wal_offset < session.Replay().durable_offset) {
        return std::nullopt;
      }
      return Participant{
        &session, session.Replay().CreateRangeEntry(wal_offset, subranges)};
    });
  for (auto& part : parts) {
    part.session->Replay().CompleteRangeEntry(*part.job);
  }
}

void InvertedStoreIndex::FinishReplay() {
  if (!_feed) {
    return;
  }
  // Hand the session over: whatever replayed is finished here, and ordinary
  // commits rebuild it. Taking it first means an exception cannot leave a
  // half-retired session behind on the index.
  const auto session = std::move(_feed);
  if (!session->HasReplay()) {
    // Nothing ever replayed into this index, so there is no queue to retire --
    // and asking for one here would build the heavy half just to drop it.
    return;
  }
  const auto success_offset = ReplaySuccessOffset();
  session->pool.executor.WorkOnTasks();
  session->Replay().FinishRetire(success_offset);
}

duckdb::ErrorData InvertedStoreIndex::AppendImpl(duckdb::DataChunk& chunk,
                                                 duckdb::Vector& row_ids) {
  // Reached only when the producer had no batch to hand over: this chunk is a
  // buffer it recycles, so a worker must not reference it. Feed on this thread,
  // through the same pooled writer/expression kit the workers use.
  auto* conn = CurrentCommittingContext();
  SDB_ENSURE(conn, "inverted index append: no committing context");
  if (chunk.size() == 0) {
    return {};
  }
  const auto& engaged = EnsureInvertedFeedSession();
  conn->EngageInvertedFeed(_index_id, engaged);
  engaged->live.FeedChunkParallel(chunk, row_ids);
  return {};
}

duckdb::ErrorData InvertedStoreIndex::Append(duckdb::IndexLock&,
                                             duckdb::DataChunk& chunk,
                                             duckdb::Vector& row_ids) {
  return AppendImpl(chunk, row_ids);
}

duckdb::ErrorData InvertedStoreIndex::Insert(duckdb::IndexLock&,
                                             duckdb::DataChunk& chunk,
                                             duckdb::Vector& row_ids) {
  return AppendImpl(chunk, row_ids);
}

void InvertedStoreIndex::Delete(duckdb::IndexLock&, duckdb::DataChunk& chunk,
                                duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    ReplayDelete(chunk, row_ids);
    return;
  }
  const auto& engaged = EnsureInvertedFeedSession();
  auto& session = *engaged;
  if (!_config->pk.index_term) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("inverted index \"", name.GetIdentifierName(),
              "\" was created WITH (store_pk = 'none') and does not "
              "index row PKs: DELETE/UPDATE cannot maintain it; drop "
              "the index first or recreate it without store_pk = "
              "'none'"));
  }
  conn->EngageInvertedFeed(_index_id, engaged);
  const auto& storage = session.pool.storage;
  duckdb::UnifiedVectorFormat fmt;
  row_ids.ToUnifiedFormat(count, fmt);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(fmt);
  if (storage->IsDeleteLogOpen()) {
    const auto log_begin = storage->DeleteLogRowidBegin();
    const auto log_end = storage->DeleteLogRowidEnd();
    std::vector<int64_t> native;
    std::vector<int64_t> logged;
    native.reserve(count);
    logged.reserve(count);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      const int64_t row = data[fmt.sel->get_index(i)];
      (row < log_begin || row >= log_end ? native : logged).push_back(row);
    }
    // Reads like a use-after-move and is not: AppendDeleteLog only takes the
    // vector when it accepts it, and returns false without touching it once the
    // log is latched. Then these rows are past publication and delete natively.
    if (!logged.empty() && !storage->AppendDeleteLog(std::move(logged))) {
      absl::c_move(logged, std::back_inserter(native));
    }
    if (!native.empty()) {
      session.live.FeedDeletesInline(native.size(),
                                     [&](size_t i) { return native[i]; });
    }
    return;
  }
  session.live.FeedDeletesInline(
    count, [&](size_t i) { return data[fmt.sel->get_index(i)]; });
}

idx_t InvertedStoreIndex::TryDelete(
  duckdb::IndexLock& l, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
  duckdb::optional_ptr<duckdb::SelectionVector> deleted_sel,
  duckdb::optional_ptr<duckdb::SelectionVector>) {
  Delete(l, chunk, row_ids);
  if (deleted_sel) {
    for (duckdb::idx_t i = 0; i < chunk.size(); ++i) {
      deleted_sel->set_index(i, i);
    }
  }
  return chunk.size();
}

std::string InvertedStoreIndex::ToString(duckdb::IndexLock&, bool) {
  return "inverted store index";
}

void InvertedStoreIndex::CheckpointBarrier() {
  if (!_storage) {
    return;
  }
  SDB_ENSURE(!_storage->IsOutOfSync(), "inverted index ", _index_id,
             " is out of sync with its store table; refusing to checkpoint "
             "(WAL retained for replay; REINDEX to clear)");
  _storage->CheckpointRefresh();
}

// The catalog-WAL phase supplies the real bound (how far the WAL replayed
// cleanly, so a torn tail is dropped); until then every buffered op retires.
duckdb::idx_t InvertedStoreIndex::ReplaySuccessOffset() const {
  return std::numeric_limits<duckdb::idx_t>::max() - 1;
}

std::string InvertedStoreIndex::GetConstraintViolationMessage(
  duckdb::VerifyExistenceType, idx_t, duckdb::DataChunk&) {
  return "inverted store index constraint violation";
}

duckdb::optional_ptr<const InvertedStoreIndex> FindInvertedStore(
  duckdb::ClientContext& context, const duckdb::IndexCatalogEntry& entry) {
  if (!IsInvertedIndex(entry)) {
    return nullptr;
  }
  const auto& index_entry = entry.Cast<duckdb::DuckIndexEntry>();
  // A view-backed index is registered in no table's index list: nothing
  // live-feeds it, so there is no bound index to find. Its storage hangs off
  // the catalog entry instead.
  if (!index_entry.info || !index_entry.info->info) {
    return nullptr;
  }
  auto& info = index_entry.GetDataTableInfo();
  info.BindIndexes(context, InvertedStoreIndex::kTypeName);
  auto bound = info.GetIndexes().Find(entry.name);
  if (!bound || !bound->IsBound() ||
      bound->GetIndexType() != std::string{InvertedStoreIndex::kTypeName}) {
    return nullptr;
  }
  return &bound->Cast<InvertedStoreIndex>();
}

duckdb::unique_ptr<duckdb::BoundIndex> InvertedStoreIndex::Create(
  duckdb::CreateIndexInput& input) {
  // Everything this needs is in the record duckdb read back: the id names the
  // entry, and the entry says the rest. No injection pass and no held
  // definition -- the registry builds the index the way it builds an ART.
  const auto index_id = IdOption(input.options, kIndexIdOption);
  const auto entry = FindIndexEntry(&input.context, input.db, index_id);
  SDB_ENSURE(entry, "inverted index: catalog entry for ", index_id, " missing");
  auto config = entry->Cast<catalog::InvertedIndexEntry>().Config();
  SDB_ENSURE(config, "inverted index entry has no configuration");
  auto tokenizers = ResolveKeyTokenizers(input.context, input.db, entry->schema,
                                         entry->options);
  auto top_k_scorer = ParseTopKScorer(input.context, entry->options);
  // A rebind (an ALTER-driven table rebuild, a re-bind after replay) must not
  // open a second writer over the same directory, so it adopts the storage the
  // index already registered under this name is holding.
  std::shared_ptr<search::InvertedIndexStorage> storage;
  auto& indexes = entry->Cast<duckdb::DuckIndexEntry>().GetDataTableInfo();
  if (auto bound = indexes.GetIndexes().Find(input.name);
      bound && bound->IsBound() &&
      bound->GetIndexType() == std::string{kTypeName}) {
    storage = bound->Cast<InvertedStoreIndex>().Storage();
  }
  return duckdb::make_uniq<InvertedStoreIndex>(
    input, std::move(storage), std::move(config), std::move(tokenizers),
    std::move(top_k_scorer));
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToDisk(
  duckdb::QueryContext context,
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  // The postings are in the iresearch directory, so a checkpoint has nothing
  // to write here; the name is what lets the reload find the index again.
  return duckdb::IndexStorageInfo{name};
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToWAL(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  return duckdb::IndexStorageInfo{name};
}

duckdb::IndexType InvertedStoreIndex::GetInvertedIndexType() {
  duckdb::IndexType type;
  type.name = kTypeName;
  type.create_instance = &InvertedStoreIndex::Create;
  // duckdb's default plan is ART-shaped: it hard-casts, rejects views and
  // finalizes against an in-memory index. See DESIGN_INVERTED_INDEX.md §5.
  type.create_plan = &SereneDBCreateIndexPlan;
  // Bound by name once the storage layer is up, never by the implicit
  // "bind all indexes" pass.
  type.defer_implicit_bind = true;
  return type;
}

std::shared_ptr<search::InvertedIndexStorage> PublishViewInvertedIndex(
  duckdb::ClientContext& context, duckdb::SchemaCatalogEntry& schema,
  duckdb::idx_t database_id, duckdb::idx_t relation_id, duckdb::idx_t index_id,
  const duckdb::CreateIndexInfo& info) {
  auto top_k_scorer = ParseTopKScorer(context, info.options);
  const auto options = catalog::DecodeInvertedIndexOptions(info.options);
  auto index_storage = search::InvertedIndexStorage::Create(
    database_id, schema.oid, relation_id, index_id, options, top_k_scorer,
    /*is_new=*/true);
  SDB_ENSURE(index_storage, "view index storage create returned null");
  index_storage->ApplyOptions(options);
  return index_storage;
}

duckdb::optional_ptr<const InvertedStoreIndex> PublishNewInvertedIndex(
  duckdb::ClientContext& context, duckdb::DataTable& storage,
  duckdb::SchemaCatalogEntry& schema, duckdb::idx_t database_id,
  duckdb::idx_t table_id, duckdb::idx_t index_id, duckdb::CreateIndexInfo& info,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& bound_exprs,
  const catalog::InvertedIndexEntry& entry) {
  auto& table_info = *storage.GetDataTableInfo();
  const duckdb::Identifier name = info.GetIndexName();

  // The entry decoded this from the same options; taking its object rather
  // than decoding a second one is what keeps a single definition per index --
  // and iresearch compares definitions by pointer identity.
  auto config = entry.Config();
  SDB_ENSURE(config, "inverted index entry has no configuration");
  auto tokenizers =
    ResolveKeyTokenizers(context, storage.db, schema, info.options);
  auto top_k_scorer = ParseTopKScorer(context, info.options);

  const auto options = catalog::DecodeInvertedIndexOptions(info.options);
  auto index_storage = search::InvertedIndexStorage::Create(
    database_id, schema.oid, table_id, index_id, options, top_k_scorer,
    /*is_new=*/true);
  // Opening the directory is half of it: the writer's segment limits and the
  // maintenance intervals come from the WITH options, and a writer left with
  // the defaults never gets them.
  index_storage->ApplyOptions(options);

  duckdb::CreateIndexInput input{
    context,     duckdb::TableIOManager::Get(storage),
    storage.db,  info.constraint_type,
    name,        info.column_ids,
    bound_exprs, duckdb::IndexStorageInfo{name},
    info.options};
  auto index = duckdb::make_uniq<InvertedStoreIndex>(
    input, std::move(index_storage), std::move(config), std::move(tokenizers),
    std::move(top_k_scorer));
  auto published = index.get();
  table_info.GetIndexes().AddIndex(std::move(index));
  return published;
}

}  // namespace sdb::connector
