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

// The repl_src('schema','table','col,col,...') table function and the pgoutput
// row decode + DML construction behind it -- the SOURCE of a subscriber's
// batched DELETE/UPDATE/INSERT. Kept apart from the network client
// (pg_replication_client): this is the parser/execution side (build the DML,
// decode rows into vectors during the scan), that is the io/transport side.

#include "replication/repl_source.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/comparison_expression.hpp>
#include <duckdb/parser/expression/conjunction_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/star_expression.hpp>
#include <duckdb/parser/parsed_data/copy_info.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/query_node/update_query_node.hpp>
#include <duckdb/parser/statement/copy_statement.hpp>
#include <duckdb/parser/statement/delete_statement.hpp>
#include <duckdb/parser/statement/insert_statement.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/statement/update_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <stdexcept>
#include <utility>

#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/deserialize.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "replication/repl_stream.h"

namespace sdb::replication {
namespace {

// Read a whole pgoutput tuple into a column vector for random access; the views
// alias the frame buffer and are valid only for the duration of the apply.
std::vector<PgColumn> ReadTuple(std::string_view tuple) {
  PgTupleReader reader{tuple};
  std::vector<PgColumn> cols;
  cols.reserve(reader.Count());
  while (reader.HasNext()) {
    cols.push_back(reader.Next());
  }
  return cols;
}

// The text and binary decoders for one column; the tuple's kind byte selects
// which. pgoutput sends 't' (text) unless `binary` was negotiated, then 'b'
// (and it may still mix in 't' for types without a binary send function).
struct ColDeser {
  sdb::pg::DeserializationFunction<sdb::pg::VectorSink> text = nullptr;
  sdb::pg::DeserializationFunction<sdb::pg::VectorSink> bin = nullptr;
};

// Decode one pgoutput column straight into vec[row], per its kind byte.
void DecodeCell(sdb::pg::DeserializeContext& dctx, duckdb::Vector& vec,
                duckdb::idx_t row, const PgColumn& col, const ColDeser& d) {
  sdb::pg::VectorSink sink{vec, row};
  switch (col.kind) {
    case TupleColKind::Null:
      sink.SetNull();
      return;
    case TupleColKind::Text:
      if (d.text != nullptr && d.text(dctx, col.data, sink)) {
        return;
      }
      break;
    case TupleColKind::Binary:
      if (d.bin != nullptr && d.bin(dctx, col.data, sink)) {
        return;
      }
      break;
    case TupleColKind::Unchanged:
      break;  // never selected into a batch column
  }
  throw std::runtime_error("subscription: failed to decode column");
}

// repl_src() as a table function ref, aliased `src`. It takes no arguments: its
// columns are the current apply batch's, which BindReplSource reads off the
// ConnectionContext. `aliases` are the distinct SQL-visible names the caller
// references as src.<alias> (a key may be sourced as both the old WHERE key and
// its new SET value, so those get distinct k#/v# aliases).
duckdb::unique_ptr<duckdb::TableRef> MakeReplSrc(
  const std::vector<std::string>& aliases) {
  auto ref = duckdb::make_uniq<duckdb::TableFunctionRef>();
  ref->function = duckdb::make_uniq<duckdb::FunctionExpression>(
    duckdb::Identifier{"repl_src"},
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>{});
  ref->alias = "src";
  for (const auto& a : aliases) {
    ref->column_name_alias.push_back(duckdb::Identifier{a});
  }
  return ref;
}

duckdb::unique_ptr<duckdb::BaseTableRef> TargetTable(std::string_view schema,
                                                     std::string_view table) {
  auto ref = duckdb::make_uniq<duckdb::BaseTableRef>();
  ref->SetQualifiedName(duckdb::Identifier{}, duckdb::Identifier{schema},
                        duckdb::Identifier{table});
  ref->alias = "tgt";
  return ref;
}

// INSERT INTO schema.table (cols) SELECT * FROM repl_src()
duckdb::unique_ptr<duckdb::SQLStatement> BuildInsert(
  std::string_view schema, std::string_view table,
  const std::vector<std::string>& cols) {
  auto node = duckdb::make_uniq<duckdb::InsertQueryNode>();
  node->SetQualifiedName(duckdb::Identifier{}, duckdb::Identifier{schema},
                         duckdb::Identifier{table});
  for (const auto& c : cols) {
    node->columns.push_back(duckdb::Identifier{c});
  }
  node->column_order = duckdb::InsertColumnOrder::INSERT_BY_POSITION;

  auto select = duckdb::make_uniq<duckdb::SelectStatement>();
  auto sel = duckdb::make_uniq<duckdb::SelectNode>();
  sel->select_list.push_back(duckdb::make_uniq<duckdb::StarExpression>());
  sel->from_table = MakeReplSrc(cols);
  select->node = std::move(sel);
  node->select_statement = std::move(select);

  auto stmt = duckdb::make_uniq<duckdb::InsertStatement>();
  stmt->node = std::move(node);
  return stmt;
}

// tgt.k = src.k [AND ..] over the key columns. Under REPLICA IDENTITY FULL the
// match is NULL-safe (IS NOT DISTINCT FROM), since any column may be NULL.
duckdb::unique_ptr<duckdb::ParsedExpression> KeyEq(
  const std::vector<std::string>& keys, bool full) {
  const auto cmp = full ? duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM
                        : duckdb::ExpressionType::COMPARE_EQUAL;
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> terms;
  for (const auto& k : keys) {
    auto lhs = duckdb::make_uniq<duckdb::ColumnRefExpression>(
      duckdb::Identifier{k}, duckdb::Identifier{"tgt"});
    auto rhs = duckdb::make_uniq<duckdb::ColumnRefExpression>(
      duckdb::Identifier{k}, duckdb::Identifier{"src"});
    terms.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
      cmp, std::move(lhs), std::move(rhs)));
  }
  if (terms.size() == 1) {
    return std::move(terms.front());
  }
  return duckdb::make_uniq<duckdb::ConjunctionExpression>(
    duckdb::ExpressionType::CONJUNCTION_AND, std::move(terms));
}

// DELETE FROM schema.table AS tgt USING repl_src() AS src(keys) WHERE
// tgt.k=src.k
duckdb::unique_ptr<duckdb::SQLStatement> BuildDelete(
  std::string_view schema, std::string_view table,
  const std::vector<std::string>& keys, bool full) {
  auto node = duckdb::make_uniq<duckdb::DeleteQueryNode>();
  node->table = TargetTable(schema, table);
  node->using_clauses.push_back(MakeReplSrc(keys));
  node->condition = KeyEq(keys, full);

  auto stmt = duckdb::make_uniq<duckdb::DeleteStatement>();
  stmt->node = std::move(node);
  return stmt;
}

// UPDATE schema.table AS tgt SET col=src.v0,.. FROM repl_src() AS src(k0,..,
// v0,..) WHERE tgt.key=src.k0 AND ..  The source carries the OLD key columns
// (k#, for the WHERE) followed by every column present in the new tuple (v#,
// for the SET) -- so a key-changing update rewrites the key too, keyed on its
// old value. keys and sets may name the same column; distinct k#/v# aliases
// keep them unambiguous.
duckdb::unique_ptr<duckdb::SQLStatement> BuildUpdate(
  std::string_view schema, std::string_view table,
  const std::vector<std::string>& keys, const std::vector<std::string>& sets,
  bool full) {
  std::vector<std::string> aliases;
  aliases.reserve(keys.size() + sets.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    aliases.push_back("k" + std::to_string(i));
  }
  for (size_t j = 0; j < sets.size(); ++j) {
    aliases.push_back("v" + std::to_string(j));
  }

  auto node = duckdb::make_uniq<duckdb::UpdateQueryNode>();
  node->table = TargetTable(schema, table);
  node->from_table = MakeReplSrc(aliases);

  auto set_info = duckdb::make_uniq<duckdb::UpdateSetInfo>();
  for (size_t j = 0; j < sets.size(); ++j) {
    set_info->columns.push_back(duckdb::Identifier{sets[j]});
    set_info->expressions.push_back(
      duckdb::make_uniq<duckdb::ColumnRefExpression>(
        duckdb::Identifier{"v" + std::to_string(j)},
        duckdb::Identifier{"src"}));
  }

  const auto cmp = full ? duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM
                        : duckdb::ExpressionType::COMPARE_EQUAL;
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> terms;
  for (size_t i = 0; i < keys.size(); ++i) {
    auto lhs = duckdb::make_uniq<duckdb::ColumnRefExpression>(
      duckdb::Identifier{keys[i]}, duckdb::Identifier{"tgt"});
    auto rhs = duckdb::make_uniq<duckdb::ColumnRefExpression>(
      duckdb::Identifier{"k" + std::to_string(i)}, duckdb::Identifier{"src"});
    terms.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
      cmp, std::move(lhs), std::move(rhs)));
  }
  set_info->condition =
    terms.size() == 1
      ? std::move(terms.front())
      : duckdb::make_uniq<duckdb::ConjunctionExpression>(
          duckdb::ExpressionType::CONJUNCTION_AND, std::move(terms));
  node->set_info = std::move(set_info);

  auto stmt = duckdb::make_uniq<duckdb::UpdateStatement>();
  stmt->node = std::move(node);
  return stmt;
}

// Decode one row (msg) that belongs to `batch` into output[row], columns in the
// batch order via `deser`. Returns false if msg belongs to a different batch (a
// boundary the scan stops on) -- the analogue of COPY's per-row ProcessRow.
bool DecodeRow(const ReplBatch& batch, const PgOutputMessage& msg,
               const std::vector<ColDeser>& deser,
               sdb::pg::DeserializeContext& dctx, duckdb::DataChunk& output,
               duckdb::idx_t row) {
  const auto relid = RowRelId(msg);
  if (!relid || *relid != batch.relid) {
    return false;
  }
  char op = 0;
  std::vector<size_t> keys;
  std::vector<size_t> cols;
  std::vector<PgColumn> cells;
  bool full = false;
  if (!RowShape(msg, *batch.rel, op, keys, cols, cells, full)) {
    return false;
  }
  if (op != batch.op || keys != batch.keys || cols != batch.cols ||
      full != batch.full) {
    return false;  // same relation, different statement shape -> next batch
  }
  if (op == 'I') {
    for (size_t j = 0; j < batch.cols.size(); ++j) {
      DecodeCell(dctx, output.data[j], row, cells[batch.cols[j]], deser[j]);
    }
  } else if (op == 'D') {
    for (size_t j = 0; j < batch.keys.size(); ++j) {
      DecodeCell(dctx, output.data[j], row, cells[batch.keys[j]], deser[j]);
    }
  } else {  // 'U'
    const auto& upd = std::get<UpdateMessage>(msg);
    std::vector<PgColumn> old_cells;
    if (upd.has_old) {
      old_cells = ReadTuple(upd.old_tuple);
      if (old_cells.size() != cells.size()) {
        return false;
      }
    }
    const auto& key_cells = upd.has_old ? old_cells : cells;
    const size_t nkeys = batch.keys.size();
    for (size_t j = 0; j < nkeys; ++j) {
      DecodeCell(dctx, output.data[j], row, key_cells[batch.keys[j]], deser[j]);
    }
    for (size_t k = 0; k < batch.cols.size(); ++k) {
      DecodeCell(dctx, output.data[nkeys + k], row, cells[batch.cols[k]],
                 deser[nkeys + k]);
    }
  }
  return true;
}

using connector::kSereneDBClientStateKey;
using connector::SereneDBClientState;

struct ReplSourceBindData final : public duckdb::FunctionData {
  duckdb::vector<duckdb::LogicalType> return_types;
  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
    auto r = duckdb::make_uniq<ReplSourceBindData>();
    r->return_types = return_types;
    return r;
  }
  bool Equals(const duckdb::FunctionData& other) const override {
    return return_types == other.Cast<ReplSourceBindData>().return_types;
  }
};

struct ReplSourceGlobalState final : public duckdb::GlobalTableFunctionState {
  ReplBatch* batch = nullptr;
  std::vector<ColDeser> deser;  // per output column, text + binary decoders
};

duckdb::unique_ptr<duckdb::FunctionData> BindReplSource(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<std::string>& names) {
  // repl_src takes no arguments: its columns are the current apply batch's,
  // which the subscriber published on the ConnectionContext before Prepare. The
  // batch's relation already carries the local column types (resolved from the
  // catalog in OnRelation), so no catalog lookup is needed here.
  auto* state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey)
      .get();
  if (state == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("repl_src requires a serenedb connection"));
  }
  const ReplBatch* batch = state->GetConnectionContext().GetReplBatch();
  if (batch == nullptr || batch->rel == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("repl_src: no active replication batch"));
  }
  // Column output order = keys then cols (keys is empty for INSERT, cols is
  // empty for DELETE), matching what the scan's DecodeRow writes: DELETE/UPDATE
  // WHERE keys first, then the INSERT/UPDATE set columns.
  const auto emit = [&](size_t idx) {
    return_types.push_back(batch->rel->columns[idx].type);
    names.push_back(batch->rel->columns[idx].name);
  };
  for (size_t idx : batch->keys) {
    emit(idx);
  }
  for (size_t idx : batch->cols) {
    emit(idx);
  }
  auto result = duckdb::make_uniq<ReplSourceBindData>();
  result->return_types = return_types;
  return result;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobalReplSource(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput&) {
  auto result = duckdb::make_uniq<ReplSourceGlobalState>();
  auto* state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey)
      .get();
  if (state == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("repl_src requires a serenedb connection"));
  }
  result->batch = state->GetConnectionContext().GetReplBatch();
  return result;
}

void ScanReplSource(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
                    duckdb::DataChunk& output) {
  auto& g = input.global_state->Cast<ReplSourceGlobalState>();
  if (g.batch == nullptr || g.batch->stream == nullptr) {
    output.SetCardinality(0);
    return;
  }
  const auto& bind = input.bind_data->Cast<ReplSourceBindData>();
  if (g.deser.empty() && !bind.return_types.empty()) {
    g.deser.reserve(bind.return_types.size());
    for (const auto& type : bind.return_types) {
      g.deser.push_back({sdb::pg::GetDeserialization<sdb::pg::VectorSink>(
                           type, sdb::pg::VarFormat::Text),
                         sdb::pg::GetDeserialization<sdb::pg::VectorSink>(
                           type, sdb::pg::VarFormat::Binary)});
    }
  }
  // Own the loop (like ScanFrom): pull the next row off the stream, decode it,
  // stop at EOF or the batch boundary. The stream is a pure conduit.
  sdb::pg::DeserializeContext dctx{g.batch->snapshot.get()};
  duckdb::idx_t row = 0;
  while (row < STANDARD_VECTOR_SIZE) {
    const PgOutputMessage* m = g.batch->stream->PeekBlocking();
    if (m == nullptr) {  // stream EOF
      break;
    }
    if (!DecodeRow(*g.batch, *m, g.deser, dctx, output, row)) {  // boundary
      break;
    }
    g.batch->stream->Advance();
    ++row;
  }
  output.SetChildCardinality(row);
}

}  // namespace

std::optional<uint32_t> RowRelId(const PgOutputMessage& msg) {
  if (const auto* m = std::get_if<InsertMessage>(&msg)) {
    return m->relation_id;
  }
  if (const auto* m = std::get_if<UpdateMessage>(&msg)) {
    return m->relation_id;
  }
  if (const auto* m = std::get_if<DeleteMessage>(&msg)) {
    return m->relation_id;
  }
  return std::nullopt;
}

namespace {

// Present (non-`u`) columns of a tuple -- the match set under REPLICA IDENTITY
// FULL, where the old tuple carries every column (unchanged-TOAST ones
// omitted).
void PresentColumns(const std::vector<PgColumn>& cells,
                    std::vector<size_t>& out) {
  for (size_t i = 0; i < cells.size(); ++i) {
    if (cells[i].kind != TupleColKind::Unchanged) {
      out.push_back(i);
    }
  }
}

}  // namespace

bool RowShape(const PgOutputMessage& msg, const RelInfo& rel, char& op,
              std::vector<size_t>& keys, std::vector<size_t>& cols,
              std::vector<PgColumn>& cells, bool& full) {
  keys.clear();
  cols.clear();
  full = false;
  if (const auto* m = std::get_if<InsertMessage>(&msg)) {
    cells = ReadTuple(m->new_tuple);
    if (cells.size() != rel.columns.size()) {
      return false;
    }
    op = 'I';
    cols.reserve(rel.columns.size());
    for (size_t i = 0; i < rel.columns.size(); ++i) {
      cols.push_back(i);
    }
    return true;
  }
  if (const auto* m = std::get_if<DeleteMessage>(&msg)) {
    cells = ReadTuple(m->old_tuple);
    if (cells.size() != rel.columns.size()) {
      return false;
    }
    op = 'D';
    if (m->old_is_key) {  // DEFAULT / USING INDEX: key columns only
      for (size_t i = 0; i < rel.columns.size(); ++i) {
        if (rel.columns[i].is_key) {
          keys.push_back(i);
        }
      }
    } else {  // REPLICA IDENTITY FULL: match on the whole old row
      full = true;
      PresentColumns(cells, keys);
    }
    return !keys.empty();
  }
  if (const auto* m = std::get_if<UpdateMessage>(&msg)) {
    cells = ReadTuple(m->new_tuple);
    if (cells.size() != rel.columns.size()) {
      return false;
    }
    op = 'U';
    // SET columns = every column present in the new tuple (keys included, so a
    // key change rewrites the key). Unchanged-TOAST columns keep their value.
    for (size_t i = 0; i < rel.columns.size(); ++i) {
      if (cells[i].kind != TupleColKind::Unchanged) {
        cols.push_back(i);
      }
    }
    // WHERE keys: under FULL the old tuple ('O') carries the whole row; else
    // the relation's replica-identity key columns.
    if (m->has_old && !m->old_is_key) {
      full = true;
      const auto old_cells = ReadTuple(m->old_tuple);
      if (old_cells.size() != rel.columns.size()) {
        return false;
      }
      PresentColumns(old_cells, keys);
    } else {
      for (size_t i = 0; i < rel.columns.size(); ++i) {
        if (rel.columns[i].is_key) {
          keys.push_back(i);
        }
      }
    }
    return !keys.empty() && !cols.empty();
  }
  return false;
}

duckdb::unique_ptr<duckdb::SQLStatement> BuildReplStatement(
  char op, std::string_view schema, std::string_view table,
  const std::vector<std::string>& key_names,
  const std::vector<std::string>& col_names, bool full) {
  if (op == 'I') {
    return BuildInsert(schema, table, col_names);
  }
  if (op == 'D') {
    return BuildDelete(schema, table, key_names, full);
  }
  return BuildUpdate(schema, table, key_names, col_names, full);
}

duckdb::unique_ptr<duckdb::SQLStatement> BuildTruncate(std::string_view schema,
                                                       std::string_view table) {
  auto node = duckdb::make_uniq<duckdb::DeleteQueryNode>();
  node->table = TargetTable(schema, table);
  node->is_truncate = true;  // TRUNCATE, not per-row DELETE
  auto stmt = duckdb::make_uniq<duckdb::DeleteStatement>();
  stmt->node = std::move(node);
  return stmt;
}

duckdb::unique_ptr<duckdb::SQLStatement> BuildCopyFromStdin(
  std::string_view schema, std::string_view table,
  const std::vector<std::string>& columns, bool binary) {
  auto info = duckdb::make_uniq<duckdb::CopyInfo>();
  info->SetQualifiedName(duckdb::Identifier{}, duckdb::Identifier{schema},
                         duckdb::Identifier{table});
  info->select_list.reserve(columns.size());
  for (const auto& col : columns) {
    info->select_list.emplace_back(col);
  }
  info->is_from = true;
  info->file_path = "/dev/stdin";
  info->format = binary ? "binary" : "text";
  info->is_format_auto_detected = false;
  auto stmt = duckdb::make_uniq<duckdb::CopyStatement>();
  stmt->info = std::move(info);
  return stmt;
}

void RegisterReplicationSourceFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  duckdb::TableFunction func("repl_src", {}, ScanReplSource, BindReplSource,
                             InitGlobalReplSource);
  loader.RegisterFunction(func);
}

}  // namespace sdb::replication
