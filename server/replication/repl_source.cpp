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
#include <duckdb/parser/tableref/joinref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <stdexcept>
#include <utility>

#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/deserialize.h"
#include "replication/repl_stream.h"

namespace sdb::replication {
namespace {

std::vector<PgColumn> ReadTuple(std::string_view tuple) {
  PgTupleReader reader{tuple};
  std::vector<PgColumn> cols;
  cols.reserve(reader.Count());
  while (reader.HasNext()) {
    cols.push_back(reader.Next());
  }
  return cols;
}

struct ColDeser {
  sdb::pg::DeserializationFunction<sdb::pg::VectorSink> text = nullptr;
  sdb::pg::DeserializationFunction<sdb::pg::VectorSink> bin = nullptr;
};

void DecodeCell(sdb::pg::DeserializeContext& dctx, duckdb::Vector& vec,
                duckdb::idx_t row, const PgColumn& col, const ColDeser& d,
                size_t column) {
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
      break;
  }
  if (col.kind == TupleColKind::Binary) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
      ERR_MSG("incorrect binary data format in logical replication column ",
              column + 1));
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
    ERR_MSG("invalid input syntax in logical replication column ", column + 1));
}

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

duckdb::unique_ptr<duckdb::ParsedExpression> Column(std::string_view name,
                                                    std::string_view table) {
  return duckdb::make_uniq<duckdb::ColumnRefExpression>(
    duckdb::Identifier{name}, duckdb::Identifier{table});
}

duckdb::unique_ptr<duckdb::ParsedExpression> Match(
  const std::vector<std::string>& keys, const std::vector<std::string>& sources,
  std::string_view target, bool full) {
  const auto cmp = full ? duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM
                        : duckdb::ExpressionType::COMPARE_EQUAL;
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> terms;
  for (size_t i = 0; i < keys.size(); ++i) {
    terms.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
      cmp, Column(keys[i], target), Column(sources[i], "src")));
  }
  if (terms.size() == 1) {
    return std::move(terms.front());
  }
  return duckdb::make_uniq<duckdb::ConjunctionExpression>(
    duckdb::ExpressionType::CONJUNCTION_AND, std::move(terms));
}

duckdb::unique_ptr<duckdb::TableRef> FirstMatches(
  std::string_view schema, std::string_view table,
  const std::vector<std::string>& keys,
  const std::vector<std::string>& aliases) {
  auto scan = TargetTable(schema, table);
  scan->alias = "t";
  auto join = duckdb::make_uniq<duckdb::JoinRef>();
  join->left = std::move(scan);
  join->right = MakeReplSrc(aliases);
  join->condition = Match(keys, aliases, "t", true);

  auto select = duckdb::make_uniq<duckdb::SelectNode>();
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> rowid;
  rowid.push_back(Column("rowid", "t"));
  auto first = duckdb::make_uniq<duckdb::FunctionExpression>(
    duckdb::Identifier{"min"}, std::move(rowid));
  first->SetAlias(duckdb::Identifier{"rid"});
  select->select_list.push_back(std::move(first));
  select->select_list.push_back(
    duckdb::make_uniq<duckdb::StarExpression>(duckdb::Identifier{"src"}));
  select->from_table = std::move(join);
  select->aggregate_handling = duckdb::AggregateHandling::FORCE_AGGREGATES;

  auto statement = duckdb::make_uniq<duckdb::SelectStatement>();
  statement->node = std::move(select);
  return duckdb::make_uniq<duckdb::SubqueryRef>(std::move(statement),
                                                duckdb::Identifier{"src"});
}

duckdb::unique_ptr<duckdb::ParsedExpression> SameRow() {
  return duckdb::make_uniq<duckdb::ComparisonExpression>(
    duckdb::ExpressionType::COMPARE_EQUAL, Column("rowid", "tgt"),
    Column("rid", "src"));
}

duckdb::unique_ptr<duckdb::SQLStatement> BuildDelete(
  std::string_view schema, std::string_view table,
  const std::vector<std::string>& keys, bool full) {
  auto node = duckdb::make_uniq<duckdb::DeleteQueryNode>();
  node->table = TargetTable(schema, table);
  if (full) {
    node->using_clauses.push_back(FirstMatches(schema, table, keys, keys));
    node->condition = SameRow();
  } else {
    node->using_clauses.push_back(MakeReplSrc(keys));
    node->condition = Match(keys, keys, "tgt", false);
  }

  auto stmt = duckdb::make_uniq<duckdb::DeleteStatement>();
  stmt->node = std::move(node);
  return stmt;
}

duckdb::unique_ptr<duckdb::SQLStatement> BuildUpdate(
  std::string_view schema, std::string_view table,
  const std::vector<std::string>& keys, const std::vector<std::string>& sets,
  bool full) {
  std::vector<std::string> key_aliases;
  std::vector<std::string> aliases;
  key_aliases.reserve(keys.size());
  aliases.reserve(keys.size() + sets.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    key_aliases.push_back("k" + std::to_string(i));
    aliases.push_back(key_aliases.back());
  }
  for (size_t j = 0; j < sets.size(); ++j) {
    aliases.push_back("v" + std::to_string(j));
  }

  auto node = duckdb::make_uniq<duckdb::UpdateQueryNode>();
  node->table = TargetTable(schema, table);
  node->from_table =
    full ? FirstMatches(schema, table, keys, aliases) : MakeReplSrc(aliases);

  auto set_info = duckdb::make_uniq<duckdb::UpdateSetInfo>();
  for (size_t j = 0; j < sets.size(); ++j) {
    set_info->columns.push_back(duckdb::Identifier{sets[j]});
    set_info->expressions.push_back(Column("v" + std::to_string(j), "src"));
  }
  set_info->condition =
    full ? SameRow() : Match(keys, key_aliases, "tgt", false);
  node->set_info = std::move(set_info);

  auto stmt = duckdb::make_uniq<duckdb::UpdateStatement>();
  stmt->node = std::move(node);
  return stmt;
}

void AppendKey(std::string& key, const ReplBatch& batch,
               const std::vector<PgColumn>& cells) {
  for (const auto i : batch.keys) {
    const auto& cell = cells[i];
    key.push_back(static_cast<char>(cell.kind));
    const auto size = static_cast<uint32_t>(cell.data.size());
    key.append(reinterpret_cast<const char*>(&size), sizeof(size));
    key.append(cell.data);
  }
}

bool DecodeRow(ReplBatch& batch, const PgOutputMessage& msg,
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
    return false;
  }
  if (op == 'D') {
    std::string key;
    AppendKey(key, batch, cells);
    if (!batch.touched.insert(std::move(key)).second) {
      return false;
    }
  }
  if (op == 'I') {
    for (size_t j = 0; j < batch.cols.size(); ++j) {
      DecodeCell(dctx, output.data[j], row, cells[batch.cols[j]], deser[j],
                 batch.cols[j]);
    }
  } else if (op == 'D') {
    for (size_t j = 0; j < batch.keys.size(); ++j) {
      DecodeCell(dctx, output.data[j], row, cells[batch.keys[j]], deser[j],
                 batch.keys[j]);
    }
  } else {
    const auto& upd = std::get<UpdateMessage>(msg);
    std::vector<PgColumn> old_cells;
    if (upd.has_old) {
      old_cells = ReadTuple(upd.old_tuple);
      if (old_cells.size() != cells.size()) {
        return false;
      }
    }
    const auto& key_cells = upd.has_old ? old_cells : cells;
    std::string old_key;
    AppendKey(old_key, batch, key_cells);
    std::string new_key;
    AppendKey(new_key, batch, cells);
    if (batch.touched.contains(old_key) || batch.touched.contains(new_key)) {
      return false;
    }
    batch.touched.insert(std::move(old_key));
    batch.touched.insert(std::move(new_key));
    const size_t nkeys = batch.keys.size();
    for (size_t j = 0; j < nkeys; ++j) {
      DecodeCell(dctx, output.data[j], row, key_cells[batch.keys[j]], deser[j],
                 batch.keys[j]);
    }
    for (size_t k = 0; k < batch.cols.size(); ++k) {
      DecodeCell(dctx, output.data[nkeys + k], row, cells[batch.cols[k]],
                 deser[nkeys + k], batch.cols[k]);
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
  sdb::pg::DeserializeContext dctx;
  std::vector<ColDeser> deser;
};

duckdb::unique_ptr<duckdb::FunctionData> BindReplSource(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  auto* state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey)
      .get();
  if (state == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("repl_src requires a serenedb connection"));
  }
  const ReplBatch* batch =
    state->GetConnectionContext().GetSideChannel<ReplBatch>();
  if (batch == nullptr || batch->rel == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("repl_src: no active replication batch"));
  }
  const auto emit = [&](size_t idx) {
    return_types.push_back(batch->rel->columns[idx].type);
    names.emplace_back(batch->rel->columns[idx].name);
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
  result->batch = state->GetConnectionContext().GetSideChannel<ReplBatch>();
  sdb::pg::FillDeserializeContext(context, result->dctx);
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
  duckdb::idx_t row = 0;
  while (row < STANDARD_VECTOR_SIZE) {
    const PgOutputMessage* m = g.batch->stream->PeekBlocking();
    if (m == nullptr || ReplStream::IsIdle(m)) {
      break;
    }
    if (std::holds_alternative<BeginMessage>(*m) ||
        std::holds_alternative<CommitMessage>(*m)) {
      if (!g.batch->pass_through || !g.batch->pass_through(*m)) {
        break;
      }
      g.batch->stream->Advance();
      continue;
    }
    if (!DecodeRow(*g.batch, *m, g.deser, g.dctx, output, row)) {
      break;
    }
    g.batch->stream->Advance();
    ++g.batch->rows;
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
    if (m->old_is_key) {
      for (size_t i = 0; i < rel.columns.size(); ++i) {
        if (rel.columns[i].is_key) {
          keys.push_back(i);
        }
      }
    } else {
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
    for (size_t i = 0; i < rel.columns.size(); ++i) {
      if (cells[i].kind != TupleColKind::Unchanged) {
        cols.push_back(i);
      }
    }
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
      if (!m->has_old) {
        const auto changed = std::ranges::count_if(
          cols, [&](size_t i) { return !rel.columns[i].is_key; });
        if (changed != 0) {
          std::erase_if(cols, [&](size_t i) { return rel.columns[i].is_key; });
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

duckdb::unique_ptr<duckdb::SQLStatement> BuildTruncate(
  std::string_view schema, std::string_view table,
  std::span<const std::pair<std::string_view, std::string_view>> group) {
  auto node = duckdb::make_uniq<duckdb::DeleteQueryNode>();
  node->table = TargetTable(schema, table);
  node->is_truncate = true;
  for (const auto& [group_schema, group_table] : group) {
    auto ref = TargetTable(group_schema, group_table);
    ref->alias = duckdb::Identifier{};
    node->truncate_group.push_back(std::move(ref));
  }
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
  duckdb::TableFunction func("repl_src", duckdb::FunctionSignature{},
                             ScanReplSource, BindReplSource,
                             InitGlobalReplSource);
  loader.RegisterFunction(func);
}

}  // namespace sdb::replication
