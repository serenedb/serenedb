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

#include "docs/docs_functions.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/replacement_scan.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <iterator>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "docs/docs_search.h"

namespace sdb::docs {
namespace {

constexpr size_t kSnippetChars = 400;
constexpr size_t kSnippetBatch = 32;

enum class Column : uint8_t {
  Path,
  Title,
  Breadcrumb,
  Depth,
  Content,
  ContentText,
  Markdown,
  Snippet,
  Score,
  Kind,
  Name,
  Signature,
  Summary,
  Aliases,
  Page,
  Category,
};

std::string_view ColumnName(Column column) {
  switch (column) {
    case Column::Path:
      return "path";
    case Column::Title:
      return "title";
    case Column::Breadcrumb:
      return "breadcrumb";
    case Column::Depth:
      return "depth";
    case Column::Content:
      return "content";
    case Column::ContentText:
      return "content_text";
    case Column::Markdown:
      return "markdown";
    case Column::Snippet:
      return "snippet";
    case Column::Score:
      return "score";
    case Column::Kind:
      return "kind";
    case Column::Name:
      return "name";
    case Column::Signature:
      return "signature";
    case Column::Summary:
      return "summary";
    case Column::Aliases:
      return "aliases";
    case Column::Page:
      return "page";
    case Column::Category:
      return "category";
  }
  SDB_UNREACHABLE();
}

duckdb::LogicalType ColumnType(Column column) {
  switch (column) {
    case Column::Depth:
      return duckdb::LogicalType::INTEGER;
    case Column::Score:
      return duckdb::LogicalType::DOUBLE;
    default:
      return duckdb::LogicalType::VARCHAR;
  }
}

constexpr std::array kDocsColumns{
  Column::Path,    Column::Title,       Column::Breadcrumb, Column::Depth,
  Column::Content, Column::ContentText, Column::Markdown};
constexpr std::array kObjectsColumns{
  Column::Kind,    Column::Name,     Column::Signature,
  Column::Summary, Column::Aliases,  Column::Path,
  Column::Page,    Column::Category, Column::Breadcrumb};
constexpr std::array kSearchColumns{Column::Path, Column::Title,
                                    Column::Breadcrumb, Column::Snippet,
                                    Column::Score};
constexpr std::array kObjectLookupColumns{Column::Path, Column::Title,
                                          Column::Breadcrumb, Column::Kind};

constexpr std::array kSearchArguments{duckdb::LogicalTypeId::VARCHAR,
                                      duckdb::LogicalTypeId::BIGINT};
constexpr std::array kObjectLookupArguments{duckdb::LogicalTypeId::VARCHAR,
                                            duckdb::LogicalTypeId::VARCHAR};

struct Args {
  std::string text;
  size_t limit = std::numeric_limits<size_t>::max();
  std::string kind;

  bool operator==(const Args&) const = default;
};

struct Claim {
  std::vector<std::string> paths;
  std::optional<std::string> prefix;

  bool operator==(const Claim&) const = default;
};

using Row = std::variant<Entry, Object>;

template<typename T>
std::vector<Row> Rows(std::vector<T> items) {
  return {std::make_move_iterator(items.begin()),
          std::make_move_iterator(items.end())};
}

std::vector<Row> ProduceDocs(duckdb::DatabaseInstance& db, const Args&,
                             const Claim& claim, Columns columns) {
  if (claim.paths.empty()) {
    return Rows(ListPrefix(db, claim.prefix.value_or(""), false, columns));
  }
  auto paths = claim.paths;
  absl::c_sort(paths);
  paths.erase(std::unique(paths.begin(), paths.end()), paths.end());
  std::vector<Row> rows;
  for (const auto& path : paths) {
    if (auto entry = EntryAt(db, path, columns)) {
      rows.emplace_back(std::move(*entry));
    }
  }
  return rows;
}

std::vector<Row> ProduceObjects(duckdb::DatabaseInstance& db, const Args&,
                                const Claim&, Columns) {
  return Rows(Objects(db));
}

std::vector<Row> ProduceSearch(duckdb::DatabaseInstance& db, const Args& args,
                               const Claim&, Columns) {
  std::string error;
  auto hits = Search(db, args.text, args.limit, Columns{}, error);
  if (!error.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("sdb_docs.search: ", error));
  }
  return Rows(std::move(hits));
}

std::vector<Row> ProduceObject(duckdb::DatabaseInstance& db, const Args& args,
                               const Claim&, Columns) {
  if (auto objects = FindObjects(db, args.text, args.kind); !objects.empty()) {
    return Rows(std::move(objects));
  }
  if (!absl::StripAsciiWhitespace(args.kind).empty()) {
    return {};
  }
  return Rows(Lookup(db, args.text));
}

using Produce = std::vector<Row> (*)(duckdb::DatabaseInstance&, const Args&,
                                     const Claim&, Columns);

struct Table {
  std::string_view name;
  std::span<const Column> columns;
  std::span<const duckdb::LogicalTypeId> arguments;
  bool path_claims = false;
  Produce produce = nullptr;
};

constexpr std::array kTables{
  Table{.name = "docs",
        .columns = kDocsColumns,
        .path_claims = true,
        .produce = ProduceDocs},
  Table{
    .name = "objects", .columns = kObjectsColumns, .produce = ProduceObjects},
  Table{.name = "search",
        .columns = kSearchColumns,
        .arguments = kSearchArguments,
        .produce = ProduceSearch},
  Table{.name = "object",
        .columns = kObjectLookupColumns,
        .arguments = kObjectLookupArguments,
        .produce = ProduceObject},
};

struct TableInfo final : duckdb::TableFunctionInfo {
  explicit TableInfo(const Table& table) : table{table} {}

  const Table& table;
};

struct BindData final : duckdb::TableFunctionData {
  const Table* table = nullptr;
  Args args;
  Claim claim;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    auto copy = duckdb::make_uniq<BindData>();
    copy->table = table;
    copy->args = args;
    copy->claim = claim;
    return std::move(copy);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    const auto& rhs = other.Cast<BindData>();
    return table == rhs.table && args == rhs.args && claim == rhs.claim;
  }
};

struct State final : duckdb::GlobalTableFunctionState {
  std::vector<Column> columns;
  std::vector<Row> rows;
  size_t offset = 0;
  size_t batch = STANDARD_VECTOR_SIZE;
};

duckdb::Value Text(std::string_view text) {
  return duckdb::Value{std::string{text}};
}

duckdb::Value Nullable(const std::optional<std::string>& text) {
  return text ? duckdb::Value{*text}
              : duckdb::Value{duckdb::LogicalType::VARCHAR};
}

duckdb::Value ObjectCell(const Object& object, Column column) {
  switch (column) {
    case Column::Kind:
      return Text(object.kind);
    case Column::Name:
      return Text(object.name);
    case Column::Title:
    case Column::Signature:
      return Text(object.signature);
    case Column::Summary:
      return Nullable(object.summary);
    case Column::Aliases:
      return Nullable(object.aliases);
    case Column::Path:
      return Text(object.path);
    case Column::Page:
      return Text(object.page);
    case Column::Category:
      return Nullable(object.category);
    case Column::Breadcrumb:
      return Text(object.breadcrumb);
    default:
      SDB_UNREACHABLE();
  }
}

duckdb::Value EntryCell(duckdb::DatabaseInstance& db, const Entry& entry,
                        Column column) {
  switch (column) {
    case Column::Path:
      return Text(entry.path);
    case Column::Title:
      return Text(entry.title);
    case Column::Breadcrumb:
      return Text(entry.breadcrumb);
    case Column::Depth:
      return duckdb::Value::INTEGER(
        static_cast<int32_t>(HeadingDepth(entry.path)));
    case Column::Content:
      return Text(entry.content);
    case Column::ContentText:
      return Text(entry.content_text);
    case Column::Markdown:
      return Text(Markdown(entry));
    case Column::Snippet: {
      const auto full = EntryAt(db, entry.path, Columns{.content_text = true});
      return Text(Snippet(full ? full->content_text : "", kSnippetChars));
    }
    case Column::Score:
      return duckdb::Value::DOUBLE(entry.score);
    case Column::Kind:
      return duckdb::Value{duckdb::LogicalType::VARCHAR};
    default:
      SDB_UNREACHABLE();
  }
}

duckdb::Value Cell(duckdb::DatabaseInstance& db, const Row& row,
                   Column column) {
  if (const auto* object = std::get_if<Object>(&row)) {
    return ObjectCell(*object, column);
  }
  return EntryCell(db, std::get<Entry>(row), column);
}

Columns Needed(std::span<const Column> columns) {
  const auto wants = [&](Column column) {
    return absl::c_linear_search(columns, column);
  };
  return {.content = wants(Column::Content) || wants(Column::Markdown),
          .content_text = wants(Column::ContentText)};
}

duckdb::unique_ptr<duckdb::FunctionData> Bind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto bind = duckdb::make_uniq<BindData>();
  bind->table = &input.info->Cast<TableInfo>().table;
  const auto& table = *bind->table;
  for (const auto column : table.columns) {
    names.emplace_back(ColumnName(column));
    return_types.push_back(ColumnType(column));
  }
  const auto& inputs = input.inputs;
  if (!inputs.empty()) {
    if (inputs[0].IsNull()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("sdb_docs.", table.name, " argument must not be NULL"));
    }
    bind->args.text = duckdb::StringValue::Get(inputs[0]);
  }
  if (inputs.size() > 1 && !inputs[1].IsNull()) {
    if (table.arguments[1] == duckdb::LogicalTypeId::BIGINT) {
      bind->args.limit = static_cast<size_t>(
        std::max<int64_t>(inputs[1].GetValue<int64_t>(), 0));
    } else {
      bind->args.kind = duckdb::StringValue::Get(inputs[1]);
    }
  }
  return std::move(bind);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind = input.bind_data->Cast<BindData>();
  const auto& table = *bind.table;
  auto state = duckdb::make_uniq<State>();
  for (const auto id : input.column_ids) {
    SDB_ASSERT(id < table.columns.size());
    state->columns.push_back(table.columns[id]);
  }
  if (absl::c_linear_search(state->columns, Column::Snippet)) {
    state->batch = kSnippetBatch;
  }
  state->rows = table.produce(duckdb::DatabaseInstance::GetDatabase(context),
                              bind.args, bind.claim, Needed(state->columns));
  return std::move(state);
}

void Execute(duckdb::ClientContext& context, duckdb::TableFunctionInput& input,
             duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<State>();
  auto& db = duckdb::DatabaseInstance::GetDatabase(context);
  const auto n = std::min(state.batch, state.rows.size() - state.offset);
  output.SetChildCardinality(n);
  for (size_t column = 0; column < state.columns.size(); ++column) {
    for (size_t i = 0; i < n; ++i) {
      output.data[column].SetValue(
        i, Cell(db, state.rows[state.offset + i], state.columns[column]));
    }
  }
  state.offset += n;
}

std::optional<std::string> TextConstant(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return std::nullopt;
  }
  const auto& value = expr.Cast<duckdb::BoundConstantExpression>().GetValue();
  if (value.IsNull() || value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return std::nullopt;
  }
  return duckdb::StringValue::Get(value);
}

bool IsPath(const duckdb::LogicalGet& get, const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return false;
  }
  const auto& binding = expr.Cast<duckdb::BoundColumnRefExpression>().Binding();
  const auto& ids = get.GetColumnIds();
  if (binding.table_index != get.table_index ||
      binding.column_index >= ids.size()) {
    return false;
  }
  const auto index = ids[binding.column_index].GetPrimaryIndex();
  return index < kDocsColumns.size() && kDocsColumns[index] == Column::Path;
}

std::optional<std::vector<std::string>> EqualPaths(
  const duckdb::LogicalGet& get, const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION ||
      expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_EQUAL) {
    return std::nullopt;
  }
  const auto& comparison = expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& left = duckdb::BoundComparisonExpression::Left(comparison);
  const auto& right = duckdb::BoundComparisonExpression::Right(comparison);
  const auto value = IsPath(get, left)    ? TextConstant(right)
                     : IsPath(get, right) ? TextConstant(left)
                                          : std::nullopt;
  if (!value) {
    return std::nullopt;
  }
  return std::vector{*value};
}

std::optional<std::vector<std::string>> AnyPath(
  const duckdb::LogicalGet& get, const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_OPERATOR ||
      expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_IN) {
    return std::nullopt;
  }
  const auto& children =
    expr.Cast<duckdb::BoundOperatorExpression>().GetChildren();
  if (children.size() < 2 || !IsPath(get, *children[0])) {
    return std::nullopt;
  }
  std::vector<std::string> paths;
  for (size_t i = 1; i < children.size(); ++i) {
    auto value = TextConstant(*children[i]);
    if (!value) {
      return std::nullopt;
    }
    paths.push_back(std::move(*value));
  }
  return paths;
}

std::optional<std::string> PathPrefix(const duckdb::LogicalGet& get,
                                      const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return std::nullopt;
  }
  const auto& function = expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& name = function.Function().GetName().GetIdentifierName();
  const auto& children = function.GetChildren();
  if ((name != "prefix" && name != "starts_with") || children.size() != 2 ||
      !IsPath(get, *children[0])) {
    return std::nullopt;
  }
  return TextConstant(*children[1]);
}

template<typename Match>
auto Find(Match match, const duckdb::LogicalGet& get,
          const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters)
  -> decltype(match(get, *filters.front())) {
  for (const auto& filter : filters) {
    if (auto value = match(get, *filter)) {
      return value;
    }
  }
  return std::nullopt;
}

void Pushdown(duckdb::ClientContext&, duckdb::LogicalGet& get,
              duckdb::FunctionData* bind_data,
              duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  auto& claim = bind_data->Cast<BindData>().claim;
  if (!claim.paths.empty() || claim.prefix) {
    return;
  }
  if (auto paths = Find(EqualPaths, get, filters)) {
    claim.paths = std::move(*paths);
  } else if (auto any = Find(AnyPath, get, filters)) {
    claim.paths = std::move(*any);
  } else {
    claim.prefix = Find(PathPrefix, get, filters);
  }
}

duckdb::unique_ptr<duckdb::TableRef> ReplaceDocsTable(
  duckdb::ClientContext&, duckdb::ReplacementScanInput& input,
  duckdb::optional_ptr<duckdb::ReplacementScanData>) {
  if (!absl::EqualsIgnoreCase(input.schema_name,
                              irs::StaticStrings::kDocsSchema)) {
    return nullptr;
  }
  const auto table = absl::c_find_if(kTables, [&](const Table& candidate) {
    return candidate.arguments.empty() &&
           absl::EqualsIgnoreCase(candidate.name, input.table_name);
  });
  if (table == kTables.end()) {
    return nullptr;
  }
  auto ref = duckdb::make_uniq<duckdb::TableFunctionRef>();
  ref->function = duckdb::make_uniq<duckdb::FunctionExpression>(
    duckdb::QualifiedName{
      duckdb::Identifier::SystemCatalog(),
      duckdb::Identifier{std::string_view{irs::StaticStrings::kDocsSchema}},
      duckdb::Identifier{table->name}},
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>{});
  return std::move(ref);
}

}  // namespace

void RegisterDocsFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.UseDedicatedSchemaForExtension(
    duckdb::Identifier{std::string_view{irs::StaticStrings::kDocsSchema}});
  for (const auto& table : kTables) {
    duckdb::TableFunctionSet set{duckdb::Identifier{table.name}};
    for (auto arity = std::min<size_t>(table.arguments.size(), 1);
         arity <= table.arguments.size(); ++arity) {
      duckdb::vector<duckdb::LogicalType> arguments;
      for (const auto id : table.arguments.first(arity)) {
        arguments.emplace_back(id);
      }
      duckdb::TableFunction function{duckdb::Identifier{table.name},
                                     std::move(arguments), Execute, Bind, Init};
      function.projection_pushdown = true;
      if (table.path_claims) {
        function.pushdown_complex_filter = Pushdown;
      }
      function.function_info = duckdb::make_shared_ptr<TableInfo>(table);
      set.AddFunction(std::move(function));
    }
    loader.RegisterFunction(std::move(set));
  }
  duckdb::DBConfig::GetConfig(db).replacement_scans.emplace_back(
    ReplaceDocsTable);
}

}  // namespace sdb::docs
