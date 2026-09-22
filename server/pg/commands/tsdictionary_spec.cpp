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

#include "pg/commands/tsdictionary_spec.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/enums/expression_type.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/lambda_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/expression/parameter_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression_binder/constant_binder.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/tokenizer.h"
#include "pg/commands/create_tsdictionary.h"
#include "pg/option_help.h"
#include "pg/tokenizer_options.h"

namespace sdb::pg {
namespace {

constexpr std::string_view kOperation = "CREATE TEXT SEARCH DICTIONARY";
constexpr std::string_view kInput = "input";
constexpr std::string_view kPipe = "|";
constexpr std::string_view kListValue = "list_value";
constexpr std::string_view kMainSchema = "main";

constexpr std::string_view kPipelineName =
  irs::analysis::PipelineTokenizer::type_name();
constexpr std::string_view kUnionName =
  irs::analysis::UnionTokenizer::type_name();
constexpr std::string_view kSqlName = irs::analysis::SqlTokenizer::type_name();
constexpr std::string_view kKeywordName = irs::KeywordTokenizer::type_name();

struct Stage;
using Chain = std::vector<Stage>;

struct Stage {
  enum class Kind : uint8_t {
    Template,
    Sql,
    Dictionary,
    Identity,
  };

  Kind kind;
  std::string name;
  std::vector<std::pair<std::string, duckdb::Value>> options;
  std::vector<Chain> children;
};

const duckdb::FunctionExpression* AsFunction(
  const duckdb::ParsedExpression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::FUNCTION) {
    return nullptr;
  }
  return &expr.Cast<duckdb::FunctionExpression>();
}

const duckdb::FunctionExpression* AsPlainCall(
  const duckdb::ParsedExpression& expr) {
  const auto* fn = AsFunction(expr);
  if (!fn || fn->IsOperator()) {
    return nullptr;
  }
  const auto& qualified = fn->GetQualifiedName();
  if (!qualified.Catalog().empty() || !qualified.Schema().empty()) {
    return nullptr;
  }
  return fn;
}

std::string LowerName(const duckdb::FunctionExpression& fn) {
  return absl::AsciiStrToLower(fn.FunctionName().GetIdentifierName());
}

bool IsPipe(const duckdb::ParsedExpression& expr) {
  const auto* fn = AsFunction(expr);
  return fn && fn->IsOperator() && fn->GetArguments().size() == 2 &&
         fn->FunctionName().GetIdentifierName() == kPipe;
}

const duckdb::FunctionExpression* AsCall(const duckdb::ParsedExpression& expr) {
  const auto* fn = AsFunction(expr);
  return fn && !fn->IsOperator() ? fn : nullptr;
}

bool IsInputColumn(const duckdb::ParsedExpression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::COLUMN_REF) {
    return false;
  }
  const auto& names = expr.Cast<duckdb::ColumnRefExpression>().ColumnNames();
  return names.size() == 1 &&
         absl::EqualsIgnoreCase(names.back().GetIdentifierName(), kInput);
}

void RejectPlaceholders(const duckdb::ParsedExpression& expr,
                        bool reject_input) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::PARAMETER) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("\"", expr.ToString(), "\" is not available in an SQL stage"),
      ERR_HINT("the value is passed as the first argument, so write "
               "lower(); name it with a lambda when it is not the first "
               "argument, as in (lambda x: nullif(x, 'skip'))"));
  }
  if (reject_input && IsInputColumn(expr)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("column \"", kInput, "\" is not available in an SQL stage"),
      ERR_HINT("the value is passed as the first argument, so write "
               "lower(); name it with a lambda when it is not the first "
               "argument, as in (lambda x: nullif(x, 'skip'))"));
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::ParsedExpression& child) {
      RejectPlaceholders(child, reject_input);
    });
}

bool ReferencesInput(const duckdb::ParsedExpression& expr) {
  if (IsInputColumn(expr)) {
    return true;
  }
  bool found = false;
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::ParsedExpression& child) {
      found = found || ReferencesInput(child);
    });
  return found;
}

bool IsUserTemplate(const OptionGroup& group) {
  return group.kind != TemplateKind::Features && group.name != kSqlName;
}

const OptionGroup* FindTemplate(std::string_view name) {
  for (const auto& group : tokenizer_options::kTokenizerSubgroups) {
    if (group.name == name && IsUserTemplate(group)) {
      return &group;
    }
  }
  return nullptr;
}

std::vector<std::string_view> TemplateNames() {
  std::vector<std::string_view> names;
  for (const auto& group : tokenizer_options::kTokenizerSubgroups) {
    if (IsUserTemplate(group)) {
      names.push_back(group.name);
    }
  }
  return names;
}

bool FunctionExists(duckdb::ClientContext& ctx,
                    const duckdb::Identifier& name) {
  const auto exists = [&](duckdb::CatalogType type) -> bool {
    const duckdb::EntryLookupInfo lookup{type, duckdb::QualifiedName{name}};
    return duckdb::Catalog::GetEntry(ctx, lookup,
                                     duckdb::OnEntryNotFound::RETURN_NULL);
  };
  return exists(duckdb::CatalogType::SCALAR_FUNCTION_ENTRY) ||
         exists(duckdb::CatalogType::MACRO_ENTRY) ||
         exists(duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY);
}

const OptionGroup* AsTemplateCall(const duckdb::ParsedExpression& expr) {
  const auto* fn = AsPlainCall(expr);
  return fn ? FindTemplate(LowerName(*fn)) : nullptr;
}

std::optional<std::vector<const duckdb::ParsedExpression*>> AsListLiteral(
  const duckdb::ParsedExpression& expr) {
  std::vector<const duckdb::ParsedExpression*> elements;
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::OPERATOR &&
      expr.GetExpressionType() == duckdb::ExpressionType::ARRAY_CONSTRUCTOR) {
    for (const auto& child :
         expr.Cast<duckdb::OperatorExpression>().GetChildren()) {
      elements.push_back(child.get());
    }
    return elements;
  }
  const auto* fn = AsFunction(expr);
  if (!fn || fn->IsOperator() || LowerName(*fn) != kListValue) {
    return std::nullopt;
  }
  const auto& qualified = fn->GetQualifiedName();
  if (!qualified.Catalog().empty() ||
      (!qualified.Schema().empty() &&
       qualified.Schema().GetIdentifierName() != kMainSchema)) {
    return std::nullopt;
  }
  for (const auto& arg : fn->GetArguments()) {
    if (arg.HasName()) {
      return std::nullopt;
    }
    elements.push_back(&arg.GetExpression());
  }
  return elements;
}

bool IsStageExpression(const duckdb::ParsedExpression& expr);

bool IsStructuralAnalyzer(const duckdb::ParsedExpression& expr) {
  if (IsPipe(expr) || AsTemplateCall(expr) ||
      expr.GetExpressionClass() == duckdb::ExpressionClass::LAMBDA ||
      expr.GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF) {
    return true;
  }
  const auto elements = AsListLiteral(expr);
  return elements && !elements->empty() &&
         absl::c_all_of(*elements, [](const duckdb::ParsedExpression* e) {
           return IsStageExpression(*e);
         });
}

// A stage is a template call, an SQL function call, a lambda, a union list or
// a stored dictionary name; only the first two are plain calls.
bool IsStageExpression(const duckdb::ParsedExpression& expr) {
  return IsStructuralAnalyzer(expr) || AsCall(expr);
}

std::string SuggestionHint(std::span<const std::string_view> known,
                           std::string_view name) {
  const auto closest = FindClosestName(known, name);
  if (!closest.empty()) {
    return absl::StrCat("did you mean \"", closest, "\"?");
  }
  return absl::StrCat("known names: ", absl::StrJoin(known, ", "));
}

void ThrowNeedsBase(const OptionGroup& group) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_SYNTAX_ERROR),
    ERR_MSG(group.name, "() requires a nested analyzer as its first argument"),
    ERR_HINT(group.name, "(split_text(...), ...)"));
}

void RejectNestedAnalyzers(const duckdb::ParsedExpression& expr) {
  if (const auto* group = AsTemplateCall(expr)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("\"", group->name,
              "\" is a text search template and cannot be nested inside "
              "an SQL expression"),
      ERR_HINT("chain analyzers with |, or call the SQL function as main.",
               group->name, "(...)"));
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expr, [](const duckdb::ParsedExpression& child) {
      if (IsPipe(child)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG("analyzers cannot be nested inside an SQL expression"),
          ERR_HINT("chain analyzers with |"));
      }
      RejectNestedAnalyzers(child);
    });
}

struct BuildContext {
  duckdb::ClientContext& context;
};

irs::analysis::TokenizerConfig BuildChainConfig(const Chain& chain,
                                                const BuildContext& ctx);

irs::analysis::TokenizerConfig BuildStageConfig(const Stage& stage,
                                                const BuildContext& ctx) {
  Options options;
  const auto put = [&](std::string_view name, duckdb::Value value) {
    options.try_emplace(std::string{name},
                        std::make_unique<duckdb::Value>(std::move(value)));
  };
  std::string_view type;
  switch (stage.kind) {
    case Stage::Kind::Identity:
      type = kKeywordName;
      break;
    case Stage::Kind::Dictionary:
      type = tokenizer_options::kDictionaryTemplate;
      put(tokenizer_options::kFrom.name, duckdb::Value{stage.name});
      break;
    case Stage::Kind::Sql:
      type = kSqlName;
      put(tokenizer_options::kSqlExpression.name, duckdb::Value{stage.name});
      break;
    case Stage::Kind::Template:
      type = stage.name;
      for (const auto& [name, value] : stage.options) {
        put(name, value);
      }
      break;
  }
  auto children = stage.children |
                  std::views::transform([&](const Chain& child) {
                    return std::make_unique<irs::analysis::TokenizerConfig>(
                      BuildChainConfig(child, ctx));
                  }) |
                  std::ranges::to<TokenizerConfigs>();
  return BuildStage(ctx.context, type, std::move(options), std::move(children),
                    kOperation);
}

irs::analysis::TokenizerConfig BuildChainConfig(const Chain& chain,
                                                const BuildContext& ctx) {
  if (chain.size() == 1) {
    return BuildStageConfig(chain.front(), ctx);
  }
  auto children = chain | std::views::transform([&](const Stage& stage) {
                    return std::make_unique<irs::analysis::TokenizerConfig>(
                      BuildStageConfig(stage, ctx));
                  }) |
                  std::ranges::to<TokenizerConfigs>();
  return BuildStage(ctx.context, kPipelineName, {}, std::move(children),
                    kOperation);
}

class SpecCompiler {
 public:
  explicit SpecCompiler(duckdb::ClientContext& context) : _context{context} {}

  irs::analysis::TokenizerConfig Compile(std::string_view spec) {
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> exprs;
    try {
      exprs = duckdb::Parser::ParseExpressionList(spec);
    } catch (const std::exception& e) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG(kOperation, ": ", e.what()));
    }
    if (exprs.size() != 1) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG(kOperation, ": expected one analyzer expression"));
    }
    return BuildChainConfig(CompileChain(*exprs[0]), {_context});
  }

 private:
  Chain CompileChain(const duckdb::ParsedExpression& expr) {
    if (!IsPipe(expr)) {
      Chain chain;
      chain.push_back(CompileStage(expr));
      return chain;
    }
    const auto& args = expr.Cast<duckdb::FunctionExpression>().GetArguments();
    auto chain = CompileChain(args[0].GetExpression());
    auto tail = CompileChain(args[1].GetExpression());
    chain.insert(chain.end(), std::make_move_iterator(tail.begin()),
                 std::make_move_iterator(tail.end()));
    return chain;
  }

  Stage CompileStage(const duckdb::ParsedExpression& expr) {
    if (expr.GetExpressionClass() == duckdb::ExpressionClass::LAMBDA) {
      return CompileLambda(expr.Cast<duckdb::LambdaExpression>());
    }
    if (const auto elements = AsListLiteral(expr)) {
      return CompileUnion(*elements);
    }
    if (expr.GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF) {
      return CompileDictionary(expr.Cast<duckdb::ColumnRefExpression>());
    }
    if (const auto* fn = AsCall(expr)) {
      if (AsPlainCall(expr)) {
        const auto name = LowerName(*fn);
        if (name == kPipelineName) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_SYNTAX_ERROR),
            ERR_MSG("pipeline() is not available in the expression form"),
            ERR_HINT("chain the stages with |: a | b"));
        }
        if (name == kUnionName) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_SYNTAX_ERROR),
            ERR_MSG("union() is not available in the expression form"),
            ERR_HINT("write the branches as a list: [a, b]"));
        }
        if (const auto* group = FindTemplate(name)) {
          return CompileTemplate(*fn, *group);
        }
      }
      return CompileSqlCall(*fn);
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("\"", expr.ToString(), "\" is not a stage"),
      ERR_HINT("a stage is a template call, an SQL function call taking the "
               "value as its first argument, a lambda, a list of stages or a "
               "dictionary name; wrap anything else in a lambda, as in "
               "(lambda x: x || '!')"));
  }

  Stage CompileUnion(
    std::span<const duckdb::ParsedExpression* const> elements) {
    if (elements.empty() ||
        !absl::c_all_of(elements, [](const duckdb::ParsedExpression* e) {
          return IsStageExpression(*e);
        })) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("a list stage must hold analyzers: [split_text(...), "
                "generate_ngrams(...)]"));
    }
    Stage stage{.kind = Stage::Kind::Template, .name = std::string{kUnionName}};
    for (const auto* element : elements) {
      stage.children.emplace_back(CompileChain(*element));
    }
    return stage;
  }

  bool DictionaryExists(std::string_view name) {
    return static_cast<bool>(
      duckdb::Catalog::GetEntry<catalog::TokenizerCatalogEntry>(
        _context,
        duckdb::QualifiedName{duckdb::Identifier{}, duckdb::Identifier{},
                              duckdb::Identifier{name}},
        duckdb::OnEntryNotFound::RETURN_NULL));
  }

  [[noreturn]] void ThrowUnknownStage(const duckdb::FunctionExpression& fn) {
    const auto spelled = fn.GetQualifiedName().Name().GetIdentifierName();
    if (DictionaryExists(spelled)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("text search dictionary \"", spelled, "\" takes no arguments"),
        ERR_HINT("use it as a stage by name: ", spelled));
    }
    const auto closest = FindClosestName(TemplateNames(), LowerName(fn));
    if (closest.empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unknown stage \"", spelled, "\""));
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unknown stage \"", spelled, "\""),
                    ERR_HINT("did you mean \"", closest, "\"?"));
  }

  Stage CompileDictionary(const duckdb::ColumnRefExpression& ref) {
    const auto& names = ref.ColumnNames();
    const auto spelled = absl::StrJoin(
      names, ".", [](std::string* out, const duckdb::Identifier& id) {
        absl::StrAppend(out, id.GetIdentifierName());
      });
    if (names.size() > 2) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("invalid text search dictionary reference \"", spelled, "\""));
    }
    const auto tokenizer =
      duckdb::Catalog::GetEntry<catalog::TokenizerCatalogEntry>(
        _context,
        duckdb::QualifiedName{
          duckdb::Identifier{},
          names.size() == 2 ? names[0] : duckdb::Identifier{}, names.back()},
        duckdb::OnEntryNotFound::RETURN_NULL);
    if (!tokenizer) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("text search dictionary \"", spelled, "\" does not exist"));
    }
    return {.kind = Stage::Kind::Dictionary, .name = spelled};
  }

  Stage CompileLambda(const duckdb::LambdaExpression& lambda) {
    std::string error;
    const auto params = lambda.ExtractColumnRefExpressions(error);
    if (!error.empty() || params.size() != 1) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("a lambda stage takes exactly one parameter"),
        ERR_HINT(
          "write it in parentheses after |: ... | (lambda x: upper(x))"));
    }
    const auto& param =
      params.front().get().Cast<duckdb::ColumnRefExpression>().GetColumnName();
    auto body = lambda.Right().Copy();
    if (IsParameterRef(*body, param)) {
      return {.kind = Stage::Kind::Identity};
    }
    const bool param_is_input =
      absl::EqualsIgnoreCase(param.GetIdentifierName(), kInput);
    RejectPlaceholders(*body, /*reject_input=*/!param_is_input);
    SubstituteParameter(*body, param);
    if (!ReferencesInput(*body)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("lambda stage \"", lambda.ToString(),
                "\" does not use its parameter"),
        ERR_HINT("the parameter is the value being analyzed, as in "
                 "(lambda x: upper(x))"));
    }
    RejectNestedAnalyzers(*body);
    return {.kind = Stage::Kind::Sql, .name = body->ToString()};
  }

  static bool IsParameterRef(const duckdb::ParsedExpression& expr,
                             const duckdb::Identifier& param) {
    if (expr.GetExpressionClass() != duckdb::ExpressionClass::COLUMN_REF) {
      return false;
    }
    const auto& names = expr.Cast<duckdb::ColumnRefExpression>().ColumnNames();
    return names.size() == 1 &&
           absl::EqualsIgnoreCase(names[0].GetIdentifierName(),
                                  param.GetIdentifierName());
  }

  static void SubstituteParameter(duckdb::ParsedExpression& expr,
                                  const duckdb::Identifier& param) {
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      expr, [&](duckdb::unique_ptr<duckdb::ParsedExpression>& child) {
        if (IsParameterRef(*child, param)) {
          child = duckdb::make_uniq<duckdb::ColumnRefExpression>(
            duckdb::Identifier{std::string{kInput}});
          return;
        }
        SubstituteParameter(*child, param);
      });
  }

  // The value the stage analyzes is passed as the call's first argument.
  Stage CompileSqlCall(const duckdb::FunctionExpression& fn) {
    const auto& name = fn.GetQualifiedName().Name();
    RejectPlaceholders(fn, /*reject_input=*/true);
    RejectNestedAnalyzers(fn);
    if (AsPlainCall(fn) && (DictionaryExists(name.GetIdentifierName()) ||
                            !FunctionExists(_context, name))) {
      ThrowUnknownStage(fn);
    }
    auto copy = fn.Copy();
    auto& args = copy->Cast<duckdb::FunctionExpression>().GetArgumentsMutable();
    args.insert(args.begin(), duckdb::FunctionArgument{
                                duckdb::make_uniq<duckdb::ColumnRefExpression>(
                                  duckdb::Identifier{std::string{kInput}})});
    return {.kind = Stage::Kind::Sql, .name = copy->ToString()};
  }

  Stage CompileTemplate(const duckdb::FunctionExpression& fn,
                        const OptionGroup& group) {
    Stage stage{.kind = Stage::Kind::Template, .name = std::string{group.name}};
    const bool wrapper = group.kind == TemplateKind::Wrapper;
    const auto flat = group.FlatOptions();
    std::vector<std::string> given;
    size_t positional = 0;
    for (const auto& arg : fn.GetArguments()) {
      const auto& value = arg.GetExpression();
      if (arg.HasName()) {
        const auto name =
          absl::AsciiStrToLower(arg.GetName().GetIdentifierName());
        const bool known = absl::c_any_of(
          flat, [&](const OptionInfo& info) { return info.name == name; });
        if (!known) {
          const auto names = group.FlatNames();
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_SYNTAX_ERROR),
            ERR_MSG(group.name, "(): unknown option \"", name, "\""),
            ERR_HINT(SuggestionHint(names, name)));
        }
        AddOption(stage, group, name, value, given);
        continue;
      }
      if (wrapper && stage.children.empty() && positional == 0) {
        if (!IsStageExpression(value)) {
          ThrowNeedsBase(group);
        }
        stage.children.emplace_back(CompileChain(value));
        continue;
      }
      if (positional == flat.size()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG(group.name, "() takes at most ", flat.size(),
                                " positional arguments"));
      }
      AddOption(stage, group, flat[positional++].name, value, given);
    }
    if (wrapper && stage.children.empty()) {
      ThrowNeedsBase(group);
    }
    for (const auto& info : flat) {
      if (info.IsRequired() && !absl::c_linear_search(given, info.name)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG(group.name, "(): required option \"", info.name,
                                "\" not given"));
      }
    }
    return stage;
  }

  void AddOption(Stage& stage, const OptionGroup& group, std::string_view name,
                 const duckdb::ParsedExpression& value,
                 std::vector<std::string>& given) {
    if (absl::c_linear_search(given, name)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG(group.name, "(): option \"", name, "\" given more than once"));
    }
    given.emplace_back(name);
    if (value.GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF ||
        value.GetExpressionClass() == duckdb::ExpressionClass::PARAMETER) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG(group.name, "(): argument \"", value.ToString(),
                "\" is not a constant"),
        ERR_HINT("quote string values: '", value.ToString(), "'"));
    }
    if (IsStructuralAnalyzer(value)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG(group.name, "() does not take an analyzer argument"),
        ERR_HINT("chain analyzers with |: ... | ", group.name, "(...)"));
    }
    auto folded = Fold(group, name, value);
    if (folded.IsNull()) {
      return;
    }
    stage.options.emplace_back(std::string{name}, std::move(folded));
  }

  duckdb::Value Fold(const OptionGroup& group, std::string_view name,
                     const duckdb::ParsedExpression& value) {
    std::string error;
    try {
      auto binder = duckdb::Binder::CreateBinder(_context);
      duckdb::ConstantBinder constant_binder{*binder, _context,
                                             std::string{kOperation}};
      auto copy = value.Copy();
      auto bound = constant_binder.Bind(copy);
      return duckdb::ExpressionExecutor::EvaluateScalar(_context, *bound, true);
    } catch (const std::exception& e) {
      error = e.what();
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(group.name, "(): option \"", name, "\": ", error));
  }

  duckdb::ClientContext& _context;
};

}  // namespace

irs::analysis::TokenizerConfig CompileTSDictionarySpec(
  duckdb::ClientContext& context, std::string_view spec) {
  return SpecCompiler{context}.Compile(spec);
}

namespace {

std::string RenderDefault(const OptionInfo& info) {
  if (info.type == OptionInfo::Type::StringList) {
    return "[]";
  }
  return std::visit(
    [](const auto& value) -> std::string {
      using T = std::decay_t<decltype(value)>;
      if constexpr (std::is_same_v<T, std::string_view>) {
        return absl::StrCat("'", value, "'");
      } else if constexpr (std::is_same_v<T, bool>) {
        return value ? "true" : "false";
      } else if constexpr (std::is_same_v<T, char>) {
        return absl::StrCat("'", std::string_view{&value, 1}, "'");
      } else if constexpr (std::is_same_v<T, std::monostate>) {
        return {};
      } else {
        return absl::StrCat(value);
      }
    },
    info.default_value);
}

std::vector<std::string> Parameters(const OptionGroup& group) {
  std::vector<std::string> params;
  for (const auto& info : group.FlatOptions()) {
    if (info.IsRequired()) {
      params.emplace_back(info.name);
      continue;
    }
    params.push_back(absl::StrCat(info.name, " := ", RenderDefault(info)));
  }
  return params;
}

std::string Signature(const OptionGroup& group) {
  auto params = Parameters(group);
  if (group.kind == TemplateKind::Wrapper) {
    params.insert(params.begin(), "<analyzer>");
  }
  return absl::StrCat(group.name, "(", absl::StrJoin(params, ", "), ")");
}

std::string FunctionSignature(const OptionGroup& group) {
  auto params = Parameters(group);
  params.insert(params.begin(), "value");
  return absl::StrCat(group.function, "(", absl::StrJoin(params, ", "), ")");
}

void AppendDescriptions(std::string& out, std::span<const OptionInfo> options) {
  size_t width = 0;
  for (const auto& info : options) {
    width = std::max(width, info.name.size());
  }
  for (const auto& info : options) {
    absl::StrAppend(&out, "  ", info.name,
                    std::string(width - info.name.size() + 2, ' '),
                    info.description, "\n");
  }
}

}  // namespace

std::string FormatTSDictionaryHelp() {
  std::string out;
  absl::StrAppend(&out,
                  "CREATE TEXT SEARCH DICTIONARY <name> AS <analyzer> "
                  "[WITH (frequency, position, norm, offset)]\n",
                  "<analyzer> := <template>(<arguments>) | <analyzer> | "
                  "<analyzer> | [<analyzer>, ...] | <function>(<arguments>) | "
                  "(lambda <x>: <expression>) | <dictionary name>\n\n");
  for (const auto& group : tokenizer_options::kTokenizerSubgroups) {
    if (!IsUserTemplate(group)) {
      continue;
    }
    if (group.name == kPipelineName) {
      absl::StrAppend(&out, "<analyzer> | <analyzer>\n",
                      "  chain: each stage re-analyzes the tokens of the "
                      "previous one\n\n");
      continue;
    }
    if (group.name == kUnionName) {
      absl::StrAppend(&out, "[<analyzer>, <analyzer>, ...]\n",
                      "  union: every branch analyzes the same input, the "
                      "tokens are merged\n\n");
      continue;
    }
    absl::StrAppend(&out, Signature(group), "\n");
    if (!group.function.empty()) {
      absl::StrAppend(&out, "  as a function: ", FunctionSignature(group),
                      "\n");
    }
    AppendDescriptions(out, group.FlatOptions());
    absl::StrAppend(&out, "\n");
  }
  absl::StrAppend(&out, "<function>(<arguments>)\n",
                  "  an SQL stage: the value is passed as the first "
                  "argument, so lower() folds case and string_split(',') "
                  "fans a value out into tokens\n\n");
  absl::StrAppend(&out, "(lambda <parameter>: <expression>)\n",
                  "  a lambda stage, for an expression the value does not "
                  "lead, as in (lambda x: x || '!')\n\n");
  absl::StrAppend(&out, "<dictionary name>\n",
                  "  a stored dictionary as a stage: its analyzer is copied "
                  "as it stands when the new dictionary is created\n\n");
  absl::StrAppend(&out, "WITH (frequency, position, norm, offset)\n");
  AppendDescriptions(out, tokenizer_options::kFeaturesOptions);
  return out;
}

}  // namespace sdb::pg
