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

#include "connector/explain_output.h"

#include <absl/algorithm/container.h>
#include <absl/functional/overload.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <simdjson.h>
#include <simdutf.h>

#include <array>
#include <cmath>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <variant>
#include <vector>

#include "connector/functions/vector.h"
#include "connector/search_filter_printer.hpp"
#include "connector/term_dict.h"
#include "pg/types/float.h"

namespace sdb::connector {
namespace {

using Builder = simdjson::builder::string_builder;
using irs::explain::Node;

void AppendValue(Builder& out, const irs::explain::Value& value) {
  std::visit(absl::Overload{
               [&](irs::score_t score) {
                 if (!std::isfinite(score)) {
                   out.append_null();
                   return;
                 }
                 char buffer[pg::kNumberStrMaxLen];
                 const auto* end = pg::WriteShortestFloat(score, buffer);
                 out.append_raw(
                   std::string_view{buffer, static_cast<size_t>(end - buffer)});
               },
               [&](uint64_t count) { out.append(count); },
             },
             value);
}

void AppendString(Builder& out, std::string_view key, std::string_view value) {
  out.append(',');
  out.escape_and_append_with_quotes(key);
  out.append(':');
  out.escape_and_append_with_quotes(value);
}

void AppendTerms(Builder& out, const irs::explain::Subject& subject,
                 const ExplainLabels& labels) {
  if (subject.terms.empty()) {
    return;
  }
  if (!subject.phrase && subject.terms.size() == 1) {
    AppendString(out, "term", labels.Term(subject.field, subject.terms[0]));
    return;
  }
  out.append_raw(subject.phrase ? R"(,"phrase":[)" : R"(,"terms":[)");
  for (size_t i = 0; i != subject.terms.size(); ++i) {
    if (i != 0) {
      out.append(',');
    }
    out.escape_and_append_with_quotes(
      labels.Term(subject.field, subject.terms[i]));
  }
  out.append(']');
}

bool IsInput(const Node& parent, const Node& child) noexcept {
  return irs::explain::IsGroup(parent) ? child.key == "constant"
                                       : !irs::explain::HasSubject(child);
}

void AppendTree(Builder& out, const Node& node, const ExplainLabels& labels) {
  out.append_raw(R"({"value":)");
  AppendValue(out, node.value);
  if (node.key == "match") {
    out.append_raw(R"(,"scored":false)");
  }
  if (irs::explain::HasSubject(node)) {
    AppendString(out, "column", labels.Column(node.subject.field));
    AppendTerms(out, node.subject, labels);
  }
  if (!node.scorer.empty()) {
    AppendString(out, "scorer", node.scorer);
  }
  if (!node.formula.empty()) {
    AppendString(out, "formula", node.formula);
  }
  if (!node.note.empty()) {
    AppendString(out, "note", node.note);
  }
  bool opened = false;
  for (const auto& child : node.details) {
    if (IsInput(node, child)) {
      continue;
    }
    if (!opened) {
      out.append_raw(irs::explain::IsGroup(node) ? R"(,"clauses":[)"
                                                 : R"(,"parts":[)");
      opened = true;
    } else {
      out.append(',');
    }
    AppendTree(out, child, labels);
  }
  if (opened) {
    out.append(']');
  }
  opened = false;
  for (const auto& child : node.details) {
    if (!IsInput(node, child)) {
      continue;
    }
    out.append_raw(opened ? "," : R"(,"inputs":{)");
    opened = true;
    out.escape_and_append_with_quotes(child.key);
    out.append(':');
    AppendTree(out, child, labels);
  }
  if (opened) {
    out.append('}');
  }
  out.append('}');
}

struct Meaning {
  std::string_view input;
  std::string_view text;
};

constexpr std::array kMeanings{
  Meaning{"boost", "the query's weight for this clause"},
  Meaning{"scale", "the weight of this row's match within the clause"},
  Meaning{"freq", "occurrences of the term in the row"},
  Meaning{"count", "matches in the row before scaling"},
  Meaning{"dl", "length of the column in the row"},
  Meaning{"avgdl", "average length of the column"},
  Meaning{"L", "total length of the column over all rows"},
  Meaning{"N", "number of rows with the column"},
  Meaning{"n", "number of rows containing the term"},
  Meaning{"k1", "term saturation parameter"},
  Meaning{"b", "length normalization parameter"},
  Meaning{"ttf", "occurrences of the term over all rows"},
  Meaning{"p", "the term's share of all text in the column"},
  Meaning{"mu", "the Dirichlet prior"},
  Meaning{"lambda", "the share of the collection model"},
  Meaning{"expected", "occurrences of the term if it were spread evenly"},
  Meaning{"value", "the constant score"},
  Meaning{"constant", "the score of clauses every row matches"},
  Meaning{"match", "matches without scoring"},
  Meaning{"exact", "the distance computed from the row's stored vector"},
  Meaning{"dims", "dimensions of the vectors"},
  Meaning{"k", "the number of nearest rows the search returns"},
  Meaning{"nprobe", "clusters the search scans in each segment"},
  Meaning{"ef_search", "candidates the search keeps in its beam"},
  Meaning{"rescore_width",
          "how far a code score may be off and still be read again at full "
          "precision"},
};

std::string_view MeaningOf(std::string_view input) {
  const auto it = absl::c_find_if(
    kMeanings, [&](const Meaning& meaning) { return meaning.input == input; });
  return it == kMeanings.end() ? std::string_view{} : it->text;
}

bool IsDistance(std::string_view scorer) {
  return absl::c_linear_search(
    std::array{kL2Distance, kL2SqrDistance, kL1Distance, kCosineDistance,
               kCosineSimilarity, kIP, kNegativeIP, kL2Norm, kL1Norm},
    scorer);
}

std::string_view StringOf(simdjson::dom::object node, std::string_view key) {
  std::string_view value;
  if (node[key].get_string().get(value) != simdjson::SUCCESS) {
    return {};
  }
  return value;
}

std::string Label(simdjson::dom::object node) {
  const auto column = StringOf(node, "column");
  if (column.empty()) {
    return {};
  }
  if (const auto term = StringOf(node, "term"); !term.empty()) {
    return absl::StrCat(column, ":", term);
  }
  const auto words = [&](std::string_view key) {
    std::vector<std::string_view> terms;
    simdjson::dom::array array;
    if (node[key].get_array().get(array) == simdjson::SUCCESS) {
      for (const auto element : array) {
        std::string_view term;
        if (element.get_string().get(term) == simdjson::SUCCESS) {
          terms.push_back(term);
        }
      }
    }
    return absl::StrJoin(terms, " ");
  };
  if (auto phrase = words("phrase"); !phrase.empty()) {
    return absl::StrCat(column, ":\"", phrase, "\"");
  }
  if (auto terms = words("terms"); !terms.empty()) {
    return absl::StrCat(column, ":(", terms, ")");
  }
  return std::string{column};
}

bool Has(simdjson::dom::object node, std::string_view key) {
  return node[key].error() == simdjson::SUCCESS;
}

std::string_view ClauseKey(simdjson::dom::object node) {
  if (Has(node, "clauses")) {
    return StringOf(node, "formula");
  }
  bool scored = true;
  if (node["scored"].get_bool().get(scored) == simdjson::SUCCESS && !scored) {
    return "match";
  }
  const auto scorer = StringOf(node, "scorer");
  if (IsDistance(scorer)) {
    return "distance";
  }
  return scorer.empty() ? "unexplained" : "weight";
}

std::string LuceneDescription(simdjson::dom::object node, std::string_view key,
                              bool has_details) {
  const auto formula = StringOf(node, "formula");
  if (Has(node, "clauses")) {
    return absl::StrCat(formula, " of:");
  }
  std::string text{key};
  if (const auto label = Label(node); !label.empty()) {
    absl::StrAppend(&text, "(", label, ")");
  }
  if (const auto scorer = StringOf(node, "scorer"); !scorer.empty()) {
    absl::StrAppend(&text, " [", scorer, "]");
  }
  if (const auto note = StringOf(node, "note"); !note.empty()) {
    absl::StrAppend(&text, ", ", note);
  } else if (const auto meaning = MeaningOf(key); !meaning.empty()) {
    absl::StrAppend(&text, ", ", meaning);
  }
  if (formula == "sum" || formula == "max") {
    absl::StrAppend(&text, ", ", formula, " of:");
  } else if (!formula.empty()) {
    absl::StrAppend(&text, ", computed as ", formula,
                    has_details ? " from:" : "");
  }
  return text;
}

void AppendLucene(Builder& out, simdjson::dom::object node,
                  std::string_view key) {
  simdjson::dom::array clauses;
  simdjson::dom::array parts;
  simdjson::dom::object inputs;
  const bool has_clauses =
    node["clauses"].get_array().get(clauses) == simdjson::SUCCESS;
  const bool has_parts =
    node["parts"].get_array().get(parts) == simdjson::SUCCESS;
  const bool has_inputs =
    node["inputs"].get_object().get(inputs) == simdjson::SUCCESS;
  out.append_raw(R"({"value":)");
  simdjson::dom::element value;
  if (node["value"].get(value) == simdjson::SUCCESS) {
    out.append_raw(simdjson::minify(value));
  } else {
    out.append_null();
  }
  out.append_raw(R"(,"description":)");
  out.escape_and_append_with_quotes(
    LuceneDescription(node, key, has_clauses || has_parts || has_inputs));
  out.append_raw(R"(,"details":[)");
  bool first = true;
  const auto append = [&](simdjson::dom::element element,
                          std::string_view child_key) {
    simdjson::dom::object child;
    if (element.get_object().get(child) != simdjson::SUCCESS) {
      return;
    }
    if (!first) {
      out.append(',');
    }
    first = false;
    AppendLucene(out, child, child_key.empty() ? ClauseKey(child) : child_key);
  };
  if (has_clauses) {
    for (const auto clause : clauses) {
      append(clause, {});
    }
  }
  if (has_parts) {
    for (const auto part : parts) {
      append(part, key);
    }
  }
  if (has_inputs) {
    for (const auto [input, child] : inputs) {
      append(child, input);
    }
  }
  out.append_raw("]}");
}

}  // namespace

duckdb::LogicalType ExplainType() { return duckdb::LogicalType::VARIANT(); }

std::string ExplainLabels::Column(irs::field_id field) const {
  return _bind.DisplayColumnName(field);
}

std::string ExplainLabels::Term(irs::field_id field,
                                irs::bytes_view term) const {
  auto type = _bind.ColumnTypeById(field);
  if (type.id() == duckdb::LogicalTypeId::INVALID) {
    type = _bind.relation.inverted_config->ExpressionType(field);
  }
  const auto* data = reinterpret_cast<const char*>(term.data());
  if (!term_dict::IsNumeric(type.id()) &&
      type.id() != duckdb::LogicalTypeId::BOOLEAN &&
      simdutf::validate_utf8(data, term.size())) {
    return std::string{data, term.size()};
  }
  return irs::TermValue(term, type);
}

void WriteExplanations(duckdb::ClientContext& context,
                       std::span<const Node> rows, const ExplainLabels& labels,
                       duckdb::Vector& out) {
  duckdb::Vector json{duckdb::LogicalType::JSON(), rows.size()};
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(json);
  Builder builder;
  for (size_t row = 0; row != rows.size(); ++row) {
    builder.clear();
    AppendTree(builder, rows[row], labels);
    const std::string_view text{builder.view().value()};
    data[row] = duckdb::StringVector::AddString(json, text.data(), text.size());
  }
  duckdb::VectorOperations::Cast(context, json, out, rows.size());
}

void ExplainJsonFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  const auto count = args.size();
  duckdb::Vector tree{duckdb::LogicalType::JSON(), count};
  duckdb::VectorOperations::Cast(state.GetContext(), args.data[0], tree, count);
  simdjson::dom::parser parser;
  Builder builder;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    tree, result, count, [&](duckdb::string_t text) {
      simdjson::dom::object root;
      if (parser.parse(text.GetData(), text.GetSize()).get_object().get(root) !=
          simdjson::SUCCESS) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("ts_explain_json() takes the result of "
                                "ts_explain()"));
      }
      builder.clear();
      AppendLucene(builder, root, ClauseKey(root));
      const std::string_view json{builder.view().value()};
      return duckdb::StringVector::AddString(result, json.data(), json.size());
    });
}

}  // namespace sdb::connector
