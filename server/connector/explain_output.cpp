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
#include <absl/container/inlined_vector.h>
#include <absl/functional/overload.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <fast_float/fast_float.h>
#include <simdjson.h>
#include <simdutf.h>

#include <array>
#include <cmath>
#include <duckdb/common/types/variant/variant_builder.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <system_error>
#include <type_traits>
#include <variant>
#include <vector>

#include "connector/functions/vector.h"
#include "connector/search_filter_printer.hpp"
#include "connector/term_dict.h"
#include "pg/types/float.h"

namespace sdb::connector {
namespace {

using Builder = simdjson::builder::string_builder;
using duckdb::VariantLogicalType;
using irs::explain::Node;

struct Primitive {
  std::string_view text;
  double real = 0;
  uint64_t whole = 0;
  int64_t negative = 0;

  template<typename T>
  T GetData() const noexcept {
    if constexpr (std::is_same_v<T, double>) {
      return real;
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      return whole;
    } else if constexpr (std::is_same_v<T, int64_t>) {
      return negative;
    } else {
      SDB_ASSERT(false);
      return T{};
    }
  }

  duckdb::string_t GetString() const noexcept {
    return {text.data(), static_cast<uint32_t>(text.size())};
  }

  duckdb::VariantDecimalData GetDecimalProperties() const noexcept {
    SDB_ASSERT(false);
    return {0, 0, nullptr};
  }
};

enum class Field : uint8_t {
  Clauses,
  Column,
  Formula,
  Inputs,
  Note,
  Parts,
  Phrase,
  Scored,
  Scorer,
  Slop,
  Term,
  Terms,
  Value,
};

constexpr std::array<std::string_view, 13> kFieldNames{
  "clauses", "column", "formula", "inputs", "note",  "parts", "phrase",
  "scored",  "scorer", "slop",    "term",   "terms", "value",
};

duckdb::string_t Key(std::string_view key) noexcept {
  return {key.data(), static_cast<uint32_t>(key.size())};
}

bool IsInput(const Node& parent, const Node& child) noexcept {
  return irs::explain::IsGroup(parent) ? child.key == "constant"
                                       : !irs::explain::HasSubject(child);
}

class Emitter {
 public:
  explicit Emitter(const ExplainLabels& labels) noexcept : _labels{labels} {}

  bool Emit(const Node& row, duckdb::VariantBuilder& builder) {
    _builder = &builder;
    Tree(row);
    return false;
  }

 private:
  void Tree(const Node& node) {
    absl::InlinedVector<const Node*, 8> parts;
    absl::InlinedVector<const Node*, 8> inputs;
    for (const auto& child : node.details) {
      (IsInput(node, child) ? inputs : parts).push_back(&child);
    }
    absl::c_sort(inputs, [](const Node* lhs, const Node* rhs) {
      return lhs->key < rhs->key;
    });
    const auto& subject = node.subject;
    const bool labeled = irs::explain::HasSubject(node);
    std::array<Field, kFieldNames.size()> fields;
    size_t size = 0;
    const auto add = [&](Field field, bool present) {
      if (present) {
        fields[size++] = field;
      }
    };
    const bool group = irs::explain::IsGroup(node);
    add(Field::Clauses, group && !parts.empty());
    add(Field::Column, labeled);
    add(Field::Formula, !node.formula.empty());
    add(Field::Inputs, !inputs.empty());
    add(Field::Note, !node.note.empty());
    add(Field::Parts, !group && !parts.empty());
    add(Field::Phrase, labeled && !subject.phrase.empty());
    add(Field::Scored, node.key == "match");
    add(Field::Scorer, !node.scorer.empty());
    add(Field::Slop, labeled && !subject.phrase.empty() && subject.slop != 0);
    add(Field::Term,
        labeled && subject.phrase.empty() && subject.terms.size() == 1);
    add(Field::Terms, labeled && subject.terms.size() > 1);
    add(Field::Value, true);
    _builder->EmitObject(
      size,
      [&](duckdb::idx_t i) {
        return Key(kFieldNames[static_cast<size_t>(fields[i])]);
      },
      [&](duckdb::idx_t i) {
        switch (fields[i]) {
          case Field::Clauses:
          case Field::Parts:
            _builder->EmitArray(parts.size(),
                                [&](duckdb::idx_t j) { Tree(*parts[j]); });
            break;
          case Field::Column:
            String(Column(subject.field));
            break;
          case Field::Formula:
            String(node.formula);
            break;
          case Field::Inputs:
            _builder->EmitObject(
              inputs.size(),
              [&](duckdb::idx_t j) { return Key(inputs[j]->key); },
              [&](duckdb::idx_t j) { Tree(*inputs[j]); });
            break;
          case Field::Note:
            String(node.note);
            break;
          case Field::Phrase:
            Phrase(subject);
            break;
          case Field::Scored:
            _builder->EmitPrimitiveNode(Primitive{},
                                        VariantLogicalType::BOOL_FALSE);
            break;
          case Field::Scorer:
            String(node.scorer);
            break;
          case Field::Slop:
            Whole(subject.slop);
            break;
          case Field::Term:
            String(_labels.Term(subject.field, subject.terms.front()));
            break;
          case Field::Terms:
            _builder->EmitArray(subject.terms.size(), [&](duckdb::idx_t j) {
              String(_labels.Term(subject.field, subject.terms[j]));
            });
            break;
          case Field::Value:
            Number(node.value);
            break;
        }
      });
  }

  void Phrase(const irs::explain::Subject& subject) {
    struct Element {
      const irs::explain::PhrasePosition* position;
      bool gap;
    };
    absl::InlinedVector<Element, 8> elements;
    for (const auto& position : subject.phrase) {
      if (position.gap_min != position.gap_max) {
        elements.push_back({&position, true});
      } else {
        elements.insert(elements.end(), position.gap_min, {nullptr, true});
      }
      elements.push_back({&position, false});
    }
    _builder->EmitArray(elements.size(), [&](duckdb::idx_t i) {
      const auto [position, gap] = elements[i];
      if (gap && !position) {
        _builder->EmitNull();
      } else if (gap) {
        _builder->EmitObject(
          2, [](duckdb::idx_t j) { return Key(j == 0 ? "max" : "min"); },
          [&](duckdb::idx_t j) {
            Whole(j == 0 ? position->gap_max : position->gap_min);
          });
      } else if (position->terms.size() == 1) {
        String(_labels.Term(subject.field, position->terms.front()));
      } else {
        _builder->EmitArray(position->terms.size(), [&](duckdb::idx_t j) {
          String(_labels.Term(subject.field, position->terms[j]));
        });
      }
    });
  }

  void String(std::string_view text) {
    _builder->EmitPrimitiveNode(Primitive{.text = text},
                                VariantLogicalType::VARCHAR);
  }

  void Whole(uint64_t value) {
    _builder->EmitPrimitiveNode(Primitive{.whole = value},
                                VariantLogicalType::UINT64);
  }

  void Number(const irs::explain::Value& value) {
    std::visit(absl::Overload{
                 [&](irs::score_t score) { Score(score); },
                 [&](uint64_t count) { Whole(count); },
               },
               value);
  }

  void Score(irs::score_t score) {
    if (!std::isfinite(score)) {
      _builder->EmitNull();
      return;
    }
    char buffer[pg::kNumberStrMaxLen];
    const auto* end = pg::WriteShortestFloat(score, buffer);
    const std::string_view text{buffer, static_cast<size_t>(end - buffer)};
    Primitive number;
    if (text.find_first_of(".eE") != std::string_view::npos) {
      [[maybe_unused]] const auto parsed = fast_float::from_chars(
        text.data(), text.data() + text.size(), number.real);
      SDB_ASSERT(parsed.ec == std::errc{});
      _builder->EmitPrimitiveNode(number, VariantLogicalType::DOUBLE);
    } else if (text.front() == '-') {
      [[maybe_unused]] const auto parsed = fast_float::from_chars(
        text.data(), text.data() + text.size(), number.negative);
      SDB_ASSERT(parsed.ec == std::errc{});
      _builder->EmitPrimitiveNode(number, VariantLogicalType::INT64);
    } else {
      [[maybe_unused]] const auto parsed = fast_float::from_chars(
        text.data(), text.data() + text.size(), number.whole);
      SDB_ASSERT(parsed.ec == std::errc{});
      _builder->EmitPrimitiveNode(number, VariantLogicalType::UINT64);
    }
  }

  std::string_view Column(irs::field_id field) {
    auto [it, added] = _columns.try_emplace(field);
    if (added) {
      it->second = _labels.Column(field);
    }
    return it->second;
  }

  const ExplainLabels& _labels;
  irs::containers::FlatHashMap<irs::field_id, std::string> _columns;
  duckdb::VariantBuilder* _builder = nullptr;
};

class ExplanationSource {
 public:
  ExplanationSource(std::span<const Node> rows,
                    const ExplainLabels& labels) noexcept
    : _rows{rows}, _emitter{labels} {}

  bool Emit(duckdb::idx_t row, duckdb::VariantBuilder& builder) {
    return _emitter.Emit(_rows[row], builder);
  }

 private:
  std::span<const Node> _rows;
  Emitter _emitter;
};

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
  Meaning{"df", "number of rows containing the term"},
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

std::string Words(simdjson::dom::array array) {
  std::vector<std::string_view> words;
  for (const auto element : array) {
    std::string_view word;
    if (element.get_string().get(word) == simdjson::SUCCESS) {
      words.push_back(word);
    }
  }
  return absl::StrJoin(words, " ");
}

uint64_t WholeOf(simdjson::dom::object node, std::string_view key) {
  uint64_t value = 0;
  if (node[key].get_uint64().get(value) != simdjson::SUCCESS) {
    return 0;
  }
  return value;
}

std::string PhraseText(simdjson::dom::array phrase) {
  std::vector<std::string> words;
  for (const auto element : phrase) {
    std::string_view word;
    simdjson::dom::array alternatives;
    simdjson::dom::object gap;
    if (element.get_string().get(word) == simdjson::SUCCESS) {
      words.emplace_back(word);
    } else if (element.get_array().get(alternatives) == simdjson::SUCCESS) {
      words.push_back(absl::StrCat("(", Words(alternatives), ")"));
    } else if (element.get_object().get(gap) == simdjson::SUCCESS) {
      words.push_back(
        absl::StrCat("?{", WholeOf(gap, "min"), ",", WholeOf(gap, "max"), "}"));
    } else {
      words.emplace_back("?");
    }
  }
  return absl::StrJoin(words, " ");
}

std::string Label(simdjson::dom::object node) {
  const auto column = StringOf(node, "column");
  if (column.empty()) {
    return {};
  }
  if (const auto term = StringOf(node, "term"); !term.empty()) {
    return absl::StrCat(column, ":", term);
  }
  simdjson::dom::array array;
  if (node["phrase"].get_array().get(array) == simdjson::SUCCESS) {
    auto text = absl::StrCat(column, ":\"", PhraseText(array), "\"");
    if (const auto slop = WholeOf(node, "slop"); slop != 0) {
      absl::StrAppend(&text, "~", slop);
    }
    return text;
  }
  if (node["terms"].get_array().get(array) == simdjson::SUCCESS) {
    return absl::StrCat(column, ":(", Words(array), ")");
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

void WriteExplanations(std::span<const Node> rows, const ExplainLabels& labels,
                       duckdb::Vector& out) {
  ExplanationSource source{rows, labels};
  duckdb::BuildVariant(source, rows.size(), out);
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
