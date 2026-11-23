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
#include <fast_float/fast_float.h>
#include <simdjson.h>
#include <simdutf.h>

#include <array>
#include <cmath>
#include <duckdb/common/types/variant/variant_builder.hpp>
#include <duckdb/common/types/variant_iterator.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <system_error>
#include <variant>

#include "connector/functions/vector.h"
#include "connector/search_filter_printer.hpp"
#include "pg/types/float.h"

namespace sdb::connector {
namespace {

using Builder = simdjson::builder::string_builder;
using duckdb::VariantLogicalType;
using duckdb::VariantNode;
using irs::explain::Node;
using irs::explain::Role;

enum class Field : uint8_t {
  Clauses,
  Column,
  Expression,
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
  "clauses", "column", "expression", "inputs", "note",  "parts", "phrase",
  "scored",  "scorer", "slop",       "term",   "terms", "value",
};

class Emitter {
 public:
  Emitter(std::span<const Node> rows, const ScanBindData& bind) noexcept
    : _rows{rows}, _bind{bind} {}

  bool Emit(duckdb::idx_t row, duckdb::VariantBuilder& builder) {
    _builder = &builder;
    Tree(_rows[row]);
    return false;
  }

 private:
  void Tree(const Node& node) {
    absl::InlinedVector<const Node*, 8> parts;
    absl::InlinedVector<const Node*, 8> inputs;
    for (const auto& child : node.details) {
      (child.role == Role::Input ? inputs : parts).push_back(&child);
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
    add(Field::Expression, !node.expression.empty());
    add(Field::Inputs, !inputs.empty());
    add(Field::Note, !node.note.empty());
    add(Field::Parts, !group && !parts.empty());
    add(Field::Phrase, labeled && !subject.phrase.empty());
    add(Field::Scored, node.role == Role::Match);
    add(Field::Scorer, !node.scorer.empty());
    add(Field::Slop, labeled && !subject.phrase.empty() && subject.slop != 0);
    add(Field::Term,
        labeled && subject.phrase.empty() && subject.terms.size() == 1);
    add(Field::Terms, labeled && subject.terms.size() > 1);
    add(Field::Value, true);
    _builder->EmitObject(
      size,
      [&](duckdb::idx_t i) {
        return duckdb::string_t{kFieldNames[static_cast<size_t>(fields[i])]};
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
          case Field::Expression:
            String(node.expression);
            break;
          case Field::Inputs:
            _builder->EmitObject(
              inputs.size(),
              [&](duckdb::idx_t j) { return duckdb::string_t{inputs[j]->key}; },
              [&](duckdb::idx_t j) { Tree(*inputs[j]); });
            break;
          case Field::Note:
            String(node.note);
            break;
          case Field::Phrase:
            Phrase(subject);
            break;
          case Field::Scored:
            _builder->EmitBool(false);
            break;
          case Field::Scorer:
            String(node.scorer);
            break;
          case Field::Slop:
            Whole(subject.slop);
            break;
          case Field::Term:
            Term(subject, subject.terms.front());
            break;
          case Field::Terms:
            _builder->EmitArray(subject.terms.size(), [&](duckdb::idx_t j) {
              Term(subject, subject.terms[j]);
            });
            break;
          case Field::Value:
            Number(node.value);
            break;
        }
      });
  }

  void Phrase(const irs::explain::SubjectView& subject) {
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
          2,
          [](duckdb::idx_t j) {
            return duckdb::string_t{j == 0 ? "max" : "min"};
          },
          [&](duckdb::idx_t j) {
            Whole(j == 0 ? position->gap_max : position->gap_min);
          });
      } else if (position->terms.size() == 1) {
        Term(subject, position->terms.front());
      } else {
        _builder->EmitArray(position->terms.size(), [&](duckdb::idx_t j) {
          Term(subject, position->terms[j]);
        });
      }
    });
  }

  void String(std::string_view text) {
    _builder->EmitString(VariantLogicalType::VARCHAR, duckdb::string_t{text});
  }

  void Whole(uint64_t value) {
    _builder->EmitFixed(VariantLogicalType::UINT64, value);
  }

  template<typename T>
  void Parsed(std::string_view text, VariantLogicalType type) {
    T value = 0;
    [[maybe_unused]] const auto parsed =
      fast_float::from_chars(text.data(), text.data() + text.size(), value);
    SDB_ASSERT(parsed.ec == std::errc{});
    _builder->EmitFixed(type, value);
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
    if (text.find_first_of(".eE") != std::string_view::npos) {
      Parsed<double>(text, VariantLogicalType::DOUBLE);
    } else if (text.front() == '-') {
      Parsed<int64_t>(text, VariantLogicalType::INT64);
    } else {
      Parsed<uint64_t>(text, VariantLogicalType::UINT64);
    }
  }

  std::string_view Column(irs::field_id field) {
    auto [it, added] = _columns.try_emplace(field);
    if (added) {
      it->second = _bind.DisplayColumnName(field);
    }
    return it->second;
  }

  void Term(const irs::explain::SubjectView& subject, irs::bytes_view term) {
    auto [it, added] =
      _types.try_emplace(std::pair{subject.field, subject.dict});
    if (added) {
      it->second = _bind.FieldType(subject.field, subject.dict);
    }
    const auto text = irs::ViewCast<char>(term);
    if (it->second.id() == duckdb::LogicalTypeId::VARCHAR &&
        simdutf::validate_utf8(text.data(), text.size())) {
      String(text);
    } else {
      String(irs::DictTermValue(subject.dict, term, it->second));
    }
  }

  std::span<const Node> _rows;
  const ScanBindData& _bind;
  irs::containers::FlatHashMap<irs::field_id, std::string> _columns;
  irs::containers::FlatHashMap<std::pair<irs::field_id, irs::DictRef>,
                               duckdb::LogicalType>
    _types;
  duckdb::VariantBuilder* _builder = nullptr;
};

struct Meaning {
  std::string_view input;
  std::string_view text;
};

constexpr std::array kMeanings{
  Meaning{"boost", "the query's weight for this clause"},
  Meaning{"scale", "the weight of this row's match within the clause"},
  Meaning{"score", "the score before scaling"},
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

std::string_view View(const duckdb::string_t& text) noexcept {
  return {text.GetData(), text.GetSize()};
}

bool Is(const VariantNode& node, VariantLogicalType type) {
  return node.GetTypeId() == type;
}

duckdb::string_t StringOf(const VariantNode& node) {
  return Is(node, VariantLogicalType::VARCHAR) ? node.GetString()
                                               : duckdb::string_t{};
}

std::optional<uint64_t> UnsignedOf(const VariantNode& node) {
  switch (node.GetTypeId()) {
    case VariantLogicalType::UINT8:
      return node.GetData<uint8_t>();
    case VariantLogicalType::UINT16:
      return node.GetData<uint16_t>();
    case VariantLogicalType::UINT32:
      return node.GetData<uint32_t>();
    case VariantLogicalType::UINT64:
      return node.GetData<uint64_t>();
    default:
      return std::nullopt;
  }
}

std::optional<int64_t> SignedOf(const VariantNode& node) {
  switch (node.GetTypeId()) {
    case VariantLogicalType::INT8:
      return node.GetData<int8_t>();
    case VariantLogicalType::INT16:
      return node.GetData<int16_t>();
    case VariantLogicalType::INT32:
      return node.GetData<int32_t>();
    case VariantLogicalType::INT64:
      return node.GetData<int64_t>();
    default:
      return std::nullopt;
  }
}

uint64_t WholeOf(const VariantNode& node) {
  if (const auto value = UnsignedOf(node)) {
    return *value;
  }
  const auto value = SignedOf(node);
  return value && *value > 0 ? static_cast<uint64_t>(*value) : 0;
}

struct Fields {
  VariantNode value;
  VariantNode clauses;
  VariantNode parts;
  VariantNode inputs;
  VariantNode phrase;
  VariantNode terms;
  duckdb::string_t column{};
  duckdb::string_t expression{};
  duckdb::string_t note{};
  duckdb::string_t scorer{};
  duckdb::string_t term{};
  uint64_t slop = 0;
  bool unscored = false;
};

Fields FieldsOf(const VariantNode& node) {
  Fields fields;
  for (const auto& entry : node.GetObjectChildren()) {
    const auto it = absl::c_find(kFieldNames, View(entry.key));
    if (it == kFieldNames.end()) {
      continue;
    }
    const auto& value = entry.value;
    switch (static_cast<Field>(it - kFieldNames.begin())) {
      case Field::Clauses:
        fields.clauses = value;
        break;
      case Field::Column:
        fields.column = StringOf(value);
        break;
      case Field::Expression:
        fields.expression = StringOf(value);
        break;
      case Field::Inputs:
        fields.inputs = value;
        break;
      case Field::Note:
        fields.note = StringOf(value);
        break;
      case Field::Parts:
        fields.parts = value;
        break;
      case Field::Phrase:
        fields.phrase = value;
        break;
      case Field::Scored:
        fields.unscored = Is(value, VariantLogicalType::BOOL_FALSE);
        break;
      case Field::Scorer:
        fields.scorer = StringOf(value);
        break;
      case Field::Slop:
        fields.slop = WholeOf(value);
        break;
      case Field::Term:
        fields.term = StringOf(value);
        break;
      case Field::Terms:
        fields.terms = value;
        break;
      case Field::Value:
        fields.value = value;
        break;
    }
  }
  return fields;
}

void AppendWords(std::string& text, const VariantNode& array) {
  bool first = true;
  for (const auto element : array.GetArrayChildren()) {
    if (!Is(element, VariantLogicalType::VARCHAR)) {
      continue;
    }
    const auto word = element.GetString();
    absl::StrAppend(&text, first ? "" : " ", View(word));
    first = false;
  }
}

void AppendPhrase(std::string& text, const VariantNode& phrase) {
  bool first = true;
  for (const auto element : phrase.GetArrayChildren()) {
    if (!first) {
      text += ' ';
    }
    first = false;
    switch (element.GetTypeId()) {
      case VariantLogicalType::VARCHAR: {
        const auto word = element.GetString();
        text += View(word);
      } break;
      case VariantLogicalType::ARRAY:
        text += '(';
        AppendWords(text, element);
        text += ')';
        break;
      case VariantLogicalType::OBJECT: {
        uint64_t min = 0;
        uint64_t max = 0;
        for (const auto& entry : element.GetObjectChildren()) {
          const auto key = View(entry.key);
          if (key == "min") {
            min = WholeOf(entry.value);
          } else if (key == "max") {
            max = WholeOf(entry.value);
          }
        }
        absl::StrAppend(&text, "?{", min, ",", max, "}");
      } break;
      default:
        text += '?';
        break;
    }
  }
}

std::string Label(const Fields& node) {
  const auto column = View(node.column);
  if (column.empty()) {
    return {};
  }
  if (const auto term = View(node.term); !term.empty()) {
    return absl::StrCat(column, ":", term);
  }
  std::string text{column};
  if (Is(node.phrase, VariantLogicalType::ARRAY)) {
    text += ":\"";
    AppendPhrase(text, node.phrase);
    text += '"';
    if (node.slop != 0) {
      absl::StrAppend(&text, "~", node.slop);
    }
  } else if (Is(node.terms, VariantLogicalType::ARRAY)) {
    text += ":(";
    AppendWords(text, node.terms);
    text += ')';
  }
  return text;
}

std::string_view ClauseKey(const Fields& node) {
  if (Is(node.clauses, VariantLogicalType::ARRAY)) {
    return View(node.expression);
  }
  if (node.unscored) {
    return "match";
  }
  const auto scorer = View(node.scorer);
  if (IsDistance(scorer)) {
    return "distance";
  }
  return scorer.empty() ? "unexplained" : "weight";
}

std::string LuceneDescription(const Fields& node, std::string_view key,
                              bool has_details) {
  const auto expression = View(node.expression);
  if (Is(node.clauses, VariantLogicalType::ARRAY)) {
    return absl::StrCat(expression, " of:");
  }
  std::string text{key};
  if (const auto label = Label(node); !label.empty()) {
    absl::StrAppend(&text, "(", label, ")");
  }
  if (const auto scorer = View(node.scorer); !scorer.empty()) {
    absl::StrAppend(&text, " [", scorer, "]");
  }
  if (const auto note = View(node.note); !note.empty()) {
    absl::StrAppend(&text, ", ", note);
  } else if (const auto meaning = MeaningOf(key); !meaning.empty()) {
    absl::StrAppend(&text, ", ", meaning);
  }
  if (expression == "sum" || expression == "max") {
    absl::StrAppend(&text, ", ", expression, " of:");
  } else if (!expression.empty()) {
    absl::StrAppend(&text, ", computed as ", expression,
                    has_details ? " from:" : "");
  }
  return text;
}

template<typename T>
void AppendReal(Builder& out, T value) {
  if (!std::isfinite(value)) {
    out.append_null();
    return;
  }
  char buffer[pg::kNumberStrMaxLen];
  const auto* end = pg::WriteShortestFloat(value, buffer);
  out.append_raw(std::string_view{buffer, static_cast<size_t>(end - buffer)});
}

void AppendNumber(Builder& out, const VariantNode& node) {
  if (Is(node, VariantLogicalType::FLOAT)) {
    AppendReal(out, node.GetData<float>());
  } else if (Is(node, VariantLogicalType::DOUBLE)) {
    AppendReal(out, node.GetData<double>());
  } else if (const auto value = UnsignedOf(node)) {
    out.append_raw(absl::AlphaNum(*value).Piece());
  } else if (const auto value = SignedOf(node)) {
    out.append_raw(absl::AlphaNum(*value).Piece());
  } else {
    out.append_null();
  }
}

void AppendLucene(Builder& out, const Fields& node, std::string_view key) {
  const bool has_clauses = Is(node.clauses, VariantLogicalType::ARRAY);
  const bool has_parts = Is(node.parts, VariantLogicalType::ARRAY);
  const bool has_inputs = Is(node.inputs, VariantLogicalType::OBJECT);
  out.append_raw(R"({"value":)");
  AppendNumber(out, node.value);
  out.append_raw(R"(,"description":)");
  out.escape_and_append_with_quotes(
    LuceneDescription(node, key, has_clauses || has_parts || has_inputs));
  out.append_raw(R"(,"details":[)");
  bool first = true;
  const auto append = [&](const VariantNode& element,
                          std::string_view child_key) {
    if (!Is(element, VariantLogicalType::OBJECT)) {
      return;
    }
    if (!first) {
      out.append(',');
    }
    first = false;
    const auto child = FieldsOf(element);
    AppendLucene(out, child, child_key.empty() ? ClauseKey(child) : child_key);
  };
  if (has_clauses) {
    for (const auto clause : node.clauses.GetArrayChildren()) {
      append(clause, {});
    }
  }
  if (has_parts) {
    for (const auto part : node.parts.GetArrayChildren()) {
      append(part, key);
    }
  }
  if (has_inputs) {
    for (const auto& entry : node.inputs.GetObjectChildren()) {
      append(entry.value, View(entry.key));
    }
  }
  out.append_raw("]}");
}

}  // namespace

void WriteExplanations(std::span<const Node> rows, const ScanBindData& bind,
                       duckdb::Vector& out) {
  Emitter emitter{rows, bind};
  duckdb::BuildVariant(emitter, rows.size(), out);
}

void ExplainJsonFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  const auto count = args.size();
  const duckdb::VariantIterator variants{args.data[0]};
  auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(result, count);
  Builder builder;
  for (duckdb::idx_t row = 0; row != count; ++row) {
    if (!variants.RowIsValid(row)) {
      writer.WriteNull();
      continue;
    }
    const auto root = variants.Root(row);
    if (!Is(root, VariantLogicalType::OBJECT)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_explain_json() takes the result of "
                              "ts_explain()"));
    }
    const auto fields = FieldsOf(root);
    builder.clear();
    AppendLucene(builder, fields, ClauseKey(fields));
    writer.WriteValue(duckdb::string_t{builder.view().value()});
  }
}

}  // namespace sdb::connector
