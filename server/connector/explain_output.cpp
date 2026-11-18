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

#include <absl/functional/overload.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <simdjson.h>
#include <simdutf.h>

#include <cmath>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <variant>

#include "connector/search_filter_printer.hpp"
#include "connector/term_dict.h"
#include "pg/types/float.h"

namespace sdb::connector {
namespace {

using Builder = simdjson::builder::string_builder;

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

std::string Description(const irs::explain::Node& node,
                        const ExplainLabels& labels) {
  if (!irs::explain::HasSubject(node)) {
    return node.description;
  }
  return absl::StrCat(node.key, "(", labels.Of(node.subject), ")",
                      node.description.empty() ? "" : " ", node.description);
}

void Append(Builder& out, const irs::explain::Node& node,
            const ExplainLabels& labels) {
  out.append_raw(R"({"value":)");
  AppendValue(out, node.value);
  out.append_raw(R"(,"description":)");
  out.escape_and_append_with_quotes(Description(node, labels));
  out.append_raw(R"(,"details":[)");
  for (size_t i = 0; i != node.details.size(); ++i) {
    if (i != 0) {
      out.append(',');
    }
    Append(out, node.details[i], labels);
  }
  out.append_raw("]}");
}

}  // namespace

duckdb::LogicalType ExplainType() { return duckdb::LogicalType::JSON(); }

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

std::string ExplainLabels::Of(const irs::explain::Subject& subject) const {
  auto column = _bind.DisplayColumnName(subject.field);
  const auto terms = [&](std::string_view separator) {
    return absl::StrJoin(subject.terms, separator,
                         [&](std::string* out, const irs::bstring& term) {
                           absl::StrAppend(out, Term(subject.field, term));
                         });
  };
  if (subject.terms.empty()) {
    return column;
  }
  if (subject.phrase) {
    return absl::StrCat(column, ":\"", terms(" "), "\"");
  }
  if (subject.terms.size() == 1) {
    return absl::StrCat(column, ":", terms(""));
  }
  return absl::StrCat(column, ":(", terms(" "), ")");
}

void WriteExplanations(std::span<const irs::explain::Node> rows,
                       const ExplainLabels& labels, duckdb::Vector& out) {
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(out);
  Builder builder;
  for (size_t row = 0; row != rows.size(); ++row) {
    builder.clear();
    Append(builder, rows[row], labels);
    const std::string_view json{builder.view().value()};
    data[row] = duckdb::StringVector::AddString(out, json.data(), json.size());
  }
}

}  // namespace sdb::connector
