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

#include "network/http/es/dsl.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <simdjson.h>

#include "network/http/es/common.h"

namespace sdb::network::http::es {
namespace {

// ES caps from + size at index.max_result_window.
constexpr int64_t kMaxResultWindow = 10000;

using JsonValue = simdjson::ondemand::value;
using JsonObject = simdjson::ondemand::object;

// Translation failures throw this and the entry points turn it into a 400;
// the message is the ES error reason.
struct DslError {
  std::string type;
  std::string reason;
};

[[noreturn]] void Fail(std::string_view reason,
                       std::string_view type = "illegal_argument_exception") {
  throw DslError{std::string{type}, std::string{reason}};
}

std::string_view Key(auto& field) {
  std::string_view key;
  if (field.unescaped_key().get(key) != simdjson::SUCCESS) {
    Fail("malformed query");
  }
  return key;
}

JsonValue Value(auto& field) {
  JsonValue value;
  if (field.value().get(value) != simdjson::SUCCESS) {
    Fail("malformed query");
  }
  return value;
}

JsonObject Object(JsonValue value, std::string_view what) {
  JsonObject object;
  if (value.get_object().get(object) != simdjson::SUCCESS) {
    Fail(absl::StrCat("[", what, "] must be an object"));
  }
  return object;
}

std::string String(JsonValue value, std::string_view what) {
  std::string_view text;
  if (value.get_string().get(text) != simdjson::SUCCESS) {
    Fail(absl::StrCat("[", what, "] must be a string"));
  }
  return std::string{text};
}

int64_t Int(JsonValue value, std::string_view what) {
  int64_t v = 0;
  if (value.get_int64().get(v) != simdjson::SUCCESS) {
    Fail(absl::StrCat("[", what, "] must be an integer"));
  }
  return v;
}

// A term/range scalar as a SQL literal: strings quoted (the column's type
// drives any cast, e.g. a date column compared to an ISO string), numbers
// embedded as their exact JSON token, booleans as keywords.
std::string Scalar(JsonValue value, std::string_view what) {
  simdjson::ondemand::json_type type;
  if (value.type().get(type) != simdjson::SUCCESS) {
    Fail(absl::StrCat("malformed value for [", what, "]"));
  }
  switch (type) {
    case simdjson::ondemand::json_type::string:
      return SqlLiteral(String(value, what));
    case simdjson::ondemand::json_type::number: {
      std::string_view token = value.raw_json_token();
      while (!token.empty() && (token.back() == ' ' || token.back() == '\n' ||
                                token.back() == '\t' || token.back() == '\r')) {
        token.remove_suffix(1);
      }
      return std::string{token};
    }
    case simdjson::ondemand::json_type::boolean: {
      bool v = false;
      if (value.get_bool().get(v) != simdjson::SUCCESS) {
        Fail(absl::StrCat("malformed value for [", what, "]"));
      }
      return v ? "TRUE" : "FALSE";
    }
    default:
      Fail(absl::StrCat("[", what,
                        "] must be a string, a number or a boolean"));
  }
}

struct Clause {
  std::string sql;
  bool uses_match = false;
};

Clause TranslateQuery(JsonValue value);

// {"match": {"field": "text"}} or {"field": {"query": "...", "operator":
// "and"|"or"}}. operator=or tokenizes with ANY semantics (ts_tokenize),
// operator=and requires every token (plainto_tsquery).
Clause TranslateMatch(JsonValue value) {
  auto object = Object(value, "match");
  std::string field;
  std::string query;
  bool conjunction = false;
  for (auto entry : object) {
    if (!field.empty()) {
      Fail("[match] supports exactly one field");
    }
    field = Key(entry);
    auto body = Value(entry);
    simdjson::ondemand::json_type type;
    if (body.type().get(type) != simdjson::SUCCESS) {
      Fail("malformed [match]");
    }
    if (type == simdjson::ondemand::json_type::string) {
      query = String(body, "match");
      continue;
    }
    for (auto param : Object(body, "match")) {
      const auto key = Key(param);
      if (key == "query") {
        query = String(Value(param), "match.query");
      } else if (key == "operator") {
        const auto op = String(Value(param), "match.operator");
        if (op == "and" || op == "AND") {
          conjunction = true;
        } else if (op != "or" && op != "OR") {
          Fail(absl::StrCat("unknown [match] operator [", op, "]"));
        }
      } else {
        Fail(absl::StrCat("[match] parameter [", key,
                          "] is not supported yet"));
      }
    }
  }
  if (field.empty()) {
    Fail("[match] requires a field");
  }
  return {absl::StrCat(SqlIdentifier(field), " @@ ",
                       conjunction ? "plainto_tsquery(" : "ts_tokenize(",
                       SqlLiteral(query), ")"),
          true};
}

// {"term": {"field": v}} or {"field": {"value": v}}.
Clause TranslateTerm(JsonValue value) {
  auto object = Object(value, "term");
  std::string sql;
  for (auto entry : object) {
    if (!sql.empty()) {
      Fail("[term] supports exactly one field");
    }
    const auto field = std::string{Key(entry)};
    auto body = Value(entry);
    simdjson::ondemand::json_type type;
    if (body.type().get(type) != simdjson::SUCCESS) {
      Fail("malformed [term]");
    }
    std::string literal;
    if (type == simdjson::ondemand::json_type::object) {
      for (auto param : Object(body, "term")) {
        const auto key = Key(param);
        if (key != "value") {
          Fail(absl::StrCat("[term] parameter [", key,
                            "] is not supported yet"));
        }
        literal = Scalar(Value(param), field);
      }
      if (literal.empty()) {
        Fail("[term] requires a value");
      }
    } else {
      literal = Scalar(body, field);
    }
    sql = absl::StrCat(SqlIdentifier(field), " = ", literal);
  }
  if (sql.empty()) {
    Fail("[term] requires a field");
  }
  return {std::move(sql), false};
}

// {"range": {"field": {"gte": x, "lt": y, ...}}}
Clause TranslateRange(JsonValue value) {
  auto object = Object(value, "range");
  std::string sql;
  for (auto entry : object) {
    if (!sql.empty()) {
      Fail("[range] supports exactly one field");
    }
    const auto field = std::string{Key(entry)};
    const auto ident = SqlIdentifier(field);
    std::vector<std::string> parts;
    for (auto param : Object(Value(entry), "range")) {
      const auto key = Key(param);
      std::string_view op;
      if (key == "gt") {
        op = " > ";
      } else if (key == "gte") {
        op = " >= ";
      } else if (key == "lt") {
        op = " < ";
      } else if (key == "lte") {
        op = " <= ";
      } else {
        Fail(absl::StrCat("[range] parameter [", key,
                          "] is not supported yet"));
      }
      parts.push_back(absl::StrCat(ident, op, Scalar(Value(param), field)));
    }
    if (parts.empty()) {
      Fail("[range] requires at least one bound");
    }
    sql = absl::StrJoin(parts, " AND ");
  }
  if (sql.empty()) {
    Fail("[range] requires a field");
  }
  return {std::move(sql), false};
}

// A clause group value is a single query object or an array of them.
std::vector<Clause> TranslateGroup(JsonValue value, std::string_view what) {
  std::vector<Clause> out;
  simdjson::ondemand::json_type type;
  if (value.type().get(type) != simdjson::SUCCESS) {
    Fail(absl::StrCat("malformed [", what, "]"));
  }
  if (type == simdjson::ondemand::json_type::array) {
    simdjson::ondemand::array array;
    if (value.get_array().get(array) != simdjson::SUCCESS) {
      Fail(absl::StrCat("malformed [", what, "]"));
    }
    for (auto element : array) {
      JsonValue item;
      if (element.get(item) != simdjson::SUCCESS) {
        Fail(absl::StrCat("malformed [", what, "]"));
      }
      out.push_back(TranslateQuery(item));
    }
  } else {
    out.push_back(TranslateQuery(value));
  }
  return out;
}

Clause TranslateBool(JsonValue value) {
  std::vector<Clause> must;
  std::vector<Clause> must_not;
  std::vector<Clause> should;
  int64_t min_should = -1;
  for (auto entry : Object(value, "bool")) {
    const auto key = Key(entry);
    if (key == "must" || key == "filter") {
      auto group = TranslateGroup(Value(entry), key);
      must.insert(must.end(), std::make_move_iterator(group.begin()),
                  std::make_move_iterator(group.end()));
    } else if (key == "must_not") {
      auto group = TranslateGroup(Value(entry), key);
      must_not.insert(must_not.end(), std::make_move_iterator(group.begin()),
                      std::make_move_iterator(group.end()));
    } else if (key == "should") {
      should = TranslateGroup(Value(entry), key);
    } else if (key == "minimum_should_match") {
      min_should = Int(Value(entry), "minimum_should_match");
      if (min_should != 0 && min_should != 1) {
        Fail("only minimum_should_match 0 or 1 is supported yet");
      }
    } else {
      Fail(absl::StrCat("[bool] parameter [", key, "] is not supported yet"));
    }
  }
  // ES default: should is required only when there is no must/filter.
  if (min_should < 0) {
    min_should = must.empty() ? 1 : 0;
  }

  Clause out;
  std::vector<std::string> parts;
  for (auto& clause : must) {
    out.uses_match |= clause.uses_match;
    parts.push_back(absl::StrCat("(", clause.sql, ")"));
  }
  for (auto& clause : must_not) {
    out.uses_match |= clause.uses_match;
    parts.push_back(absl::StrCat("NOT (", clause.sql, ")"));
  }
  if (!should.empty() && min_should == 1) {
    // The planner claims an OR group all-or-nothing: an OR mixing @@ with a
    // plain predicate would leave the @@ to its runtime stub.
    bool any_match = false;
    bool any_filter = false;
    std::vector<std::string> options;
    for (auto& clause : should) {
      (clause.uses_match ? any_match : any_filter) = true;
      out.uses_match |= clause.uses_match;
      options.push_back(absl::StrCat("(", clause.sql, ")"));
    }
    if (any_match && any_filter) {
      Fail(
        "[should] mixing full-text and filter clauses is not supported yet");
    }
    parts.push_back(
      absl::StrCat("(", absl::StrJoin(options, " OR "), ")"));
  }
  out.sql = parts.empty() ? "TRUE" : absl::StrJoin(parts, " AND ");
  return out;
}

Clause TranslateQuery(JsonValue value) {
  Clause out;
  for (auto entry : Object(value, "query")) {
    if (!out.sql.empty()) {
      Fail("a query must have exactly one clause");
    }
    const auto key = Key(entry);
    if (key == "match_all") {
      Object(Value(entry), "match_all");
      out = {"TRUE", false};
    } else if (key == "match") {
      out = TranslateMatch(Value(entry));
    } else if (key == "term") {
      out = TranslateTerm(Value(entry));
    } else if (key == "range") {
      out = TranslateRange(Value(entry));
    } else if (key == "bool") {
      out = TranslateBool(Value(entry));
    } else {
      Fail(absl::StrCat("query type [", key, "] is not supported yet"));
    }
  }
  if (out.sql.empty()) {
    Fail("a query must have exactly one clause");
  }
  return out;
}

// "field" | {"field": "desc"} | {"field": {"order": "desc"}}; _score and
// _doc orders are no-ops here (scores are constant).
void TranslateSort(JsonValue value, SearchRequest& out) {
  auto add = [&](std::string_view field, std::string_view order) {
    if (field == "_score" || field == "_doc") {
      return;
    }
    if (order != "asc" && order != "desc") {
      Fail(absl::StrCat("unknown sort order [", order, "]"));
    }
    absl::StrAppend(&out.order_by, out.order_by.empty() ? "" : ", ",
                    SqlIdentifier(field), order == "desc" ? " DESC" : " ASC",
                    " NULLS LAST");
    out.sort_fields.emplace_back(field);
  };
  auto add_entry = [&](JsonValue entry) {
    simdjson::ondemand::json_type type;
    if (entry.type().get(type) != simdjson::SUCCESS) {
      Fail("malformed [sort]");
    }
    if (type == simdjson::ondemand::json_type::string) {
      add(String(entry, "sort"), "asc");
      return;
    }
    for (auto field : Object(entry, "sort")) {
      const auto name = std::string{Key(field)};
      auto body = Value(field);
      simdjson::ondemand::json_type body_type;
      if (body.type().get(body_type) != simdjson::SUCCESS) {
        Fail("malformed [sort]");
      }
      if (body_type == simdjson::ondemand::json_type::string) {
        add(name, String(body, "sort"));
        continue;
      }
      std::string order = "asc";
      for (auto param : Object(body, "sort")) {
        const auto key = Key(param);
        if (key != "order") {
          Fail(absl::StrCat("[sort] parameter [", key,
                            "] is not supported yet"));
        }
        order = String(Value(param), "sort.order");
      }
      add(name, order);
    }
  };

  simdjson::ondemand::json_type type;
  if (value.type().get(type) != simdjson::SUCCESS) {
    Fail("malformed [sort]");
  }
  if (type == simdjson::ondemand::json_type::array) {
    simdjson::ondemand::array array;
    if (value.get_array().get(array) != simdjson::SUCCESS) {
      Fail("malformed [sort]");
    }
    for (auto element : array) {
      JsonValue entry;
      if (element.get(entry) != simdjson::SUCCESS) {
        Fail("malformed [sort]");
      }
      add_entry(entry);
    }
  } else {
    add_entry(value);
  }
}

template<typename F>
bool ParseBody(std::string_view body, HttpResponseWriter& writer, F&& fill) {
  try {
    if (!body.empty()) {
      simdjson::padded_string padded{body};
      simdjson::ondemand::parser parser;
      simdjson::ondemand::document doc;
      if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
        Fail("request body must be JSON", "parsing_exception");
      }
      JsonObject object;
      if (doc.get_object().get(object) != simdjson::SUCCESS) {
        Fail("request body must be a JSON object", "parsing_exception");
      }
      fill(object);
    }
    return true;
  } catch (const DslError& e) {
    WriteError(writer, 400, e.type, e.reason);
    return false;
  } catch (const std::exception& e) {
    WriteError(writer, 400, "parsing_exception", e.what());
    return false;
  }
}

}  // namespace

bool ParseSearchBody(std::string_view body, SearchRequest& out,
                     HttpResponseWriter& writer) {
  const bool ok = ParseBody(body, writer, [&](JsonObject& object) {
    for (auto entry : object) {
      const auto key = Key(entry);
      if (key == "query") {
        auto clause = TranslateQuery(Value(entry));
        out.where = std::move(clause.sql);
        out.uses_match = clause.uses_match;
      } else if (key == "size") {
        out.size = Int(Value(entry), "size");
      } else if (key == "from") {
        out.from = Int(Value(entry), "from");
      } else if (key == "sort") {
        TranslateSort(Value(entry), out);
      } else if (key == "_source") {
        bool include = true;
        if (Value(entry).get_bool().get(include) != simdjson::SUCCESS) {
          Fail("only boolean [_source] is supported yet");
        }
        out.include_source = include;
      } else if (key == "track_total_hits") {
        // Totals are exact; the tracking hint has nothing to change.
      } else {
        Fail(absl::StrCat("request parameter [", key,
                          "] is not supported yet"));
      }
    }
  });
  if (!ok) {
    return false;
  }
  if (out.size < 0 || out.from < 0) {
    WriteError(writer, 400, "illegal_argument_exception",
               "[size] and [from] must be non-negative");
    return false;
  }
  if (out.from + out.size > kMaxResultWindow) {
    WriteError(
      writer, 400, "illegal_argument_exception",
      absl::StrCat("Result window is too large, from + size must be less "
                   "than or equal to: [",
                   kMaxResultWindow, "] but was [", out.from + out.size, "]"));
    return false;
  }
  if (out.where == "TRUE") {
    out.where.clear();
  }
  return true;
}

bool ParseCountBody(std::string_view body, SearchRequest& out,
                    HttpResponseWriter& writer) {
  const bool ok = ParseBody(body, writer, [&](JsonObject& object) {
    for (auto entry : object) {
      const auto key = Key(entry);
      if (key != "query") {
        Fail(absl::StrCat("request does not support [", key, "]"),
             "parsing_exception");
      }
      auto clause = TranslateQuery(Value(entry));
      out.where = std::move(clause.sql);
      out.uses_match = clause.uses_match;
    }
  });
  if (ok && out.where == "TRUE") {
    out.where.clear();
  }
  return ok;
}

}  // namespace sdb::network::http::es
