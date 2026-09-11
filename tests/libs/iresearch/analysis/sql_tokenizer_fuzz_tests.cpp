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

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <duckdb.hpp>
#include <format>
#include <map>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/utils/duckdb_engine.h"
#include "iresearch/analysis/sql_tokenizer.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/utils/pg/sql_exception.h"
#include "tests_shared.hpp"
#include "tokenizer_fuzz_checks.hpp"
#include "tokenizer_fuzz_corpus.hpp"
#include "tokenizer_fuzz_mutator.hpp"
#include "tokenizer_fuzz_oracles.hpp"
#include "tokenizer_fuzz_specs.hpp"

namespace {

using namespace tests::fuzz;
using irs::analysis::SqlTokenizer;
using Clock = std::chrono::steady_clock;

duckdb::ClientContext& Context() {
  static auto* conn =
    new duckdb::Connection{sdb::DuckDBEngine::Instance().instance()};
  return *conn->context;
}

uint64_t NameSeed(std::string_view name) {
  uint64_t h = 0xCBF29CE484222325ull;
  for (const char c : name) {
    h = (h ^ static_cast<unsigned char>(c)) * 0x100000001B3ull;
  }
  return h;
}

double Seconds(Clock::time_point from) {
  return std::chrono::duration<double>(Clock::now() - from).count();
}

const std::vector<std::string>& SeedValues() {
  static const std::vector<std::string> kValues = {"",
                                                   "a",
                                                   "abc",
                                                   "Hello, World!",
                                                   "1",
                                                   "42",
                                                   "-7",
                                                   "x'y",
                                                   "日本語",
                                                   "ß",
                                                   " spaced ",
                                                   "a,b,c",
                                                   "2024-01-01",
                                                   "true",
                                                   "1.5",
                                                   "the quick brown fox",
                                                   "ωω",
                                                   "A-B_C",
                                                   "tab\there",
                                                   "new\nline",
                                                   std::string(300, 'z'),
                                                   "12abc",
                                                   "0",
                                                   "NULL",
                                                   "null",
                                                   "a  b   c",
                                                   ",,,",
                                                   "%_%",
                                                   "'quoted'",
                                                   "back\\slash"};
  return kValues;
}

const std::vector<std::string>& InputDictionary() {
  static const std::vector<std::string> kDict = {
    ",", " ", "'", "\\", "-suffix", "a", "0", "%", "_", "ß", "ab,cd", "\n"};
  return kDict;
}

class ExprGen {
 public:
  explicit ExprGen(uint64_t seed) : _rng{seed} {}

  std::string Next() {
    const auto roll = Below(10);
    if (roll < 6) {
      return S(3, "input");
    }
    if (roll < 9) {
      return L(3, "input");
    }
    return Invalid();
  }

 private:
  template<size_t N>
  const char* Pick(const char* const (&xs)[N]) {
    return xs[Below(N)];
  }

  size_t Below(size_t n) {
    return n == 0 ? 0 : static_cast<size_t>(_rng() % n);
  }
  bool Chance(size_t one_in) { return Below(one_in) == 0; }

  std::string Lit() {
    static const char* const kLits[] = {
      "'a'",       "''",       "'A'",     "'x y'",   "','", "' '",
      "'-suffix'", "'ß'",      "'Ωμέγα'", "'it''s'", "'%'", "'_'",
      "'\\'",      "'日本語'", "'ab,cd'", "'0'",     "'.'", "'\n'"};
    return Pick(kLits);
  }

  std::string Pattern() {
    static const char* const kPatterns[] = {
      "'\\s+'", "','",       "'[aeiou]'", "'(ab|cd)'", "'^x'",  "'\\d+'", "'.'",
      "'a*'",   "'[^a-z]+'", "''",        "'(?i)A'",   "'\\b'", "'.*'"};
    return Pick(kPatterns);
  }

  std::string Small() {
    static const char* const kSmall[] = {"0", "1", "2", "3"};
    return Pick(kSmall);
  }

  std::string Var() { return std::format("x{}", ++_lambda); }

  std::string S(int depth, std::string_view leaf) {
    if (depth <= 0 || Chance(4)) {
      const auto roll = Below(20);
      if (roll < 14) {
        return std::string{leaf};
      }
      if (roll < 19) {
        return Lit();
      }
      return "NULL::VARCHAR";
    }
    const int d = depth - 1;
    if (Chance(40)) {
      return std::format("CAST({} AS INTEGER)::VARCHAR", S(d, leaf));
    }
    if (Chance(40)) {
      return std::format("struct_extract({{'a': {}}}, 'a')", S(d, leaf));
    }
    if (Chance(40)) {
      return std::format("concat({}, NULL)", S(d, leaf));
    }
    if (Chance(40)) {
      return std::format("coalesce(NULL, {})", S(d, leaf));
    }
    switch (Below(44)) {
      case 0:
        return std::format("upper({})", S(d, leaf));
      case 1:
        return std::format("lower({})", S(d, leaf));
      case 2:
        return std::format("reverse({})", S(d, leaf));
      case 3:
        return std::format("trim({})", S(d, leaf));
      case 4:
        return std::format("ltrim({}, {})", S(d, leaf), Lit());
      case 5:
        return std::format("rtrim({})", S(d, leaf));
      case 6:
        return std::format("md5({})", S(d, leaf));
      case 7:
        return std::format("sha256({})", S(d, leaf));
      case 8:
        return std::format("nfc_normalize({})", S(d, leaf));
      case 9:
        return std::format("strip_accents({})", S(d, leaf));
      case 10:
        return std::format("hex({})", S(d, leaf));
      case 11:
        return std::format("base64(encode({}))", S(d, leaf));
      case 12:
        return std::format("decode(encode({}))", S(d, leaf));
      case 13:
        return std::format("encode({})", S(d, leaf));
      case 14:
        return std::format("({})::BLOB", S(d, leaf));
      case 15:
        return std::format("({} || {})", S(d, leaf), S(d, leaf));
      case 16:
        return std::format("concat({}, {})", S(d, leaf), S(d, leaf));
      case 17:
        return std::format("concat_ws({}, {}, {})", Lit(), S(d, leaf),
                           S(d, leaf));
      case 18:
        return std::format("coalesce({}, {})", S(d, leaf), S(d, leaf));
      case 19:
        return std::format("nullif({}, {})", S(d, leaf), S(d, leaf));
      case 20:
        return std::format("greatest({}, {})", S(d, leaf), S(d, leaf));
      case 21:
        return std::format("least({}, {})", S(d, leaf), S(d, leaf));
      case 22:
        return std::format("left({}, {})", S(d, leaf), I(d, leaf));
      case 23:
        return std::format("right({}, {})", S(d, leaf), I(d, leaf));
      case 24:
        return std::format("substr({}, {}, {})", S(d, leaf), I(d, leaf),
                           I(d, leaf));
      case 25:
        return std::format("repeat({}, {})", S(d, leaf), Small());
      case 26:
        return std::format("lpad({}, {}, {})", S(d, leaf), Small(), Lit());
      case 27:
        return std::format("rpad({}, {}, {})", S(d, leaf), Small(), Lit());
      case 28:
        return std::format("replace({}, {}, {})", S(d, leaf), Lit(), Lit());
      case 29:
        return std::format("regexp_replace({}, {}, {})", S(d, leaf), Pattern(),
                           Lit());
      case 30:
        return std::format("regexp_replace({}, {}, {}, 'g')", S(d, leaf),
                           Pattern(), Lit());
      case 31:
        return std::format("regexp_extract({}, {})", S(d, leaf), Pattern());
      case 32:
        return std::format("regexp_extract({}, {}, {})", S(d, leaf), Pattern(),
                           Small());
      case 33:
        return std::format("split_part({}, {}, {})", S(d, leaf), Lit(),
                           I(d, leaf));
      case 34:
        return std::format("translate({}, 'abc', 'xyz')", S(d, leaf));
      case 35:
        return std::format("({})[{}:{}]", S(d, leaf), I(d, leaf), I(d, leaf));
      case 36:
        return std::format("array_to_string({}, {})", L(d, leaf), Lit());
      case 37:
        return std::format("({})[{}]", L(d, leaf), I(d, leaf));
      case 38:
        return std::format("list_extract({}, {})", L(d, leaf), I(d, leaf));
      case 39:
        return std::format("CASE WHEN {} THEN {} ELSE {} END", B(d, leaf),
                           S(d, leaf), S(d, leaf));
      case 40:
        return std::format("CASE WHEN {} THEN {} END", B(d, leaf), S(d, leaf));
      case 41:
        return std::format("printf('%s!', {})", S(d, leaf));
      case 42:
        return std::format("format('{{}}-{{}}', {}, {})", S(d, leaf),
                           S(d, leaf));
      default:
        return std::format("try_cast({} AS INTEGER)::VARCHAR", S(d, leaf));
    }
  }

  std::string L(int depth, std::string_view leaf) {
    if (depth <= 0 || Chance(5)) {
      return Below(2) ? std::format("string_split({}, ',')", leaf)
                      : std::format("[{}]", leaf);
    }
    const int d = depth - 1;
    switch (Below(22)) {
      case 0:
        return std::format("string_split({}, {})", S(d, leaf), Lit());
      case 1:
        return std::format("string_split({}, '')", S(d, leaf));
      case 2:
        return std::format("regexp_split_to_array({}, {})", S(d, leaf),
                           Pattern());
      case 3:
        return std::format("str_split_regex({}, {})", S(d, leaf), Pattern());
      case 4:
        return std::format("list_value({}, {})", S(d, leaf), S(d, leaf));
      case 5:
        return std::format("list_value({}, NULL)", S(d, leaf));
      case 6:
        return std::format("[{}, {}]", S(d, leaf), S(d, leaf));
      case 7:
        return std::format("[{}]", S(d, leaf));
      case 8:
        return std::format("list_concat({}, {})", L(d, leaf), L(d, leaf));
      case 9:
        return std::format("list_sort(list_distinct({}))", L(d, leaf));
      case 10:
        return std::format("list_sort({})", L(d, leaf));
      case 11:
        return std::format("list_reverse_sort({})", L(d, leaf));
      case 12:
        return std::format("list_reverse({})", L(d, leaf));
      case 13:
        return std::format("array_slice({}, {}, {})", L(d, leaf), I(d, leaf),
                           I(d, leaf));
      case 14:
        return std::format("list_prepend({}, {})", S(d, leaf), L(d, leaf));
      case 15:
        return std::format("list_append({}, {})", L(d, leaf), S(d, leaf));
      case 16: {
        const auto v = Var();
        return std::format("list_transform({}, {} -> {})", L(d, leaf), v,
                           S(d, v));
      }
      case 17: {
        const auto v = Var();
        return std::format("list_filter({}, {} -> {})", L(d, leaf), v, B(d, v));
      }
      case 18:
        return std::format("CASE WHEN {} THEN {} ELSE {} END", B(d, leaf),
                           L(d, leaf), L(d, leaf));
      case 19:
        return std::format("coalesce({}, {})", L(d, leaf), L(d, leaf));
      case 20:
        return std::format("flatten([{}, {}])", L(d, leaf), L(d, leaf));
      default:
        return "CAST([] AS VARCHAR[])";
    }
  }

  std::string B(int depth, std::string_view leaf) {
    if (depth <= 0) {
      return std::format("({} IS NOT NULL)", leaf);
    }
    const int d = depth - 1;
    switch (Below(16)) {
      case 0:
        return std::format("({} = {})", S(d, leaf), S(d, leaf));
      case 1:
        return std::format("({} <> {})", S(d, leaf), S(d, leaf));
      case 2:
        return std::format("({} LIKE '%a%')", S(d, leaf));
      case 3:
        return std::format("({} ILIKE '%A%')", S(d, leaf));
      case 4:
        return std::format("starts_with({}, {})", S(d, leaf), Lit());
      case 5:
        return std::format("suffix({}, {})", S(d, leaf), Lit());
      case 6:
        return std::format("contains({}, {})", S(d, leaf), Lit());
      case 7:
        return std::format("regexp_matches({}, {})", S(d, leaf), Pattern());
      case 8:
        return std::format("(length({}) > {})", S(d, leaf), Small());
      case 9:
        return std::format("({} IS NULL)", S(d, leaf));
      case 10:
        return std::format("({} IS NOT NULL)", S(d, leaf));
      case 11:
        return std::format("(NOT {})", B(d, leaf));
      case 12:
        return std::format("({} AND {})", B(d, leaf), B(d, leaf));
      case 13:
        return std::format("({} OR {})", B(d, leaf), B(d, leaf));
      case 14:
        return std::format("list_contains({}, {})", L(d, leaf), S(d, leaf));
      default:
        return std::format("(len({}) > {})", L(d, leaf), Small());
    }
  }

  std::string I(int depth, std::string_view leaf) {
    if (depth <= 0 || Chance(2)) {
      return Small();
    }
    const int d = depth - 1;
    switch (Below(8)) {
      case 0:
        return std::format("length({})", S(d, leaf));
      case 1:
        return std::format("strlen({})", S(d, leaf));
      case 2:
        return std::format("instr({}, {})", S(d, leaf), Lit());
      case 3:
        return std::format("({} + {})", I(d, leaf), I(d, leaf));
      case 4:
        return std::format("({} - {})", I(d, leaf), I(d, leaf));
      case 5:
        return std::format("len({})", L(d, leaf));
      case 6:
        return std::format("unicode({})", S(d, leaf));
      default:
        return std::format("levenshtein({}, {})", S(d, leaf), S(d, leaf));
    }
  }

  std::string Invalid() {
    static const char* const kInvalid[] = {
      "length(input)",
      "input = 'a'",
      "hash(input)",
      "struct_pack(a := input)",
      "[length(input)]",
      "input::INTEGER",
      "jaccard(input, 'a')",
      "{'k': input}",
      "map(['k'], [input])",
      "[[input]]",
      "[input]::VARCHAR[1]",
      "input::VARCHAR[]",
      "1",
      "NULL",
      "TRUE",
      "input::DATE",
      "string_agg(input, ',')",
      "unnest([input])",
      "random()::VARCHAR || input",
      "(SELECT input)",
      "upper($1)",
      "hash(input)::VARCHAR",
      "md5_number(input)::VARCHAR",
      "bit_length(input)::VARCHAR",
      "input::UUID::VARCHAR",
      "input::DOUBLE::VARCHAR",
      "input::BOOLEAN::VARCHAR",
      "input::INTERVAL::VARCHAR",
      "input::TIMESTAMP::VARCHAR",
      "input::BIT::VARCHAR",
      "list_value(input)[2]",
      "array_extract(string_split(input, ','), 100)",
      "printf('%d', input)",
      "string_split(input, NULL)",
      "substr(input, 0, -1)",
      "left(input, -1)",
      "repeat(input, -1)",
      "lpad(input, -5, 'x')",
      "regexp_replace(input, '(', '')",
      "regexp_matches(input, '(?P<x)')",
    };
    return Pick(kInvalid);
  }

  std::mt19937_64 _rng;
  uint32_t _lambda = 0;
};

const std::vector<std::string>& AdversarialExpressions() {
  static const std::vector<std::string> kExpressions = [] {
    std::vector<std::string> v = {
      "(SELECT 'x')",
      "upper($1)",
      "input, input",
      "lower((",
      "",
      "   ",
      "random() || input",
      "uuid()::VARCHAR",
      "string_agg(input, ',')",
      "count(*)::VARCHAR",
      "row_number() OVER ()::VARCHAR",
      "unnest(string_split(input, ','))",
      "main.upper(input)",
      "system.main.upper(input)",
      "foo.bar(input)",
      "not_a_function(input)",
      "\"input\"",
      "INPUT",
      "\"Input\"",
      "input.x",
      "t.input",
      "input; SELECT 1",
      "upper(input) -- comment",
      "/* c */ upper(input)",
      "upper(input) /* c */",
      "NULL",
      "''",
      "1",
      "[]",
      "[NULL]",
      "CAST(input AS VARCHAR[])",
      "input::BLOB::VARCHAR",
      "regexp_replace(input, '(', '')",
      "regexp_matches(input, '(?P<x)')",
      "input ORDER BY 1",
      "input LIMIT 1",
      "struct_extract({'a': input}, 'a')",
      "input->>'k'",
      "input::JSON",
      "getvariable('x')",
      "current_setting('threads')::VARCHAR",
      "E'\\n' || input",
      "$$x$$ || input",
      "'a' 'b'",
      "input COLLATE NOCASE",
      "input[1]",
      "input[-1:]",
      "list_transform([input], x -> x)",
      "list_transform([input], (x, i) -> x)",
      "list_reduce([input, input], (a, b) -> a || b)",
      "CASE input WHEN 'a' THEN 'b' END",
      "input BETWEEN 'a' AND 'z'",
      "input IN ('a', 'b')",
      "TRY_CAST(input AS INT)",
      "input::DECIMAL(10,2)::VARCHAR",
      "printf('%d', input)",
      "string_split(input, NULL)",
      "substr(input, 0, -1)",
      "left(input, -1)",
      "list_value(input)[2]",
      "array_extract(string_split(input, ','), 100)",
      "read_text('x')",
      "sqrt(input)::VARCHAR",
      "input::INTEGER::VARCHAR",
      "chr(-1) || input",
      "repeat(input, -1)",
      "lpad(input, -5, 'x')",
      "regexp_split_to_array(input, '')",
      "string_split(input, '')",
      "array_to_string(string_split(input, ','), NULL)",
      "nullif(input, input)",
      "coalesce(NULL, NULL)",
      "list_value()",
      "reverse(reverse(input))",
      "input::UUID::VARCHAR",
      "input::INTERVAL::VARCHAR",
      "input::TIMESTAMP::VARCHAR",
      "input::BOOLEAN::VARCHAR",
      "input::DOUBLE::VARCHAR",
      "input::BIT::VARCHAR",
      "hash(input)::VARCHAR",
      "md5_number(input)::VARCHAR",
      "bit_length(input)::VARCHAR",
      "input SIMILAR TO 'a.*'",
      "input ~ 'a'",
      "ts_tokenize(input, 'text')",
      "sha256(input) || sha256(input)",
      "list_distinct(string_split(input, ','))",
      "list_sort(list_distinct(str_split_regex(input, 'a*')))",
    };
    std::string deep = "input";
    for (size_t i = 0; i < 200; ++i) {
      deep = "upper(" + deep + ")";
    }
    v.push_back(std::move(deep));
    v.push_back("'" + std::string(20000, 'x') + "' || input");
    std::string many = "input";
    for (size_t i = 0; i < 64; ++i) {
      many += " || input";
    }
    v.push_back(std::move(many));
    return v;
  }();
  return kExpressions;
}

enum class Stage : uint8_t {
  ParseFailed,
  BindFailed,
  Bound,
  UnexpectedException,
};

struct Built {
  Stage stage = Stage::UnexpectedException;
  irs::analysis::Tokenizer::ptr tokenizer;
  std::string error;
};

Built Build(const std::string& expression) {
  Built out;
  try {
    out.tokenizer = SqlTokenizer::Make({.expression = expression});
  } catch (const sdb::SqlException& e) {
    out.stage = Stage::ParseFailed;
    out.error = e.what();
    return out;
  } catch (const std::exception& e) {
    out.error = std::format("parse threw {}", e.what());
    return out;
  }
  try {
    out.tokenizer->Bind(Context());
  } catch (const sdb::SqlException& e) {
    out.stage = Stage::BindFailed;
    out.error = e.what();
    out.tokenizer.reset();
    return out;
  } catch (const std::exception& e) {
    out.error = std::format("bind threw {}", e.what());
    out.tokenizer.reset();
    return out;
  }
  out.stage = Stage::Bound;
  return out;
}

struct Verdict {
  SqlVerdict::Kind kind = SqlVerdict::Kind::Error;
  std::vector<std::string> terms;
  std::string error;
};

Verdict RunTokenizer(irs::analysis::Tokenizer& tokenizer,
                     std::string_view value) {
  Verdict out;
  try {
    const auto res = AnalyzeValue(tokenizer, value, irs::TokenLayout::TermsPos);
    if (!res.ok) {
      out.kind = SqlVerdict::Kind::Rejected;
      return out;
    }
    out.kind = SqlVerdict::Kind::Tokens;
    out.terms.reserve(res.tokens.size());
    for (const auto& token : res.tokens) {
      out.terms.push_back(token.term);
    }
  } catch (const std::exception& e) {
    out.kind = SqlVerdict::Kind::Error;
    out.error = e.what();
  }
  return out;
}

std::string Join(const std::vector<std::string>& terms) {
  std::string out = "[";
  for (size_t i = 0; i < terms.size(); ++i) {
    if (i != 0) {
      out += ", ";
    }
    out += Describe(terms[i]);
  }
  return out + "]";
}

bool OrderUnspecified(const std::string& expression) {
  return expression.contains("list_distinct") ||
         expression.contains("array_distinct");
}

std::optional<std::string> Compare(const Verdict& got, const SqlVerdict& want,
                                   bool unordered) {
  if (got.kind != want.kind) {
    return std::format(
      "tokenizer {} {} vs duckdb {} {}", SqlVerdictName(got.kind),
      got.kind == SqlVerdict::Kind::Error ? got.error : Join(got.terms),
      SqlVerdictName(want.kind),
      want.kind == SqlVerdict::Kind::Error ? want.error : Join(want.terms));
  }
  if (got.kind != SqlVerdict::Kind::Tokens) {
    return std::nullopt;
  }
  auto lhs = got.terms;
  auto rhs = want.terms;
  if (unordered) {
    std::ranges::sort(lhs);
    std::ranges::sort(rhs);
  }
  if (lhs != rhs) {
    return std::format("tokenizer terms {} vs duckdb {}", Join(got.terms),
                       Join(want.terms));
  }
  return std::nullopt;
}

bool BindClassError(std::string_view error) {
  return error.starts_with("Binder Error") ||
         error.starts_with("Parser Error") ||
         error.starts_with("Catalog Error") ||
         error.starts_with("Not implemented Error");
}

struct Stats {
  size_t expressions = 0;
  size_t parsed = 0;
  size_t bound = 0;
  size_t type_rejected = 0;
  size_t values = 0;
  size_t rejected_values = 0;
  size_t error_values = 0;
  size_t probed = 0;
  std::map<std::string, size_t> bind_reasons;
};

void Account(Stats& stats, const Built& built) {
  ++stats.expressions;
  if (built.stage != Stage::ParseFailed) {
    ++stats.parsed;
  }
  if (built.stage == Stage::Bound) {
    ++stats.bound;
  }
  if (built.stage == Stage::BindFailed) {
    if (built.error.find("must return VARCHAR") != std::string::npos) {
      ++stats.type_rejected;
    }
    ++stats.bind_reasons[built.error.substr(0, 72)];
  }
}

std::optional<std::string> CompareValues(const std::string& expression,
                                         irs::analysis::Tokenizer& tokenizer,
                                         std::span<const std::string> inputs,
                                         Stats& stats, bool& saw_error) {
  auto& oracle = SqlOracle::Shared();
  const bool unordered = OrderUnspecified(expression);
  for (const auto& input : inputs) {
    if (!IsValidUtf8(input)) {
      continue;
    }
    const auto got = RunTokenizer(tokenizer, input);
    const auto want = oracle.Evaluate(expression, input);
    if (auto err = Compare(got, want, unordered)) {
      return std::format("value {}: {}", Describe(input), *err);
    }
    ++stats.values;
    if (got.kind == SqlVerdict::Kind::Rejected) {
      ++stats.rejected_values;
    }
    if (got.kind == SqlVerdict::Kind::Error) {
      ++stats.error_values;
      saw_error = true;
      const auto again = RunTokenizer(tokenizer, "abc");
      if (auto err =
            Compare(again, oracle.Evaluate(expression, "abc"), unordered)) {
        return std::format("after error on {}: no recovery, {}",
                           Describe(input), *err);
      }
    }
  }
  return std::nullopt;
}

std::optional<std::string> ProbeValues(const std::string& expression,
                                       uint64_t seed,
                                       std::span<const std::string> inputs) {
  const Spec spec{.name = std::format("sql-fuzz[{}]", expression),
                  .config =
                    [expression] {
                      return irs::analysis::TokenizerConfig{
                        SqlTokenizer::Options{.expression = expression}};
                    },
                  .dict = InputDictionary(),
                  .model = Model::Sql,
                  .params = {.expression = expression},
                  .utf8_only = true};
  Probe probe{spec};
  if (!probe.valid()) {
    return "probe could not construct the tokenizer";
  }
  size_t i = 0;
  for (const auto& input : inputs) {
    if (!IsValidUtf8(input)) {
      continue;
    }
    try {
      if (auto err = probe(input, (i % 8) == 0)) {
        return std::format("value {}: {} (seed {})", Describe(input), *err,
                           seed);
      }
    } catch (const std::exception& e) {
      return std::format("value {}: probe threw {}", Describe(input), e.what());
    }
    ++i;
  }
  return std::nullopt;
}

std::vector<std::string> Inputs(const std::string& expression, uint64_t seed,
                                size_t count) {
  std::vector<std::string> inputs = SeedValues();
  Mutator mutator{seed ^ NameSeed(expression), InputDictionary(), 1024};
  for (size_t k = inputs.size(); k < count; ++k) {
    inputs.push_back(k % 3 == 0
                       ? mutator.Generate()
                       : mutator.Mutate(SeedValues()[k % SeedValues().size()]));
  }
  return inputs;
}

std::optional<std::string> CheckBound(const std::string& expression,
                                      irs::analysis::Tokenizer& tokenizer,
                                      uint64_t seed, size_t value_count,
                                      Stats& stats) {
  const auto traits = tokenizer.Traits();
  if (traits.output != duckdb::LogicalTypeId::VARCHAR &&
      traits.output != duckdb::LogicalTypeId::BLOB) {
    return std::format("bound tokenizer declares output type {}",
                       static_cast<int>(traits.output));
  }
  if (auto err = SqlOracle::Shared().Prepare(expression);
      err && BindClassError(*err)) {
    return std::format("tokenizer bound it but duckdb cannot prepare it: {}",
                       *err);
  }
  const auto inputs = Inputs(expression, seed, value_count);
  bool saw_error = false;
  if (auto err =
        CompareValues(expression, tokenizer, inputs, stats, saw_error)) {
    return err;
  }
  if (saw_error || OrderUnspecified(expression)) {
    return std::nullopt;
  }
  ++stats.probed;
  return ProbeValues(expression, seed, inputs);
}

void PrintSummary(const char* what, const Stats& stats, double seconds) {
  std::printf(
    "[   FUZZ   ] %s: %zu expressions, %zu parsed, %zu bound (%zu type "
    "rejections), %zu values compared (%zu rejected, %zu errors), %zu probed, "
    "%.1fs\n",
    what, stats.expressions, stats.parsed, stats.bound, stats.type_rejected,
    stats.values, stats.rejected_values, stats.error_values, stats.probed,
    seconds);
  std::vector<std::pair<size_t, std::string>> reasons;
  for (const auto& [reason, count] : stats.bind_reasons) {
    reasons.emplace_back(count, reason);
  }
  std::ranges::sort(reasons, std::greater<>{});
  for (size_t i = 0; i < reasons.size() && i < 6; ++i) {
    std::printf("[   FUZZ   ]   %4zu x bind rejection: %s\n", reasons[i].first,
                reasons[i].second.c_str());
  }
}

}  // namespace

TEST(SqlTokenizerFuzz, Expressions) {
  const auto seed = Seed();
  const auto count = static_cast<size_t>(EnvU64("SQL_FUZZ_EXPRESSIONS", 256));
  const auto values = static_cast<size_t>(EnvU64("SQL_FUZZ_VALUES", 48));
  const char* only = std::getenv("SQL_FUZZ_EXPRESSION");
  const auto start = Clock::now();
  ExprGen gen{seed};
  Stats stats;

  for (size_t n = 0; n < (only ? size_t{1} : count); ++n) {
    const std::string expression = only ? std::string{only} : gen.Next();
    SCOPED_TRACE(testing::Message()
                 << "expression: " << expression << "\n  seed: " << seed
                 << "\n  reproduce with TOKENIZER_FUZZ_SEED=" << seed
                 << " SQL_FUZZ_EXPRESSION='" << expression << "'");
    const auto built = Build(expression);
    Account(stats, built);
    ASSERT_NE(Stage::UnexpectedException, built.stage) << built.error;
    if (built.stage != Stage::Bound) {
      continue;
    }
    const auto err =
      CheckBound(expression, *built.tokenizer, seed, values, stats);
    ASSERT_FALSE(err.has_value()) << *err;
  }
  PrintSummary("sql expressions", stats, Seconds(start));
}

TEST(SqlTokenizerFuzz, AdversarialExpressions) {
  const auto seed = Seed();
  const auto start = Clock::now();
  Stats stats;
  for (const auto& expression : AdversarialExpressions()) {
    SCOPED_TRACE(testing::Message() << "expression: " << expression);
    const auto built = Build(expression);
    Account(stats, built);
    EXPECT_NE(Stage::UnexpectedException, built.stage) << built.error;
    if (built.stage != Stage::Bound) {
      continue;
    }
    const auto err = CheckBound(expression, *built.tokenizer, seed,
                                SeedValues().size(), stats);
    EXPECT_FALSE(err.has_value()) << err.value_or(std::string{});
  }
  PrintSummary("sql adversarial", stats, Seconds(start));
}
