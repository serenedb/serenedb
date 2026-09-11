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

#include "tokenizer_fuzz_oracles.hpp"

#include <algorithm>
#include <cctype>
#include <duckdb.hpp>
#include <format>
#include <unordered_map>

#include "iresearch/utils/duckdb_engine.h"
#include "pipeline_reference.hpp"

namespace tests::fuzz {

struct SqlOracle::Impl {
  Impl() : con{sdb::DuckDBEngine::Instance().instance()} {
    auto result = con.Query(
      "SET disabled_optimizers = "
      "'expression_rewriter,statistics_propagation,in_clause,regex_range'");
    if (result->HasError()) {
      throw std::runtime_error{result->GetError()};
    }
  }

  duckdb::PreparedStatement* Statement(const std::string& expression,
                                       std::string& error) {
    auto it = prepared.find(expression);
    if (it == prepared.end()) {
      auto stmt = con.Prepare(std::format(
        "FROM (VALUES ($1::VARCHAR)) AS sql_oracle(input) SELECT {}",
        expression));
      it = prepared.emplace(expression, std::move(stmt)).first;
    }
    if (it->second->HasError()) {
      error = it->second->GetError();
      return nullptr;
    }
    return it->second.get();
  }

  duckdb::Connection con;
  std::unordered_map<std::string, duckdb::unique_ptr<duckdb::PreparedStatement>>
    prepared;
};

SqlOracle& SqlOracle::Shared() {
  static thread_local SqlOracle oracle;
  return oracle;
}

SqlOracle::SqlOracle() : _impl{std::make_unique<Impl>()} {}

SqlOracle::~SqlOracle() = default;

std::optional<std::string> SqlOracle::Prepare(const std::string& expression) {
  std::string error;
  if (_impl->Statement(expression, error) == nullptr) {
    return error;
  }
  return std::nullopt;
}

SqlVerdict SqlOracle::Evaluate(const std::string& expression,
                               std::string_view value) {
  SqlVerdict out;
  auto* stmt = _impl->Statement(expression, out.error);
  if (stmt == nullptr) {
    return out;
  }
  try {
    duckdb::vector<duckdb::Value> params;
    params.emplace_back(std::string{value});
    auto result = stmt->Execute(params, false);
    if (result->HasError()) {
      out.error = result->GetError();
      return out;
    }
    auto& rows = result->Cast<duckdb::MaterializedQueryResult>();
    if (rows.RowCount() != 1 || rows.ColumnCount() != 1) {
      out.error = std::format("oracle query returned {} rows x {} columns",
                              rows.RowCount(), rows.ColumnCount());
      return out;
    }
    const auto cell = rows.GetValue(0, 0);
    if (cell.IsNull()) {
      out.kind = SqlVerdict::Kind::Rejected;
      return out;
    }
    if (cell.type().id() == duckdb::LogicalTypeId::LIST) {
      for (const auto& element : duckdb::ListValue::GetChildren(cell)) {
        if (!element.IsNull()) {
          out.terms.push_back(duckdb::StringValue::Get(element));
        }
      }
    } else {
      out.terms.push_back(duckdb::StringValue::Get(cell));
    }
    out.kind = SqlVerdict::Kind::Tokens;
  } catch (const std::exception& e) {
    out.error = e.what();
    out.kind = SqlVerdict::Kind::Error;
  }
  return out;
}

std::string_view SqlVerdictName(SqlVerdict::Kind kind) noexcept {
  switch (kind) {
    case SqlVerdict::Kind::Rejected:
      return "rejected";
    case SqlVerdict::Kind::Tokens:
      return "tokens";
    case SqlVerdict::Kind::Error:
      return "error";
  }
  return "unknown";
}

namespace {

bool AllAscii(std::string_view value) noexcept {
  return std::ranges::none_of(
    value, [](char c) { return static_cast<unsigned char>(c) >= 0x80; });
}

std::string Convert(std::string_view in, irs::Case convert) {
  std::string out{in};
  if (convert == irs::Case::None) {
    return out;
  }
  for (auto& c : out) {
    const auto b = static_cast<unsigned char>(c);
    if (b >= 0x80) {
      continue;
    }
    c = convert == irs::Case::Lower ? static_cast<char>(std::tolower(b))
                                    : static_cast<char>(std::toupper(b));
  }
  return out;
}

bool In(const std::bitset<256>& set, char c) {
  return set[static_cast<unsigned char>(c)];
}

}  // namespace

std::bitset<256> ByteSet(std::string_view members, bool negate) {
  std::bitset<256> set;
  for (const char c : members) {
    set.set(static_cast<unsigned char>(c));
  }
  if (negate) {
    set.flip();
  }
  return set;
}

std::bitset<256> AlnumBytes() {
  std::bitset<256> set;
  for (unsigned char c = '0'; c <= '9'; ++c) {
    set.set(c);
  }
  for (unsigned char c = 'a'; c <= 'z'; ++c) {
    set.set(c);
  }
  for (unsigned char c = 'A'; c <= 'Z'; ++c) {
    set.set(c);
  }
  return set;
}

std::string_view ModelName(Model model) noexcept {
  switch (model) {
    case Model::None:
      return "none";
    case Model::Keyword:
      return "keyword";
    case Model::SplitChar:
      return "split_char";
    case Model::RunsOfSet:
      return "runs_of_set";
    case Model::NGramBytes:
      return "ngram_bytes";
    case Model::Chain:
      return "chain";
    case Model::Sql:
      return "sql";
  }
  return "unknown";
}

std::optional<std::vector<ModelToken>> ModelTokens(
  Model model, const ModelParams& params,
  std::span<const irs::analysis::Tokenizer::ptr> children,
  std::string_view value) {
  std::vector<ModelToken> out;
  switch (model) {
    case Model::None:
      return std::nullopt;

    case Model::Keyword:
      out.push_back(ModelToken{std::string{value}, 1});
      return out;

    case Model::SplitChar: {
      if (value.find('"') != std::string_view::npos) {
        return std::nullopt;
      }
      size_t begin = 0;
      uint32_t pos = 1;
      for (size_t i = 0; i <= value.size(); ++i) {
        if (i == value.size() || value[i] == params.delim) {
          out.push_back(
            ModelToken{std::string{value.substr(begin, i - begin)}, pos++});
          begin = i + 1;
        }
      }
      return out;
    }

    case Model::RunsOfSet: {
      if (params.ascii_only && !AllAscii(value)) {
        return std::nullopt;
      }
      uint32_t pos = 1;
      size_t i = 0;
      while (i < value.size()) {
        while (i < value.size() && !In(params.token_bytes, value[i])) {
          ++i;
        }
        const auto begin = i;
        while (i < value.size() && In(params.token_bytes, value[i])) {
          ++i;
        }
        if (i != begin) {
          out.push_back(ModelToken{
            Convert(value.substr(begin, i - begin), params.convert), pos++});
        }
      }
      return out;
    }

    case Model::NGramBytes: {
      const auto n = value.size();
      if (params.min_gram == 0 || params.min_gram > n) {
        return out;
      }
      for (size_t start = 0; start + params.min_gram <= n; ++start) {
        const auto max_len = std::min(params.max_gram, n - start);
        for (auto len = params.min_gram; len <= max_len; ++len) {
          out.push_back(ModelToken{std::string{value.substr(start, len)},
                                   static_cast<uint32_t>(start + 1)});
        }
      }
      return out;
    }

    case Model::Chain: {
      if (children.empty()) {
        return std::nullopt;
      }
      auto toks =
        ::tests::ChainReference(children, value, irs::TokenLayout::TermsPos);
      if (!toks) {
        return std::nullopt;
      }
      out.reserve(toks->size());
      for (auto& t : *toks) {
        out.push_back(ModelToken{std::move(t.term), t.pos});
      }
      return out;
    }

    case Model::Sql: {
      auto verdict = SqlOracle::Shared().Evaluate(params.expression, value);
      if (verdict.kind != SqlVerdict::Kind::Tokens) {
        return std::nullopt;
      }
      uint32_t pos = 1;
      out.reserve(verdict.terms.size());
      for (auto& term : verdict.terms) {
        out.push_back(ModelToken{std::move(term), pos++});
      }
      return out;
    }
  }
  return std::nullopt;
}

}  // namespace tests::fuzz
