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

#include "connector/functions/search.h"

#include <duckdb/common/exception.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/utils/string.hpp>

#include "catalog/catalog.h"
#include "catalog/tokenizer.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

void SearchStubFn(duckdb::DataChunk& /*args*/,
                  duckdb::ExpressionState& /*state*/,
                  duckdb::Vector& /*result*/) {
  throw duckdb::InvalidInputException(
    "Inverted index function called outside inverted index context. "
    "Use in WHERE clause on a table with an inverted index.");
}

void Bm25StubFn(duckdb::DataChunk& /*args*/, duckdb::ExpressionState& /*state*/,
                duckdb::Vector& /*result*/) {
  throw duckdb::InvalidInputException(
    "bm25() requires an inverted index scan in the same sub-query");
}

void TfidfStubFn(duckdb::DataChunk& /*args*/,
                 duckdb::ExpressionState& /*state*/,
                 duckdb::Vector& /*result*/) {
  throw duckdb::InvalidInputException(
    "tfidf() requires an inverted index scan in the same sub-query");
}

// ts_lexize(dict_name VARCHAR, token VARCHAR) -> VARCHAR[]
// Runs `token` through the named text search dictionary and returns
// the resulting lexemes as a VARCHAR array. Mirrors pg_catalog.ts_lexize().
void TsLexizeFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                      duckdb::Vector& result) {
  auto count = args.size();
  auto& context = state.GetContext();
  auto& conn_ctx = GetSereneDBContext(context);

  auto db_id = conn_ctx.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  duckdb::UnifiedVectorFormat dict_format, text_format;
  args.data[0].ToUnifiedFormat(count, dict_format);
  args.data[1].ToUnifiedFormat(count, text_format);

  auto* dict_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(dict_format);
  auto* text_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(text_format);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);

  auto* list_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = duckdb::FlatVector::Validity(result);

  // Collect tokens per row first (tokenizer output is ephemeral).
  std::vector<std::vector<std::string>> row_tokens(count);
  duckdb::idx_t total_tokens = 0;

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto dict_idx = dict_format.sel->get_index(i);
    auto text_idx = text_format.sel->get_index(i);

    if (!dict_format.validity.RowIsValid(dict_idx) ||
        !text_format.validity.RowIsValid(text_idx)) {
      result_validity.SetInvalid(i);
      list_entries[i] = {total_tokens, 0};
      continue;
    }

    std::string_view dict_name_sv{dict_data[dict_idx].GetData(),
                                  dict_data[dict_idx].GetSize()};
    std::string_view text_sv{text_data[text_idx].GetData(),
                             text_data[text_idx].GetSize()};

    auto name = pg::ParseObjectName(dict_name_sv, current_schema);

    auto dict = snapshot->GetTokenizer(db_id, name.schema, name.relation);
    if (!dict) {
      throw duckdb::InvalidInputException(
        "text search dictionary \"%s\" does not exist",
        std::string{dict_name_sv});
    }

    auto tokenizer_result = dict->GetTokenizer();
    if (!tokenizer_result) {
      throw duckdb::InvalidInputException(
        "failed to get tokenizer: %s",
        std::string{tokenizer_result.error().errorMessage()});
    }

    auto& tokenizer = *tokenizer_result;
    if (!tokenizer->reset(text_sv)) {
      throw duckdb::InvalidInputException("error while preparing tokenizer");
    }

    auto* term = irs::get<irs::TermAttr>(*tokenizer);
    while (tokenizer->next()) {
      auto char_view = irs::ViewCast<char>(term->value);
      row_tokens[i].emplace_back(char_view.data(), char_view.size());
    }
    total_tokens += row_tokens[i].size();
  }

  // Fill result list vector.
  duckdb::ListVector::Reserve(result, total_tokens);
  auto& child = duckdb::ListVector::GetEntry(result);
  auto* child_data =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child);

  duckdb::idx_t offset = 0;
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (!result_validity.RowIsValid(i)) {
      continue;
    }
    list_entries[i].offset = offset;
    list_entries[i].length = row_tokens[i].size();
    for (const auto& tok : row_tokens[i]) {
      child_data[offset++] =
        duckdb::StringVector::AddString(child, tok.c_str(), tok.size());
    }
  }
  duckdb::ListVector::SetListSize(result, total_tokens);
}

}  // namespace

// Functions normally executed by inverted indexes. If rejected by an index the
// query fails with the "outside inverted index context" message above.
void RegisterSearchFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  // phrase(field, target) -> bool
  loader.RegisterFunction(duckdb::ScalarFunction(
    std::string{kPhrase},
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN, SearchStubFn));

  // term_eq/lt/lte/gte/gt/like(field, target) -> bool
  for (auto name : {kTermEq, kTermLt, kTermLe, kTermGe, kTermGt, kTermLike}) {
    loader.RegisterFunction(duckdb::ScalarFunction(
      std::string{name},
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
  }

  // term_in(field, values...) -> bool  (variadic: 1 fixed VARCHAR + N VARCHAR)
  {
    duckdb::ScalarFunction fn(std::string{kTermIn},
                              {duckdb::LogicalType::VARCHAR},
                              duckdb::LogicalType::BOOLEAN, SearchStubFn);
    fn.varargs = duckdb::LogicalType::VARCHAR;
    loader.RegisterFunction(std::move(fn));
  }

  // ngram_match(field, target[, threshold]) -> bool
  {
    duckdb::ScalarFunctionSet ngram{std::string{kNgramMatch}};
    ngram.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    ngram.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    loader.RegisterFunction(std::move(ngram));
  }

  // levenshtein_match(field, target, distance[, transpositions[, maxTerms[,
  // prefix]]]) -> bool
  {
    duckdb::ScalarFunctionSet lev{std::string{kLevenshteinMatch}};
    lev.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    lev.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    lev.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN,
       duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    lev.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN,
       duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN, SearchStubFn));
    loader.RegisterFunction(std::move(lev));
  }

  // boost(expr, boost_value) -> bool
  loader.RegisterFunction(duckdb::ScalarFunction(
    std::string{kBoost},
    {duckdb::LogicalType::BOOLEAN, duckdb::LogicalType::DOUBLE},
    duckdb::LogicalType::BOOLEAN, SearchStubFn));

  // bm25(tableoid) / bm25(tableoid, k1, b) -> DOUBLE -- emits the BM25
  // score per row for the scan identified by tableoid. Parameters are
  // extracted at compile time by the iresearch_plan rule; defaults
  // follow iresearch's Bm25 (k1 = 1.2, b = 0.75).
  {
    duckdb::ScalarFunctionSet set{std::string{kBm25}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, Bm25StubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::DOUBLE,
       duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::FLOAT, Bm25StubFn));
    loader.RegisterFunction(std::move(set));
  }

  // tfidf(tableoid) / tfidf(tableoid, with_norms) -> DOUBLE -- emits
  // TF-IDF. `with_norms` toggles length normalisation (default false).
  {
    duckdb::ScalarFunctionSet set{std::string{kTfidf}};
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT}, duckdb::LogicalType::FLOAT, TfidfStubFn));
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BOOLEAN},
      duckdb::LogicalType::FLOAT, TfidfStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // offsets(col) -> BIGINT[] -- emit position pairs (start, end) for
  // matched terms in `col` per row. List elements alternate start/end
  // (so length is 2*N for N positions). Claimed by the iresearch_plan
  // rule, which identifies the scan via the column ref's
  // binding.table_index.
  {
    duckdb::ScalarFunctionSet set{std::string{kOffsets}};
    // offsets(col) -- auto-anchor; legacy single-index queries.
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT), SearchStubFn));
    // offsets(tableoid, col) -- explicit anchor, consistent with bm25/tfidf.
    set.AddFunction(duckdb::ScalarFunction(
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT), SearchStubFn));
    loader.RegisterFunction(std::move(set));
  }

  // ts_lexize(dict_name, token) -> VARCHAR[]
  // Runs a single token through the named text search dictionary.
  {
    duckdb::ScalarFunction fn(
      "ts_lexize", {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR),
      TsLexizeFunction);
    fn.null_handling = duckdb::FunctionNullHandling::SPECIAL_HANDLING;
    loader.RegisterFunction(std::move(fn));
  }
}

}  // namespace sdb::connector
