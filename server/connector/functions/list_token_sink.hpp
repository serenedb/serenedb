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

#pragma once

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>

namespace sdb::connector {

class ListTokenSink {
 public:
  explicit ListTokenSink(duckdb::Vector& result_list)
    : _result_list(result_list) {}
  ~ListTokenSink() { Finalize(); }

  duckdb::idx_t Offset() const noexcept { return _offset; }

  void Bind(irs::analysis::Tokenizer& tokenizer) { _stream = &tokenizer; }

  void Tokenize(duckdb::string_t text) {
    SDB_ASSERT(_stream);
    if (!_analyzer.Analyze(*_stream, text, _tokens)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("error while preparing tokenizer"));
    }
    Append(_tokens.terms());
  }

  void Tokenize(irs::analysis::Tokenizer& tokenizer, duckdb::string_t text) {
    Bind(tokenizer);
    Tokenize(text);
  }

 private:
  void Append(std::span<const duckdb::string_t> terms) {
    auto& child = duckdb::ListVector::GetEntry(_result_list);
    const auto needed = _offset + terms.size();
    if (needed > duckdb::ListVector::GetListCapacity(_result_list)) {
      duckdb::ListVector::SetListSize(_result_list, _offset);
      duckdb::ListVector::Reserve(_result_list, needed * 2);
    }
    auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child);
    for (const auto& term : terms) {
      data[_offset++] = duckdb::StringVector::AddStringOrBlob(
        child, term.GetData(), term.GetSize());
    }
  }

  void Finalize() noexcept {
    duckdb::ListVector::SetListSize(_result_list, _offset);
  }

  duckdb::Vector& _result_list;
  duckdb::idx_t _offset = 0;
  irs::analysis::Tokenizer* _stream = nullptr;
  irs::ValueAnalyzer _analyzer;
  irs::ValueTokens<> _tokens;
};

}  // namespace sdb::connector
