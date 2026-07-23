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

#include <cstdint>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>

#include "iresearch/analysis/batch/token_batch.hpp"

namespace irs {

// Terms-only drain appending every token to the LIST(VARCHAR) vector's
// child starting at the construction offset (growing the list as needed);
// offset() after writer.Finish() is the new child offset and keeps
// accumulating across values, so one sink serves a whole result vector.
// The caller owns the final ListVector::SetListSize.
class ListVectorSink final : public TokenConsumer {
 public:
  ListVectorSink(duckdb::Vector& list, uint64_t offset)
    : writer{*this}, _list(&list), _offset(offset) {}

  void Consume(TokenBatch& batch, std::span<const DocRun> /*runs*/) final {
    auto& child = duckdb::ListVector::GetEntry(*_list);
    for (uint32_t i = 0; i < batch.count; ++i) {
      if (_offset >= duckdb::ListVector::GetListCapacity(*_list)) {
        duckdb::ListVector::SetListSize(*_list, _offset);
        duckdb::ListVector::Reserve(
          *_list, duckdb::ListVector::GetListCapacity(*_list) * 2);
      }
      auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child);
      const auto& t = batch.terms[i];
      data[_offset] =
        duckdb::StringVector::AddStringOrBlob(child, t.GetData(), t.GetSize());
      ++_offset;
    }
  }

  uint64_t offset() const noexcept { return _offset; }

  TokenWriter writer;

 private:
  duckdb::Vector* _list;
  uint64_t _offset;
};

}  // namespace irs
