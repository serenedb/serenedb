////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "tokenizers.hpp"

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/storage/arena_allocator.hpp>

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {
namespace analysis {

Tokenizer::~Tokenizer() = default;

}  // namespace analysis

template<TokenLayout Layout>
bool StringTokenizer::DoFill(std::string_view value, TokenEmitter& sink) {
  sink.Emit<Layout>(
    duckdb::string_t{value.data(), static_cast<uint32_t>(value.size())}, 0,
    static_cast<uint32_t>(value.size()));
  return true;
}

template class analysis::TypedTokenizer<StringTokenizer>;

}  // namespace irs
