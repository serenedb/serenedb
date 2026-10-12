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

#include "collation_tokenizer.hpp"

#include <limits>
#include <string>

#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::analysis {
namespace {

std::string CollationName(const duckdb::text::Locale& locale) {
  if (locale.IsBogus()) {
    THROW_SQL_ERROR(ERR_MSG("collate_tokens: invalid locale"));
  }
  std::string collation;
  locale.GetCollation(collation);
  return collation;
}

}  // namespace

CollationTokenizer::CollationTokenizer(const Options& options)
  : _collator{CollationName(options.locale)} {}

Tokenizer::ptr CollationTokenizer::Make(Options opts) {
  return std::make_unique<CollationTokenizer>(opts);
}

template<TokenLayout Layout, typename Sink>
bool CollationTokenizer::DoFill(duckdb::string_t raw, Sink& sink) {
  _collator.GetSortKey(raw.GetData(), raw.GetSize(), _buffer);
  const auto size = _buffer.key.size() - 1;
  if (size > std::numeric_limits<uint32_t>::max()) [[unlikely]] {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    ERR_MSG("collate_tokens: the collation key of a ",
                            raw.GetSize(), "-byte value exceeds the limit of ",
                            std::numeric_limits<uint32_t>::max(), " bytes"));
  }
  sink.template Emit<Layout>(_buffer.key.data(), static_cast<uint32_t>(size));
  return true;
}

template class TypedTokenizer<CollationTokenizer>;
template struct TypedTokenStage<CollationTokenizer>;

}  // namespace irs::analysis
