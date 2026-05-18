////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "catalog/tokenizer.h"

#include <vpack/builder.h>
#include <vpack/slice.h>

#include <expected>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>

#include "basics/assert.h"
#include "basics/errors.h"
#include "catalog/search_analyzer_impl.h"

namespace sdb::catalog {

ResultOr<Tokenizer::TokenizerWrapper> Tokenizer::GetTokenizer() {
  absl::MutexLock lock{&_m};
  if (_pool.empty()) {
    auto analyzer = CreateAnalyzer();
    if (!analyzer) {
      return std::unexpected<Result>{std::in_place, ERROR_INTERNAL,
                                     "Failed to create analyzer"};
    }
    return TokenizerWrapper{analyzer.release(), Deleter{this}};
  }
  auto analyzer = std::move(_pool.back());
  SDB_ASSERT(analyzer);
  _pool.pop_back();
  return TokenizerWrapper{analyzer.release(), Deleter{this}};
}

void Tokenizer::PushTokenizer(irs::analysis::Analyzer::ptr analyzer) noexcept {
  SDB_ASSERT(analyzer);
  absl::MutexLock lock{&_m};
  _pool.push_back(std::move(analyzer));
}

vpack::Slice Tokenizer::Slice() const noexcept {
  return vpack::Slice{reinterpret_cast<const uint8_t*>(_data.data())};
}

irs::analysis::Analyzer::ptr Tokenizer::CreateAnalyzer() const {
  irs::analysis::Analyzer::ptr output;
  irs::analysis::analyzers::MakeAnalyzer(Slice(), output);
  return output;
}

Tokenizer::Tokenizer(ObjectId id, std::string_view name,
                     search::Features features, uint32_t norm_row_group_size,
                     std::string data)
  : SchemaObject{{}, {}, {}, id, name, ObjectType::Tokenizer},
    _data{std::move(data)},
    _features{features},
    _norm_row_group_size{norm_row_group_size} {}

std::shared_ptr<Tokenizer> Tokenizer::ReadInternal(vpack::Slice slice,
                                                   ReadContext ctx) {
  auto name = slice.get("name");
  if (!name.isString()) {
    return nullptr;
  }
  auto features_slice = slice.get("features");
  search::Features features;
  if (auto r = features.FromVPack(features_slice); !r.ok()) {
    return nullptr;
  }
  const auto norm_row_group_size_slice = slice.get("norm_row_group_size");
  const uint32_t norm_row_group_size =
    norm_row_group_size_slice.isNumber<uint32_t>()
      ? norm_row_group_size_slice.getNumber<uint32_t>()
      : DEFAULT_ROW_GROUP_SIZE;
  return std::make_shared<Tokenizer>(
    ctx.id, name.stringView(), std::move(features), norm_row_group_size,
    std::string{slice.startAs<char>(), slice.byteSize()});
}

void Tokenizer::WriteInternal(vpack::Builder& b) const {
  b.openObject();
  WriteObject(b, [&](vpack::Builder& b) {
    auto slice = vpack::Slice{reinterpret_cast<const uint8_t*>(_data.data())};
    b.add("tokenizer", slice.get("tokenizer"));
    b.add("features");
    _features.ToVPack(b);
    b.add("norm_row_group_size", _norm_row_group_size);
  });
  b.close();
}

std::shared_ptr<Object> Tokenizer::Clone() const {
  vpack::Builder b;
  WriteInternal(b);
  return ReadInternal(b.slice(), {.id = GetId()});
}

}  // namespace sdb::catalog
