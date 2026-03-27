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

#include "executor.hpp"

#include <iresearch/parser/parser.h>

#include <cstring>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/store/store_utils.hpp>

namespace bench {

Executor::Executor(std::string_view path, const BenchConfig& config)
  : _scorer{irs::scorers::Get(config.scorer,
                              irs::Type<irs::text_format::Json>::get(),
                              config.scorer_options, false)},
    _tokenizer{irs::analysis::analyzers::Get(
      config.tokenizer, irs::Type<irs::text_format::Json>::get(),
      config.tokenizer_options)},
    _format{irs::formats::Get(config.format_name, false)},
    _dir{path},
    _reader{
      irs::DirectoryReader(_dir, _format, {.scorers = {&_scorer_ptr, 1}})} {}

size_t Executor::ExecuteTopK(size_t k, std::string_view query) {
  ResetResults(k);
  auto filter = ParseFilter(query);
  if (!filter) {
    _result_count = 0;
    return 0;
  }
  auto count =
    irs::ExecuteTopK(_reader, *filter, *_scorer, k,
                     {.index = 0, .strict = true}, std::span{_results});
  _result_count = std::min<size_t>(k, count);
  return count;
}

size_t Executor::ExecuteTopKWithCount(size_t k, std::string_view query) {
  ResetResults(k);
  auto filter = ParseFilter(query);
  if (!filter) {
    _result_count = 0;
    return 0;
  }
  auto count = irs::ExecuteTopKWithCount(_reader, *filter, *_scorer, k,
                                         std::span{_results});
  _result_count = std::min<size_t>(k, count);
  return count;
}

size_t Executor::ExecuteCount(std::string_view query) {
  auto filter = ParseFilter(query);
  if (!filter) {
    return 0;
  }
  auto prepared = filter->prepare({.index = _reader});
  if (!prepared) {
    return 0;
  }
  size_t count = 0;
  for (auto& segment : _reader) {
    auto docs = prepared->execute({.segment = segment});
    count += docs->count();
  }
  return count;
}

irs::Filter::ptr Executor::ParseFilter(std::string_view str) {
  auto root = std::make_unique<irs::MixedBooleanFilter>();
  sdb::ParserContext context{*root, "text", *_tokenizer};
  auto r = sdb::ParseQuery(context, str);
  if (!r.ok()) {
    return {};
  }
  auto& opt = root->GetOptional();
  auto& req = root->GetRequired();
  if (opt.size() == 1 && req.empty()) {
    return opt.PopBack();
  }
  if (req.size() == 1 && opt.empty()) {
    return req.PopBack();
  }
  return root;
}

}  // namespace bench
