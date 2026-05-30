////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "minhash_tokenizer.hpp"

#include <absl/strings/escaping.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include "basics/logger/logger.h"
#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kParseError =
  ", failed to parse options for MinHashTokenizer";
constexpr OffsAttr kEmptyOffset;

constexpr uint32_t kMinHashes = 1;
constexpr std::string_view kNumHashes = "numHashes";

bool ParseNumHashes(vpack::Slice input, uint32_t& num_hashes) {
  SDB_ASSERT(input.isObject());
  input = input.get(kNumHashes);
  if (!input.isNumber<uint32_t>()) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat(kNumHashes, " attribute must be positive integer",
                           kParseError));
    return false;
  }
  num_hashes = input.getNumber<uint32_t>();
  if (num_hashes < kMinHashes) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat(kNumHashes, " attribute must be at least ",
                           kMinHashes, kParseError));
    return false;
  }
  return true;
}

bool ParseOptions(vpack::Slice slice, MinHashTokenizer::Options& options) {
  if (!slice.isObject()) {
    return false;
  }
  if (!ParseNumHashes(slice, options.num_hashes)) {
    return false;
  }
  if (!analyzers::MakeAnalyzer(slice, options.analyzer)) {
    SDB_ERROR(IRESEARCH, absl::StrCat("Invalid analyzer definition in ",
                                      slice_to_string(slice), kParseError));
    return false;
  }
  return true;
}

std::shared_ptr<vpack::Builder> ParseArgs(std::string_view args) try {
  return vpack::Parser::fromJson(args.data(), args.size());
} catch (const std::exception& e) {
  SDB_ERROR(IRESEARCH,
            absl::StrCat("Caught exception: ", e.what(), kParseError));
  return {};
} catch (...) {
  SDB_ERROR(IRESEARCH, absl::StrCat("Caught unknown exception", kParseError));
  return {};
}

Analyzer::ptr MakeImpl(vpack::Slice slice) {
  if (MinHashTokenizer::Options opts; ParseOptions(slice, opts)) {
    return std::make_unique<MinHashTokenizer>(std::move(opts));
  }
  return {};
}

bool NormalizeImpl(vpack::Slice input, vpack::Builder& output) {
  if (!input.isObject()) {
    return false;
  }
  uint32_t num_hashes = 0;
  if (!ParseNumHashes(input, num_hashes)) {
    return false;
  }
  vpack::ObjectBuilder scope{&output};
  output.add(kNumHashes, num_hashes);
  if (!analyzers::NormalizeAnalyzer(input, output)) {
    SDB_ERROR(IRESEARCH, absl::StrCat("Invalid analyzer definition in ",
                                      slice_to_string(input), kParseError));
    return false;
  }
  return true;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  if (args.empty()) {
    SDB_ERROR(IRESEARCH, absl::StrCat("Empty arguments", kParseError));
    return {};
  }
  vpack::Slice slice{reinterpret_cast<const uint8_t*>(args.data())};
  return MakeImpl(slice);
}

Analyzer::ptr MakeJson(std::string_view args) {
  if (args.empty()) {
    SDB_ERROR(IRESEARCH, absl::StrCat("Empty arguments", kParseError));
    return {};
  }
  auto builder = ParseArgs(args);
  if (!builder) {
    return {};
  }
  return MakeImpl(builder->slice());
}

bool NormalizeVPack(std::string_view args, std::string& definition) {
  if (args.empty()) {
    SDB_ERROR(IRESEARCH, absl::StrCat("Empty arguments", kParseError));
    return false;
  }
  vpack::Slice input{reinterpret_cast<const uint8_t*>(args.data())};
  vpack::Builder output;
  if (!NormalizeImpl(input, output)) {
    return false;
  }
  definition.assign(output.slice().startAs<char>(), output.slice().byteSize());
  return true;
}

bool NormalizeJson(std::string_view args, std::string& definition) {
  if (args.empty()) {
    SDB_ERROR(IRESEARCH, absl::StrCat("Empty arguments", kParseError));
    return false;
  }
  auto input = ParseArgs(args);
  if (!input) {
    return {};
  }
  vpack::Builder output;
  if (!NormalizeImpl(input->slice(), output)) {
    return false;
  }
  definition = output.toString();
  return !definition.empty();
}

}  // namespace

void MinHashTokenizer::init() {
  REGISTER_ANALYZER_VPACK(MinHashTokenizer, MakeVPack, NormalizeVPack);
  REGISTER_ANALYZER_JSON(MinHashTokenizer, MakeJson, NormalizeJson);
}

MinHashTokenizer::MinHashTokenizer(Options&& opts)
  : _opts{std::move(opts)}, _minhash{_opts.num_hashes} {
  if (!_opts.analyzer) {
    // Fallback to default implementation
    _opts.analyzer = std::make_unique<StringTokenizer>();
  }

  _term = irs::get<TermAttr>(*_opts.analyzer);

  if (!_term) [[unlikely]] {
    _opts.analyzer = std::make_unique<EmptyAnalyzer>();
  }

  _offset = irs::get<OffsAttr>(*_opts.analyzer);

  std::get<TermAttr>(_attrs).value = {
    reinterpret_cast<const byte_type*>(_buf.data()), _buf.size()};
}

bool MinHashTokenizer::next() {
  if (_begin == _end) {
    return false;
  }

  const auto value = absl::little_endian::FromHost(*_begin);

  [[maybe_unused]] const size_t length = absl::Base64EscapeInternal(
    reinterpret_cast<const uint8_t*>(&value), sizeof value, _buf.data(),
    _buf.size(), absl::kBase64Chars, false);
  SDB_ASSERT(length == _buf.size());

  std::get<IncAttr>(_attrs).value = std::exchange(_next_inc.value, 0);
  ++_begin;

  return true;
}

bool MinHashTokenizer::reset(std::string_view data) {
  _begin = _end = {};
  if (_opts.analyzer->reset(data)) {
    ComputeSignature();
    return true;
  }
  return false;
}

void MinHashTokenizer::ComputeSignature() {
  _minhash.Clear();
  _next_inc.value = 1;

  if (_opts.analyzer->next()) {
    SDB_ASSERT(_term);

    const auto* offs = _offset ? _offset : &kEmptyOffset;
    auto& [start, end] = std::get<OffsAttr>(_attrs);
    start = offs->start;
    end = offs->end;

    do {
      const std::string_view value = ViewCast<char>(_term->value);
      const auto hash_value =
        VPACK_HASH(value.data(), value.size(), 0xdeadbeef);

      _minhash.Insert(hash_value);
      end = offs->end;
    } while (_opts.analyzer->next());

    _begin = std::begin(_minhash);
    _end = std::end(_minhash);
  }
}

}  // namespace irs::analysis
