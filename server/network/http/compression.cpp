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

#include "network/http/compression.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string_utils.hpp>
#include <optional>
#include <string>
#include <utility>

#include "network/http/codecs/codec.h"

namespace sdb::network::http {
namespace {

// Declaration order IS the server's preference order, used whenever the
// client accepts more than one of these equally.
constexpr std::array kContentCodings = {
  ContentCoding{
    .token = "zstd", .make = MakeZstdEncoder, .make_decoder = MakeZstdDecoder},
  ContentCoding{.token = "br",
                .make = MakeBrotliEncoder,
                .make_decoder = MakeBrotliDecoder},
  ContentCoding{
    .token = "gzip", .make = MakeGzipEncoder, .make_decoder = MakeGzipDecoder},
  ContentCoding{.token = "deflate",
                .make = MakeDeflateEncoder,
                .make_decoder = MakeDeflateDecoder},
  ContentCoding{
    .token = "zxc", .make = MakeZxcEncoder, .make_decoder = MakeZxcDecoder},
  ContentCoding{
    .token = "lz4", .make = MakeLz4Encoder, .make_decoder = MakeLz4Decoder},
  ContentCoding{.token = "snappy",
                .make = MakeSnappyEncoder,
                .make_decoder = MakeSnappyDecoder},
};

struct AcceptedCoding {
  std::string_view token;
  int level = kNoLevel;
  double quality = 1.0;
};

// codings = coding [ ";" parameter ]... ; only the "q" weight is defined for
// Accept-Encoding, but other parameters must not hide it.
// https://www.rfc-editor.org/rfc/rfc9110#name-quality-values
std::optional<double> ParseQValue(std::string_view text) {
  if (text.empty() || (text[0] != '0' && text[0] != '1')) {
    return std::nullopt;
  }
  const bool one = text[0] == '1';
  text.remove_prefix(1);
  if (text.empty()) {
    return one ? 1.0 : 0.0;
  }
  if (text[0] != '.' || text.size() > 4) {
    return std::nullopt;
  }
  text.remove_prefix(1);
  double fraction = 0.0;
  double scale = 0.1;
  for (const char digit : text) {
    if (digit < '0' || digit > '9' || (one && digit != '0')) {
      return std::nullopt;
    }
    fraction += (digit - '0') * scale;
    scale /= 10;
  }
  return one ? 1.0 : fraction;
}

std::string_view CanonicalToken(std::string_view token) {
  return absl::EqualsIgnoreCase(token, "x-gzip") ? "gzip" : token;
}

bool SplitLevel(std::string_view& token, int& level) {
  const auto open = token.find('(');
  if (open == std::string_view::npos) {
    return true;
  }
  if (token.back() != ')') {
    return false;
  }
  int value = 0;
  if (!absl::SimpleAtoi(token.substr(open + 1, token.size() - open - 2),
                        &value)) {
    return false;
  }
  token = absl::StripTrailingAsciiWhitespace(token.substr(0, open));
  level = value;
  return true;
}

std::optional<AcceptedCoding> ParseAccepted(std::string_view element) {
  element = absl::StripAsciiWhitespace(element);
  if (element.empty()) {
    return std::nullopt;
  }
  AcceptedCoding accepted;
  bool first = true;
  for (std::string_view part : absl::StrSplit(element, ';')) {
    part = absl::StripAsciiWhitespace(part);
    if (std::exchange(first, false)) {
      if (!SplitLevel(part, accepted.level)) {
        return std::nullopt;
      }
      accepted.token = CanonicalToken(part);
      continue;
    }
    if (absl::StartsWithIgnoreCase(part, "q=")) {
      part.remove_prefix(2);
      const auto quality = ParseQValue(part);
      if (!quality) {
        return std::nullopt;
      }
      accepted.quality = *quality;
    }
  }
  return accepted;
}

}  // namespace

void StringOutput::Write(size_t capacity,
                         absl::FunctionRef<size_t(uint8_t*)> fill) {
  const size_t size = _out.size();
  irs::utils::StrResize(_out, size + capacity);
  _out.resize(size + fill(reinterpret_cast<uint8_t*>(_out.data() + size)));
}

void ContentEncoder::Encode(std::string_view in, bool finish,
                            absl::FunctionRef<void(std::string_view)> sink) {
  std::string out;
  StringOutput output{out};
  Encode(in, finish, output);
  if (!out.empty()) {
    sink(out);
  }
}

void ContentEncoder::EncodeAll(std::string_view in, std::string& out) {
  out.clear();
  StringOutput output{out};
  Encode(in, true, output);
}

const ContentCoding* FindContentCoding(std::string_view token) {
  token = CanonicalToken(token);
  for (const auto& coding : kContentCodings) {
    if (absl::EqualsIgnoreCase(coding.token, token)) {
      return &coding;
    }
  }
  return nullptr;
}

Negotiation NegotiateContentCoding(std::string_view accept_encoding) {
  if (absl::StripAsciiWhitespace(accept_encoding).empty()) {
    // No field, or an empty one: the body goes out uncompressed. (RFC 9110
    // lets a server compress when the field is absent; clients that cannot
    // decode outnumber the bytes that would save.)
    return {};
  }

  std::array<std::optional<double>, kContentCodings.size()> weights;
  std::array<int, kContentCodings.size()> levels{};
  std::optional<double> identity;
  std::optional<double> wildcard;
  for (const auto element : absl::StrSplit(accept_encoding, ',')) {
    if (absl::StripAsciiWhitespace(element).empty()) {
      continue;
    }
    const auto parsed = ParseAccepted(element);
    if (!parsed) {
      return {.acceptance = Acceptance::Malformed};
    }
    if (parsed->token == "*") {
      wildcard = parsed->quality;
      continue;
    }
    if (absl::EqualsIgnoreCase(parsed->token, "identity")) {
      if (!identity) {
        identity = parsed->quality;
      }
      continue;
    }
    for (size_t i = 0; i < kContentCodings.size(); ++i) {
      if (!weights[i] &&
          absl::EqualsIgnoreCase(kContentCodings[i].token, parsed->token)) {
        weights[i] = parsed->quality;
        levels[i] = parsed->level;
      }
    }
  }

  // An explicit weight wins over the wildcard, whatever their order.
  const auto quality_of = [&](const std::optional<double>& weight) {
    return weight ? weight : wildcard;
  };

  // "the acceptable content coding with the highest non-zero qvalue is
  // preferred"; server order breaks ties, which is the usual case since most
  // clients send no weights at all.
  size_t best = kContentCodings.size();
  double best_quality = 0.0;
  for (size_t i = 0; i < kContentCodings.size(); ++i) {
    const double quality = quality_of(weights[i]).value_or(0.0);
    if (quality > best_quality) {
      best = i;
      best_quality = quality;
    }
  }
  if (best != kContentCodings.size()) {
    return {.coding = &kContentCodings[best], .level = levels[best]};
  }

  // Nothing we encode is acceptable. The uncompressed form still is, unless
  // the client ruled it out too -- then there is no representation to send.
  // https://www.rfc-editor.org/rfc/rfc9110#name-accept-encoding
  if (quality_of(identity).value_or(1.0) > 0.0) {
    return {};
  }
  return {.acceptance = Acceptance::NotAcceptable};
}

std::optional<ContentCodings> ParseContentEncoding(
  std::string_view content_encoding) {
  ContentCodings codings;
  for (const auto element : absl::StrSplit(content_encoding, ',')) {
    auto token = absl::StripAsciiWhitespace(element);
    int level = kNoLevel;
    if (!SplitLevel(token, level)) {
      return std::nullopt;
    }
    if (token.empty() || absl::EqualsIgnoreCase(token, "identity")) {
      continue;
    }
    const auto* coding = FindContentCoding(token);
    if (coding == nullptr) {
      return std::nullopt;
    }
    if (codings.size() == kMaxContentCodings) {
      return std::nullopt;
    }
    codings.push_back(coding);
  }
  return codings;
}

void DecodeContent(const message::SequenceView& body,
                   std::span<const ContentCoding* const> codings,
                   absl::FunctionRef<void(std::string_view)> sink,
                   size_t max_bytes) {
  if (codings.empty()) {
    for (const auto chunk : body) {
      sink({static_cast<const char*>(chunk.data()), chunk.size()});
    }
    return;
  }
  std::string staged;
  for (size_t stage = codings.size(); stage-- > 0;) {
    std::string next;
    size_t total = 0;
    const auto emit = [&](std::string_view part) {
      total += part.size();
      if (total > max_bytes) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
          ERR_MSG("HTTP request body decodes past ", max_bytes, " bytes"));
      }
      if (stage == 0) {
        sink(part);
      } else {
        next.append(part);
      }
    };
    auto decoder = codings[stage]->make_decoder();
    if (stage + 1 == codings.size()) {
      for (const auto chunk : body) {
        decoder->Decode({static_cast<const char*>(chunk.data()), chunk.size()},
                        false, emit);
      }
      decoder->Decode({}, true, emit);
    } else {
      decoder->Decode(staged, true, emit);
    }
    staged = std::move(next);
  }
}

}  // namespace sdb::network::http
