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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

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
      accepted.token = part;
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

const ContentCoding* FindContentCoding(std::string_view token) {
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

  std::vector<AcceptedCoding> accepted;
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
    } else {
      accepted.push_back(*parsed);
    }
  }

  // An explicit weight wins over the wildcard, whatever their order.
  const auto quality_of = [&](std::string_view token) -> std::optional<double> {
    for (const auto& candidate : accepted) {
      if (absl::EqualsIgnoreCase(candidate.token, token)) {
        return candidate.quality;
      }
    }
    return wildcard;
  };

  // "the acceptable content coding with the highest non-zero qvalue is
  // preferred"; server order breaks ties, which is the usual case since most
  // clients send no weights at all.
  const ContentCoding* best = nullptr;
  double best_quality = 0.0;
  for (const auto& coding : kContentCodings) {
    const double quality = quality_of(coding.token).value_or(0.0);
    if (quality > best_quality) {
      best = &coding;
      best_quality = quality;
    }
  }
  if (best != nullptr) {
    return {.coding = best};
  }

  // Nothing we encode is acceptable. The uncompressed form still is, unless
  // the client ruled it out too -- then there is no representation to send.
  // https://www.rfc-editor.org/rfc/rfc9110#name-accept-encoding
  if (quality_of("identity").value_or(1.0) > 0.0) {
    return {};
  }
  return {.acceptance = Acceptance::NotAcceptable};
}

std::optional<std::vector<const ContentCoding*>> ParseContentEncoding(
  std::string_view content_encoding) {
  std::vector<const ContentCoding*> codings;
  for (const auto element : absl::StrSplit(content_encoding, ',')) {
    const auto token = absl::StripAsciiWhitespace(element);
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
