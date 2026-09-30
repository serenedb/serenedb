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

#include <absl/container/flat_hash_map.h>

#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/search/detail/phrase_slop_matcher.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct SubReader;

struct PhraseDocTokens {
  std::vector<bytes_view> terms;
  std::vector<PosAttr::value_t> positions;

  void Clear() noexcept {
    terms.clear();
    positions.clear();
  }

  void Push(bytes_view term, PosAttr::value_t pos) {
    terms.push_back(term);
    positions.push_back(pos);
  }
};

struct PhraseVerdict {
  uint32_t freq = 0;
  score_t scale = kNoBoost;
};

struct PhraseVerifyScratch {
  std::vector<std::vector<PosAttr::value_t>> slots;
  std::vector<PosAttr::value_t> valid;
  std::vector<PosAttr::value_t> next;
  detail::slop::MatchScratch slop;
};

class PhraseTokenSource {
 public:
  virtual ~PhraseTokenSource() = default;

  virtual bool Load(doc_id_t doc, PhraseDocTokens& out) = 0;
};

class PhraseTokenSourceFactory {
 public:
  virtual ~PhraseTokenSourceFactory() = default;

  virtual std::unique_ptr<PhraseTokenSource> Open(
    const SubReader& segment) const = 0;

  virtual std::string_view Name() const noexcept = 0;

  virtual bool Equals(const PhraseTokenSourceFactory& other) const noexcept = 0;
};

class PhraseVerifier {
 public:
  explicit PhraseVerifier(
    std::shared_ptr<const PhraseTokenSourceFactory> source,
    std::optional<ByPhraseOptions> spec = std::nullopt)
    : _source{std::move(source)}, _spec{std::move(spec)} {
    SDB_ASSERT(_source);
  }

  const PhraseTokenSourceFactory& Source() const noexcept { return *_source; }

  const ByPhraseOptions* Spec() const noexcept {
    return _spec ? &*_spec : nullptr;
  }

  bool operator==(const PhraseVerifier& rhs) const noexcept;

 private:
  std::shared_ptr<const PhraseTokenSourceFactory> _source;
  std::optional<ByPhraseOptions> _spec;
};

class StoredValueSourceFactory final : public PhraseTokenSourceFactory {
 public:
  using MakeTokenizer = std::function<analysis::Tokenizer::ptr()>;

  StoredValueSourceFactory(field_id column, MakeTokenizer make_tokenizer)
    : _column{column}, _make_tokenizer{std::move(make_tokenizer)} {}

  std::unique_ptr<PhraseTokenSource> Open(const SubReader& segment) const final;

  std::string_view Name() const noexcept final { return "column"; }

  bool Equals(const PhraseTokenSourceFactory& other) const noexcept final;

 private:
  field_id _column;
  MakeTokenizer _make_tokenizer;
};

class PhraseVerifyKernel {
 public:
  PhraseVerifyKernel(const ByPhraseOptions& phrase,
                     std::span<const std::vector<bstring>> expanded);

  PhraseVerifyKernel(PhraseVerifyKernel&&) = delete;
  PhraseVerifyKernel& operator=(PhraseVerifyKernel&&) = delete;

  bool Sloppy() const noexcept { return _slop != 0; }

  bool Match(const PhraseDocTokens& doc, bool count,
             PhraseVerifyScratch& scratch, PhraseVerdict& out) const;

 private:
  struct SlotList {
    uint32_t begin = 0;
    uint32_t size = 0;
  };

  void Accept(bytes_view term, uint32_t slot);
  void Finish();

  bool MatchSequence(std::span<const bytes_view> terms, bool count,
                     PhraseVerdict& out) const;
  bool MatchSlots(const PhraseDocTokens& doc, bool count,
                  PhraseVerifyScratch& scratch, PhraseVerdict& out) const;

  std::vector<bstring> _owned;
  std::vector<uint32_t> _owned_slots;
  absl::flat_hash_map<bytes_view, SlotList> _accept;
  std::vector<uint32_t> _slot_ids;
  std::vector<PosAttr::value_t> _offs_min;
  std::vector<PosAttr::value_t> _offs_max;
  std::vector<PosAttr::value_t> _steps;
  std::vector<uint32_t> _groups;
  std::vector<bytes_view> _sequence;
  std::vector<uint32_t> _failure;
  PosAttr::value_t _slop = 0;
};

}  // namespace irs
