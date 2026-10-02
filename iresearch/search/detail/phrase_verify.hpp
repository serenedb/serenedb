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
#include <optional>
#include <span>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/search/detail/phrase_slop_matcher.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct PhraseVerdict {
  uint32_t freq = 0;
  score_t scale = kNoBoost;
};

struct PhraseVerifyScratch {
  std::vector<std::vector<PosAttr::value_t>> slots;
  std::vector<PosAttr::value_t> valid;
  std::vector<PosAttr::value_t> next;
  std::vector<uint64_t> ways;
  std::vector<uint64_t> next_ways;
  detail::slop::MatchScratch slop;
  uint32_t state = 0;
  uint32_t freq = 0;
  bool sequence = false;
  bool count = false;
};

struct StoredText {
  field_id column = field_limits::invalid();
  std::function<analysis::Tokenizer::ptr()> tokenizer;

  bool operator==(const StoredText& rhs) const noexcept {
    return column == rhs.column;
  }
};

class PhraseVerifier {
 public:
  explicit PhraseVerifier(StoredText text,
                          std::optional<ByPhraseOptions> spec = std::nullopt)
    : _text{std::move(text)}, _spec{std::move(spec)} {
    SDB_ASSERT(_text.tokenizer);
  }

  const StoredText& Text() const noexcept { return _text; }

  const ByPhraseOptions* Spec() const noexcept {
    return _spec ? &*_spec : nullptr;
  }

  bool operator==(const PhraseVerifier& rhs) const noexcept = default;

 private:
  StoredText _text;
  std::optional<ByPhraseOptions> _spec;
};

class PhraseVerifyKernel {
 public:
  PhraseVerifyKernel(const ByPhraseOptions& phrase,
                     std::span<const std::vector<bstring>> expanded);

  PhraseVerifyKernel(PhraseVerifyKernel&&) = delete;
  PhraseVerifyKernel& operator=(PhraseVerifyKernel&&) = delete;

  bool Sloppy() const noexcept { return _slop != 0; }

  void Begin(bool dense, bool count, PhraseVerifyScratch& scratch) const;
  void Push(std::span<const duckdb::string_t> terms, const uint32_t* pos,
            PhraseVerifyScratch& scratch) const;
  bool End(PhraseVerifyScratch& scratch, PhraseVerdict& out) const;

 private:
  struct SlotList {
    uint32_t begin = 0;
    uint32_t size = 0;
  };

  void Finish(std::span<const uint32_t> slots);

  void PushSequence(std::span<const duckdb::string_t> terms,
                    PhraseVerifyScratch& scratch) const;
  void PushSlots(std::span<const duckdb::string_t> terms, const uint32_t* pos,
                 PhraseVerifyScratch& scratch) const;
  bool EndSlots(PhraseVerifyScratch& scratch, PhraseVerdict& out) const;

  std::vector<bstring> _owned;
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

class PhraseVerifySink final : public TokenConsumer {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPos;

  PhraseVerifySink(const PhraseVerifyKernel& kernel, TokenTraits producer)
    : _kernel{&kernel}, _dense{!producer.explicit_pos} {}

  bool Match(analysis::Tokenizer& tokenizer, ValueAnalyzer& analyzer,
             duckdb::string_t value, bool count, PhraseVerdict& out);

  void Prepare(duckdb::string_t) noexcept { _pos = 0; }
  void Discard() noexcept {}
  void Consume(TokenBatch& batch, DocRuns runs) final;

 private:
  const PhraseVerifyKernel* _kernel;
  PhraseVerifyScratch _scratch;
  uint32_t _pos = 0;
  bool _dense;
};

class PhraseTokenReader {
 public:
  PhraseTokenReader(const ColReader& col_reader, const ColumnReader& column,
                    analysis::Tokenizer::ptr tokenizer,
                    const PhraseVerifyKernel& kernel);

  PhraseTokenReader(PhraseTokenReader&&) = delete;
  PhraseTokenReader& operator=(PhraseTokenReader&&) = delete;

  bool Match(doc_id_t doc, bool count, PhraseVerdict& out);

 private:
  bool Fetch(doc_id_t doc, duckdb::string_t& value);

  ReadContext _ctx;
  const ColumnReader* _column;
  ColumnReader::ScanState _state;
  ColumnReader::VectorScratch _out;
  duckdb::SelectionVector _sel;
  analysis::Tokenizer::ptr _tokenizer;
  ValueAnalyzer _analyzer;
  PhraseVerifySink _sink;
  uint32_t _loads = 0;
};

}  // namespace irs
