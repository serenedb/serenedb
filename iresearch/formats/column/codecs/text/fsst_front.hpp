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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/bitpacking.hpp>
#include <span>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/fsst.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text::fsst_front {

template<typename Impl, typename Rows>
class CompressionState final : public SortedCompressionState<Rows> {
  using Base = SortedCompressionState<Rows>;

 public:
  static constexpr ByteCodec kCodec = Impl::kCodec;

  CompressionState(ColumnDataCheckpointData& data, DedupScratch& dedup)
    : Base{kCodec, data.type, dedup, data.tuning.strings,
           data.params.segment_target},
      _codec{data.Get<fsst::SharedEncoder<Impl>>().encoder},
      _shares{Impl::kSharedTable && data.shared} {}

  void FinalizeCompress();

 private:
  using Base::_frames;
  using Base::_out;
  using Base::_rows;
  using Base::_target;

  static constexpr uint64_t kEncodeExpansion = 2;
  static constexpr uint64_t kEncodeSlack = 8;

  fsst::Encoder<Impl>& _codec;
  bool _shares;
  std::vector<uint32_t> _lcp_stream;
};

template<typename Impl, typename Rows>
void CompressionState<Impl, Rows>::FinalizeCompress() {
  this->Sort();
  const auto lengths = _rows.Lengths();
  const auto rows = _rows.View();
  const auto entries = rows.entries;
  const auto first_entry = rows.first_entry;
  auto& data = _out->bytes;
  _lcp_stream.assign(first_entry, 0);
  _frames.clear();
  uint32_t max_lcp = 0;
  uint64_t frame_raw = 0;
  uint64_t frame_first = 0;
  uint64_t suffixes = 0;
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto& entry = entries[i];
    const auto size = entry.GetSize();
    if (frame_raw != 0 && frame_raw + size > kFsstFrameRawBytes) {
      _frames.emplace_back(static_cast<uint32_t>(frame_first),
                           static_cast<uint32_t>(frame_raw), 0, 0);
      frame_raw = 0;
      frame_first = i;
    }
    const uint32_t lcp = (i - frame_first) % kChainRestart == 0
                           ? 0
                           : CommonPrefix(entries[i - 1], entry);
    _lcp_stream.emplace_back(lcp);
    max_lcp = std::max(max_lcp, lcp);
    frame_raw += size;
    suffixes += size - lcp;
  }
  if (!entries.empty()) {
    _frames.emplace_back(static_cast<uint32_t>(frame_first),
                         static_cast<uint32_t>(frame_raw), 0, 0);
  }
  const auto skip = std::span{_lcp_stream}.subspan(first_entry);
  size_t done = 0;
  const auto encode = [&](uint64_t capacity) {
    auto* out = data.Extend(capacity);
    const auto encoded =
      _codec.Encode(entries.subspan(done), skip.subspan(done), out, capacity,
                    lengths.subspan(done));
    data.Shrink(capacity - encoded.bytes);
    for (auto i = done; i < done + encoded.strings; ++i) {
      suffixes -= entries[i].GetSize() - skip[i];
    }
    done += encoded.strings;
  };
  encode(suffixes + kEncodeSlack);
  if (done != entries.size()) {
    encode(kEncodeExpansion * suffixes + kEncodeSlack);
    SDB_ENSURE(done == entries.size(), "fsst: compression ran out of space");
  }
  size_t off = 0;
  uint32_t max_enc = 0;
  for (size_t f = 0; f < _frames.size(); ++f) {
    auto& frame = _frames[f];
    const size_t end =
      f + 1 < _frames.size() ? _frames[f + 1].first_entry : entries.size();
    frame.comp_off = static_cast<uint32_t>(off);
    for (size_t e = frame.first_entry; e < end; ++e) {
      off += lengths[e];
      max_enc = std::max(max_enc, lengths[e]);
    }
    frame.comp_len = static_cast<uint32_t>(off - frame.comp_off);
    frame.first_entry += first_entry;
  }
  _lcp_stream.resize(
    duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(EntryCount(rows)),
    0);
  const auto table = _codec.SymbolTable();
  const auto symtab = _shares ? std::string_view{} : table;
  RecordRatio(this->History(), rows.raw, data.Size() + symtab.size(), _target);
  _rows.Write(
    {
      .codec = kCodec,
      .frames = _frames,
      .max_length = max_enc,
      .lcps = _lcp_stream,
      .max_lcp = max_lcp,
      .symtab = symtab,
      .shared = _shares ? table : std::string_view{},
    },
    *_out);
}

}  // namespace irs::codecs::text::fsst_front
