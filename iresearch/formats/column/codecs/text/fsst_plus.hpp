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

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/fsst.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"

namespace irs::codecs::text::fsst_plus {

class CompressionState : public SortedCompressionState {
 public:
  static constexpr ByteCodec kCodec = ByteCodec::FsstPlus;

  CompressionState(const duckdb::LogicalType& type, DedupScratch& dedup,
                   EstimatorState& estimators, uint32_t target)
    : SortedCompressionState{kCodec, type, dedup, estimators, target} {}

  void FinalizeCompress();

 private:
  struct Node {
    uint32_t left;
    uint32_t right;
  };

  struct Range {
    uint32_t first;
    uint32_t last;
    uint32_t node;
  };

  void Cleave(uint32_t first_entry);

  fsst::Encoder<fsst::Fsst8> _codec;
  std::string _encoded;
  std::string _prefixes;
  std::string _side;
  std::vector<uint32_t> _encoded_lengths;
  std::vector<std::string_view> _codes;
  std::vector<uint32_t> _shared;
  std::vector<uint32_t> _ids;
  std::vector<uint32_t> _ends;
  std::vector<uint32_t> _lcp;
  std::vector<Node> _tree;
  std::vector<uint32_t> _order;
  std::vector<uint32_t> _size;
  std::vector<uint64_t> _saved;
  std::vector<uint8_t> _whole;
  std::vector<Range> _ranges;
};

}  // namespace irs::codecs::text::fsst_plus
