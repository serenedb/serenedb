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

#include "iresearch/formats/column/codecs/bytes/zstd.hpp"

#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/text/blob.hpp"
#include "iresearch/formats/column/codecs/text/dictionary_scan.tpp"
#include "iresearch/formats/column/codecs/text/plain_scan.tpp"
#include "iresearch/formats/column/codecs/text/text_function.hpp"

/*
zstd frames in Data (string_layout.hpp draws the segments): frame 0 holds up
to 32 KiB and is the dictionary of the later frames of up to 32 KiB; or the
same frames, each compressed with the segment's trained dictionary; or
64 KiB frames without a dictionary. An entry longer than a frame holds is a
frame of its own.
*/

namespace irs::codecs {
namespace {

template<typename Rows>
using Builder = text::blob::CompressionState<Zstd, Rows>;

}  // namespace

constinit const Codec kZstdCodec =
  text::StringCodec<text::PlainScan<text::blob::Scan<Zstd>>,
                    Builder<text::PlainRows>>();

constinit const Codec kDictZstdCodec = text::StringCodec<
  text::DictionaryScan<text::blob::Scan<Zstd>>, Builder<text::DictionaryRows>,
  text::blob::kFrameFunction<Builder<text::DictionaryRows>>>();

}  // namespace irs::codecs
