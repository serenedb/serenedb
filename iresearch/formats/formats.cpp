////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "formats.hpp"

#include "iresearch/formats/index_meta_reader.hpp"
#include "iresearch/formats/index_meta_writer.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting/reader.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/formats/segment_meta_reader.hpp"
#include "iresearch/formats/segment_meta_writer.hpp"

namespace irs {

IndexMetaWriter::ptr MakeIndexMetaWriter() {
  return std::make_unique<IndexMetaWriterImpl>();
}

IndexMetaReader::ptr GetIndexMetaReader() {
  static IndexMetaReaderImpl gInstance;
  return memory::to_managed<IndexMetaReader>(gInstance);
}

SegmentMetaWriter::ptr GetSegmentMetaWriter() {
  static SegmentMetaWriterImpl gInstance;
  return memory::to_managed<SegmentMetaWriter>(gInstance);
}

SegmentMetaReader::ptr GetSegmentMetaReader() {
  static SegmentMetaReaderImpl gInstance;
  return memory::to_managed<SegmentMetaReader>(gInstance);
}

PostingsWriter::ptr MakePostingsWriter(bool compaction,
                                       IResourceManager& resource_manager) {
  return std::make_unique<PostingsWriterImpl<FormatTraits128>>(
    compaction, resource_manager);
}

PostingsReader::ptr MakePostingsReader() {
  return std::make_unique<PostingsReaderImpl<FormatTraits128>>();
}

}  // namespace irs
