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

#pragma once

#include "iresearch/formats/column/common.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index/burst_trie.hpp"
#include "iresearch/formats/index_meta_reader.hpp"
#include "iresearch/formats/index_meta_writer.hpp"
#include "iresearch/formats/posting/reader.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/formats/segment_meta_reader.hpp"
#include "iresearch/formats/segment_meta_writer.hpp"

namespace irs {

class FormatBase : public Format {
 public:
  IndexMetaWriter::ptr get_index_meta_writer() const final {
    return std::make_unique<IndexMetaWriterImpl>(
      IndexMetaWriterImpl::kFormatMax);
  }
  IndexMetaReader::ptr get_index_meta_reader() const final {
    // can reuse stateless reader
    static IndexMetaReaderImpl gInstance;
    return memory::to_managed<IndexMetaReader>(gInstance);
  }

  SegmentMetaWriter::ptr get_segment_meta_writer() const final {
    // can reuse stateless writer
    static SegmentMetaWriterImpl gInstance{SegmentMetaWriterImpl::kFormatMax};
    return memory::to_managed<SegmentMetaWriter>(gInstance);
  }
  SegmentMetaReader::ptr get_segment_meta_reader() const final {
    // can reuse stateless writer
    static SegmentMetaReaderImpl gInstance;
    return memory::to_managed<SegmentMetaReader>(gInstance);
  }

  FieldWriter::ptr get_field_writer(
    bool consolidation, IResourceManager& resource_manager) const final {
    return burst_trie::MakeWriter(
      burst_trie::Version::Min,
      get_postings_writer(consolidation, resource_manager), consolidation,
      resource_manager);
  }
  FieldReader::ptr get_field_reader(
    IResourceManager& resource_manager) const final {
    return burst_trie::MakeReader(get_postings_reader(), resource_manager);
  }

  ColumnstoreWriter::ptr get_columnstore_writer(
    bool consolidation, IResourceManager& resource_manager) const final {
    return columnstore2::MakeWriter(columnstore2::Version::Min, consolidation,
                                    resource_manager);
  }
  ColumnstoreReader::ptr get_columnstore_reader() const final {
    return columnstore2::MakeReader();
  }
};

template<typename F>
class FormatImpl final : public FormatBase {
 public:
  using FormatTraits = F;

  static constexpr std::string_view type_name() noexcept {
    return FormatTraits::kName;
  }

  static ptr make() {
    static const FormatImpl kInstance;
    return {Format::ptr{}, &kInstance};
  }

  PostingsWriter::ptr get_postings_writer(
    bool consolidation, IResourceManager& resource_manager) const final {
    return std::make_unique<PostingsWriterImpl<FormatTraits>>(
      PostingsFormat::WandSimd, consolidation, resource_manager);
  }
  PostingsReader::ptr get_postings_reader() const final {
    return std::make_unique<PostingsReaderImpl<FormatTraits>>();
  }

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<FormatImpl>::id();
  }
};

}  // namespace irs
