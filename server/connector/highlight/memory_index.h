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

#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <memory>

namespace sdb::connector::highlight {

class MemoryIndex {
 public:
  MemoryIndex();

  template<typename WriteDocs>
  std::shared_ptr<const irs::SubReader> IndexChunk(size_t row_count,
                                                   WriteDocs&& write_docs) {
    irs::IndexWriterOptions opts;
    opts.lock_repository = false;
    auto writer =
      irs::IndexWriter::Make(_dir, _codec, irs::OpenMode::kOmCreate, opts);
    {
      irs::IndexWriter::Transaction trx{*writer};
      auto doc = trx.Insert(/*disable_flush=*/false,
                            static_cast<irs::doc_id_t>(row_count));
      doc.NextFieldBatch();
      write_docs(doc);
    }
    writer->Commit();
    auto reader = writer->GetSnapshot();
    if (reader.size() == 0) {
      return {std::shared_ptr<const irs::SubReader>{},
              &irs::SubReader::empty()};
    }
    return {reader.GetImpl(), &reader[0]};
  }

 private:
  irs::Format::ptr _codec;
  irs::MemoryDirectory _dir;
};

}  // namespace sdb::connector::highlight
