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

#include <iresearch/index/directory_reader.hpp>
#include <iresearch/utils/bytes_utils.hpp>
#include <span>

namespace sdb::connector {

class UniqueProbe {
 public:
  UniqueProbe(const irs::DirectoryReader& reader,
              std::span<const irs::field_id> key_fields,
              std::span<const int64_t> deleted_rowids);

  bool Exists(std::span<const irs::bstring> key_terms);

  // Whether any segment holds `term` for key column `field`, ignoring deletes.
  // A false rules the term out without reading a posting list; a true still
  // needs `Exists` to settle it.
  bool MayHold(size_t field, irs::bytes_view term) const;

 private:
  struct Segment {
    const irs::SubReader* sub;
    irs::DocumentMask::Iterator mask;
    std::vector<const irs::TermReader*> fields;
    irs::DocumentMaskBuilder deleted;
  };

  std::vector<Segment> _segments;
  std::vector<irs::doc_id_t> _hits;
  std::vector<irs::doc_id_t> _next;
};

}  // namespace sdb::connector
