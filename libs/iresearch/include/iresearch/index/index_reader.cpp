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

#include "index_reader.hpp"

#include "basics/resource_manager.hpp"

namespace irs {
namespace {

const SegmentInfo kEmptyInfo;

struct EmptySubReader final : SubReader {
  uint64_t CountMappedMemory() const final { return 0; }

  NormReader::ptr norms(field_id) const final { return {}; }
  const SegmentInfo& Meta() const final { return kEmptyInfo; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  DocIterator::ptr docs_iterator() const final { return DocIterator::empty(); }
  const irs::TermReader* field(field_id) const final { return nullptr; }
  std::span<const field_id> field_ids() const final { return {}; }
};

const EmptySubReader kEmpty;

}  // namespace

const SubReader& SubReader::empty() noexcept { return kEmpty; }

#ifdef SDB_DEV
IResourceManager IResourceManager::gForbidden;
#endif
IResourceManager IResourceManager::gNoop;
ResourceManagementOptions ResourceManagementOptions::gDefault;

}  // namespace irs
