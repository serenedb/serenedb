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

#include "iresearch/store/mapped_input.hpp"

#include "iresearch/store/directory.hpp"
#include "iresearch/utils/math_utils.hpp"

namespace irs {

MappedFile::MappedFile(std::unique_ptr<duckdb::MemoryMappedFile> mapping,
                       IResourceManager* file_descriptors)
  : _mapping{std::move(mapping)}, _file_descriptors{file_descriptors} {
  SDB_ASSERT(_mapping);
  if (_mapping->Size() != 0) {
    _residency.Reset(math::DivCeil64(_mapping->Size(), file_utils::kPage));
  }
}

MappedFile::~MappedFile() {
  _mapping.reset();
  if (_file_descriptors) {
    _file_descriptors->Decrease(1);
  }
}

bytes_view MappedFile::Bytes() const noexcept {
  if (_mapping->Size() == 0) {
    return {};
  }
  return {_mapping->GetData(0, _mapping->Size()), _mapping->Size()};
}

MappedInput::MappedInput(std::shared_ptr<MappedFile> file) noexcept
  : BytesViewInput{file->Bytes()}, _file{std::move(file)} {}

void MappedInput::Prefetch(uint64_t offset, uint64_t count) const noexcept {
  _file->Mapping().Prefetch(offset, count);
}

void MappedInput::Advise(IOAdvice advice) noexcept {
  _file->Mapping().Advise(ToMappedFileAccess(advice));
}

bool MappedInput::Warm(uint64_t offset, uint64_t count) const noexcept {
  if (count == 0 || offset >= Length()) {
    return false;
  }
  const auto& residency = _file->Residency();
  return residency.Valid(file_utils::ResidencyEpoch()) &&
         residency.Test(offset / file_utils::kPage, LastPage(offset, count));
}

void MappedInput::MarkWarm(uint64_t offset, uint64_t count) const noexcept {
  if (count == 0 || offset >= Length()) {
    return;
  }
  const auto& residency = _file->Residency();
  const auto epoch = file_utils::ResidencyEpoch();
  if (residency.Valid(epoch) || residency.Adopt(epoch)) {
    residency.Set(offset / file_utils::kPage, LastPage(offset, count));
  }
}

uint64_t MappedInput::LastPage(uint64_t offset, uint64_t count) const noexcept {
  return (std::min<uint64_t>(offset + count, Length()) - 1) / file_utils::kPage;
}

duckdb::MappedFileAccess ToMappedFileAccess(IOAdvice advice) noexcept {
  switch (advice) {
    case IOAdvice::SEQUENTIAL:
      return duckdb::MappedFileAccess::SEQUENTIAL;
    case IOAdvice::RANDOM:
      return duckdb::MappedFileAccess::RANDOM;
    default:
      return duckdb::MappedFileAccess::NORMAL;
  }
}

}  // namespace irs
