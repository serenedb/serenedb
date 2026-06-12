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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "directory_reader.hpp"

#include <absl/container/flat_hash_map.h>

#include "basics/singleton.hpp"
#include "iresearch/index/directory_reader_impl.hpp"
#include "iresearch/index/segment_reader.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

DirectoryReader::DirectoryReader(
  const Directory& dir, Format::ptr codec /*= nullptr*/,
  const IndexReaderOptions& opts /*= directory_reader_options()*/)
  : _impl{DirectoryReaderImpl::Open(dir, opts, std::move(codec), nullptr)} {}

DirectoryReader::DirectoryReader(
  std::shared_ptr<const DirectoryReaderImpl>&& impl) noexcept
  : _impl{std::move(impl)} {}

DirectoryReader::DirectoryReader(const DirectoryReader& other) noexcept
  : _impl{std::atomic_load_explicit(&other._impl, std::memory_order_acquire)} {}

DirectoryReader& DirectoryReader::operator=(
  const DirectoryReader& other) noexcept {
  if (this != &other) {
    // make a copy
    auto impl =
      std::atomic_load_explicit(&other._impl, std::memory_order_acquire);

    std::atomic_store_explicit(&_impl, impl, std::memory_order_release);
  }

  return *this;
}

const SubReader& DirectoryReader::operator[](size_t i) const {
  return (*_impl)[i];
}

uint64_t DirectoryReader::CountMappedMemory() const {
  return _impl->CountMappedMemory();
}

uint64_t DirectoryReader::docs_count() const { return _impl->docs_count(); }

uint64_t DirectoryReader::live_docs_count() const {
  return _impl->live_docs_count();
}

const duckdb::BaseStatistics* DirectoryReader::GetColumnStats(
  field_id field) const {
  return _impl->GetColumnStats(field);
}

size_t DirectoryReader::size() const { return _impl->size(); }

const DirectoryMeta& DirectoryReader::Meta() const { return _impl->Meta(); }

DirectoryReader DirectoryReader::Reopen() const {
  // make a copy
  auto impl = std::atomic_load_explicit(&_impl, std::memory_order_acquire);

  return DirectoryReader{DirectoryReaderImpl::Open(
    impl->Dir(), impl->Options(), impl->Codec(), std::move(impl))};
}

}  // namespace irs
