////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "tests_param.hpp"

#include "tests_shared.hpp"
#ifdef IRESEARCH_URING
#include "iresearch/store/async_directory.hpp"
#endif
#include "basics/file_utils_ext.hpp"
#include "iresearch/store/fs_directory.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/mmap_directory.hpp"

namespace tests {

std::string ToString(dir_generator_f generator) {
  if (generator == &MemoryDirectory) {
    return "memory";
  }

  if (generator == &FsDirectory) {
    return "fs";
  }

  if (generator == &MmapDirectory) {
    return "mmap";
  }

#ifdef IRESEARCH_URING
  if (generator == &AsyncDirectory) {
    return "async";
  }
#endif

  return "unknown";
}

std::shared_ptr<irs::Directory> MemoryDirectory(
  const TestBase* test, irs::DirectoryAttributes attrs) {
  return std::make_shared<irs::MemoryDirectory>(
    std::move(attrs), test ? test->GetResourceManager().options
                           : irs::ResourceManagementOptions::gDefault);
}

std::shared_ptr<irs::Directory> FsDirectory(const TestBase* test,
                                            irs::DirectoryAttributes attrs) {
  return MakePhysicalDirectory<irs::FSDirectory>(test, std::move(attrs));
}

std::shared_ptr<irs::Directory> MmapDirectory(const TestBase* test,
                                              irs::DirectoryAttributes attrs) {
  return MakePhysicalDirectory<irs::MMapDirectory>(test, std::move(attrs));
}

#ifdef IRESEARCH_URING
std::shared_ptr<irs::Directory> AsyncDirectory(const TestBase* test,
                                               irs::DirectoryAttributes attrs) {
  return MakePhysicalDirectory<irs::AsyncDirectory>(test, std::move(attrs));
}
#endif

}  // namespace tests
