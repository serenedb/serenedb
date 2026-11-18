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

#include "directory_cleaner.hpp"

#include "directory.hpp"
#include "directory_attributes.hpp"

namespace irs {

size_t DirectoryCleaner::clean(Directory& dir,
                               const removal_acceptor_t& acceptor) {
  size_t remove_count = 0;
  dir.attributes().refs().refs().ForgetUnused(
    [&dir, &acceptor, &remove_count](std::string_view filename) {
      if (!acceptor(filename)) {
        return false;
      }
      if (dir.remove(filename)) {
        ++remove_count;
        return true;
      }
      bool exists = true;
      return dir.exists(exists, filename) && !exists;
    });
  return remove_count;
}

}  // namespace irs
