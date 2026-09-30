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

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

enum class PatternKind : uint8_t {
  RegexpPerl,
  RegexpPosixEre,
  Wildcard,
  Fused,
};

constexpr PatternKind RegexpPattern(RegexpSyntax syntax) noexcept {
  return syntax == RegexpSyntax::Perl ? PatternKind::RegexpPerl
                                      : PatternKind::RegexpPosixEre;
}

class PatternCache {
 public:
  static constexpr size_t kDefaultCapacity = 0;

  static PatternCache& Instance();

  std::shared_ptr<const RegexpAcceptor> Get(bytes_view pattern,
                                            PatternKind kind);

  void SetCapacity(size_t bytes);
  size_t Capacity() const;
  size_t Bytes() const;
  size_t Size() const;
  void Clear();

 private:
  struct Entry {
    std::string key;
    std::shared_ptr<const RegexpAcceptor> acceptor;
    size_t bytes;
  };

  using Entries = std::list<Entry>;

  std::shared_ptr<const RegexpAcceptor> TouchLocked(Entries::iterator entry);
  void EvictLocked();

  mutable std::mutex _mutex;
  Entries _entries;
  absl::flat_hash_map<std::string_view, Entries::iterator> _index;
  std::atomic_size_t _capacity{kDefaultCapacity};
  size_t _bytes{0};
};

}  // namespace irs
