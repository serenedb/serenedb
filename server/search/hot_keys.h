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

#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <iresearch/types.hpp>
#include <string>
#include <string_view>

namespace sdb::search {

// The constraint keys of the rows a transaction still buffers, which have no
// term dictionary yet, per key field.
class HotKeys {
 public:
  void Add(irs::field_id field, std::string_view key, irs::doc_id_t doc);
  void Clear() noexcept;

  // Visits the rows holding `key`, newest first, until `visit` returns false.
  void ForEach(irs::field_id field, std::string_view key,
               absl::FunctionRef<bool(irs::doc_id_t)> visit) const;

 private:
  struct Field {
    absl::flat_hash_map<std::string, irs::doc_id_t> newest;
    absl::flat_hash_map<irs::doc_id_t, irs::doc_id_t> previous;
  };

  mutable absl::Mutex _mutex;
  absl::flat_hash_map<irs::field_id, Field> _fields ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::search
