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

#include "search/hot_keys.h"

#include <absl/cleanup/cleanup.h>

#include <iresearch/utils/type_limits.hpp>

namespace sdb::search {

void HotKeys::Add(irs::field_id field, std::string_view key,
                  irs::doc_id_t doc) {
  absl::MutexLock lock{&_mutex};
  auto& entry = _fields[field];
  const auto [it, inserted] =
    entry.newest.try_emplace(MakeHashedKey(key), irs::doc_limits::invalid());
  if (!inserted) {
    entry.previous.emplace(doc, it->second);
    it->second = doc;
    return;
  }
  absl::Cleanup erase = [&] { entry.newest.erase(it); };
  it->first.key = _keys.AddBlob(key.data(), key.size());
  std::move(erase).Cancel();
  it->second = doc;
}

void HotKeys::Clear() noexcept {
  absl::MutexLock lock{&_mutex};
  _fields.clear();
  _keys.Destroy();
}

void HotKeys::ForEach(irs::field_id field, std::string_view key,
                      absl::FunctionRef<bool(irs::doc_id_t)> visit) const {
  absl::ReaderMutexLock lock{&_mutex};
  const auto found = _fields.find(field);
  if (found == _fields.end()) {
    return;
  }
  const auto& entry = found->second;
  const auto it = entry.newest.find(MakeHashedKey(key));
  if (it == entry.newest.end()) {
    return;
  }
  for (auto doc = it->second; visit(doc);) {
    const auto previous = entry.previous.find(doc);
    if (previous == entry.previous.end()) {
      return;
    }
    doc = previous->second;
  }
}

}  // namespace sdb::search
