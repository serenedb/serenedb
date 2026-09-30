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

#include "iresearch/search/detail/pattern_cache.hpp"

#include <utility>

#include "iresearch/utils/assert.hpp"

namespace irs {
namespace {

constexpr size_t kLengthBytes = sizeof(uint32_t);

std::shared_ptr<const RegexpAcceptor> Compile(bytes_view pattern,
                                              PatternKind kind) {
  if (kind == PatternKind::Wildcard) {
    return std::make_shared<const RegexpAcceptor>(RegexpAcceptor::WildcardTag{},
                                                  pattern);
  }
  if (kind == PatternKind::Union) {
    const auto parts = UnionParts(pattern);
    return std::make_shared<const RegexpAcceptor>(std::span{parts});
  }
  SDB_ASSERT(kind == PatternKind::RegexpPerl ||
             kind == PatternKind::RegexpPosixEre);
  return std::make_shared<const RegexpAcceptor>(
    pattern, kind == PatternKind::RegexpPerl ? RegexpSyntax::Perl
                                             : RegexpSyntax::PosixEre);
}

std::string KeyOf(bytes_view pattern, PatternKind kind) {
  std::string key;
  key.reserve(1 + pattern.size());
  key.push_back(static_cast<char>(kind));
  key.append(ViewCast<char>(pattern));
  return key;
}

}  // namespace

void AppendUnionPart(bstring& key, RegexpAcceptor::PartKind kind,
                     bytes_view pattern) {
  key.push_back(static_cast<byte_type>(kind));
  const auto size = static_cast<uint32_t>(pattern.size());
  for (size_t i = 0; i != kLengthBytes; ++i) {
    key.push_back(static_cast<byte_type>(size >> (8 * i)));
  }
  key += pattern;
}

bstring UnionKey(std::span<const RegexpAcceptor::Part> parts) {
  bstring key;
  for (const auto& part : parts) {
    AppendUnionPart(key, part.kind, part.pattern);
  }
  return key;
}

std::vector<RegexpAcceptor::Part> UnionParts(bytes_view key) {
  std::vector<RegexpAcceptor::Part> parts;
  while (key.size() > kLengthBytes) {
    const auto kind = static_cast<RegexpAcceptor::PartKind>(key[0]);
    uint32_t size = 0;
    for (size_t i = 0; i != kLengthBytes; ++i) {
      size |= uint32_t{key[1 + i]} << (8 * i);
    }
    key.remove_prefix(1 + kLengthBytes);
    SDB_ASSERT(size <= key.size());
    parts.push_back({kind, key.substr(0, size)});
    key.remove_prefix(size);
  }
  SDB_ASSERT(key.empty());
  return parts;
}

std::string DescribeUnion(bytes_view key) {
  std::string out;
  for (const auto& part : UnionParts(key)) {
    if (!out.empty()) {
      out.push_back('|');
    }
    const auto chars = ViewCast<char>(part.pattern);
    switch (part.kind) {
      case RegexpAcceptor::PartKind::Perl:
      case RegexpAcceptor::PartKind::PosixEre:
        out.append("(?:").append(chars).append(")");
        break;
      case RegexpAcceptor::PartKind::Prefix:
        out.append(chars).push_back('%');
        break;
      case RegexpAcceptor::PartKind::Term:
      case RegexpAcceptor::PartKind::Wildcard:
        out.append(chars);
        break;
    }
  }
  return out;
}

PatternCache& PatternCache::Instance() {
  static PatternCache cache;
  return cache;
}

std::shared_ptr<const RegexpAcceptor> PatternCache::Get(bytes_view pattern,
                                                        PatternKind kind) {
  if (_capacity.load(std::memory_order_relaxed) == 0) {
    return Compile(pattern, kind);
  }
  auto key = KeyOf(pattern, kind);
  {
    std::lock_guard lock{_mutex};
    if (const auto it = _index.find(key); it != _index.end()) {
      return TouchLocked(it->second);
    }
  }

  auto acceptor = Compile(pattern, kind);
  const size_t bytes = acceptor->MemoryUsage() + key.size();

  std::lock_guard lock{_mutex};
  if (const auto it = _index.find(key); it != _index.end()) {
    return TouchLocked(it->second);
  }
  if (_capacity.load(std::memory_order_relaxed) == 0) {
    return acceptor;
  }
  _entries.push_front(Entry{std::move(key), acceptor, bytes});
  _index.emplace(_entries.front().key, _entries.begin());
  _bytes += bytes;
  EvictLocked();
  return acceptor;
}

std::shared_ptr<const RegexpAcceptor> PatternCache::TouchLocked(
  Entries::iterator entry) {
  _entries.splice(_entries.begin(), _entries, entry);
  const size_t bytes = entry->acceptor->MemoryUsage() + entry->key.size();
  _bytes = _bytes - entry->bytes + bytes;
  entry->bytes = bytes;
  auto acceptor = entry->acceptor;
  EvictLocked();
  return acceptor;
}

void PatternCache::EvictLocked() {
  while (_bytes > _capacity.load(std::memory_order_relaxed) &&
         !_entries.empty()) {
    auto& victim = _entries.back();
    _index.erase(victim.key);
    _bytes -= victim.bytes;
    _entries.pop_back();
  }
}

void PatternCache::SetCapacity(size_t bytes) {
  std::lock_guard lock{_mutex};
  _capacity.store(bytes, std::memory_order_relaxed);
  EvictLocked();
}

size_t PatternCache::Capacity() const {
  return _capacity.load(std::memory_order_relaxed);
}

size_t PatternCache::Bytes() const {
  std::lock_guard lock{_mutex};
  return _bytes;
}

size_t PatternCache::Size() const {
  std::lock_guard lock{_mutex};
  return _entries.size();
}

void PatternCache::Clear() {
  std::lock_guard lock{_mutex};
  _index.clear();
  _entries.clear();
  _bytes = 0;
}

}  // namespace irs
