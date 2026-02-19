////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "rocksdb_comparator.h"

#include <vpack/iterator.h>
#include <vpack/slice.h>

#include "basics/system-compiler.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
#include "rocksdb_engine_catalog/rocksdb_prefix_extractor.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "vpack/vpack_helper.h"

namespace sdb {
namespace {

constexpr auto kPrefix = sizeof(ObjectId);

int CompareImpl(const rocksdb::Slice& l, const rocksdb::Slice& r) {
  int res = memcmp(l.data(), r.data(), kPrefix);
  if (res != 0) {
    return res;
  }

  // TODO(mbkkt) why does this possible?
  bool l_short = l.size() == kPrefix;
  bool r_short = r.size() == kPrefix;
  if (l_short && r_short) {
    return 0;
  } else if (l_short) {
    return -1;
  } else if (r_short) {
    return 1;
  }

  vpack::Slice l_slice{reinterpret_cast<const uint8_t*>(l.data()) + kPrefix};
  vpack::Slice r_slice{reinterpret_cast<const uint8_t*>(r.data()) + kPrefix};
  res = basics::VPackHelper::compareArrays(l_slice, r_slice);
  if (res != 0) {
    return res;
  }

  const auto l_offset = kPrefix + l_slice.byteSize();
  const auto r_offset = kPrefix + r_slice.byteSize();
  std::string_view l_suffix{l.data() + l_offset, l.size() - l_offset};
  std::string_view r_suffix{r.data() + r_offset, r.size() - r_offset};
  return l_suffix.compare(r_suffix);
}

bool EqualImpl(const rocksdb::Slice& l, const rocksdb::Slice& r) {
  if (memcmp(l.data(), r.data(), kPrefix) != 0) {
    return false;
  }

  // TODO(mbkkt) why does this possible?
  bool l_short = l.size() == kPrefix;
  bool r_short = r.size() == kPrefix;
  if (l_short || r_short) {
    return l_short == r_short;
  }

  // TODO(mbkkt) maybe binary equal possible
  vpack::Slice l_slice{reinterpret_cast<const uint8_t*>(l.data()) + kPrefix};
  vpack::Slice r_slice{reinterpret_cast<const uint8_t*>(r.data()) + kPrefix};
  if (!basics::VPackHelper::equalsArrays(l_slice, r_slice, true)) {
    return false;
  }

  const auto l_offset = kPrefix + l_slice.byteSize();
  const auto r_offset = kPrefix + r_slice.byteSize();
  std::string_view l_suffix{l.data() + l_offset, l.size() - l_offset};
  std::string_view r_suffix{r.data() + r_offset, r.size() - r_offset};
  return l_suffix == r_suffix;
}

#ifdef SDB_DEV
int CompareChecked(const rocksdb::Slice& l, const rocksdb::Slice& r) {
  bool res = EqualImpl(l, r);
  SDB_ASSERT(res == EqualImpl(r, l));
  int res1 = CompareImpl(l, r);
  int res2 = CompareImpl(r, l);
  if (res) {
    SDB_ASSERT(res1 == 0);
    SDB_ASSERT(res2 == 0);
  } else {
    SDB_ASSERT(res1 != 0);
    SDB_ASSERT(res2 != 0);
    SDB_ASSERT(res1 * res2 < 0);
  }
  return res1;
}
#endif

}  // namespace

int RocksDBVPackComparator::compare(const rocksdb::Slice& l,
                                    const rocksdb::Slice& r) {
#ifdef SDB_DEV
  return CompareChecked(l, r);
#else
  return CompareImpl(l, r);
#endif
}

bool RocksDBVPackComparator::equal(const rocksdb::Slice& l,
                                   const rocksdb::Slice& r) {
#ifdef SDB_DEV
  return CompareChecked(l, r) == 0;
#else
  return EqualImpl(l, r);
#endif
}

}  // namespace sdb
