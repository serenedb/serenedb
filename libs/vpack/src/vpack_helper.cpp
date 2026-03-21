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

#include "vpack/vpack_helper.h"

#include "basics/error.h"
#include "basics/exceptions.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/number_utils.h"
#include "basics/operating-system.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "basics/system-compiler.h"
#include "basics/utf8_helper.h"

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <fcntl.h>
#include <string.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <string_view>

#include "basics/sink.h"
#include "vpack/collection.h"
#include "vpack/common.h"
#include "vpack/dumper.h"
#include "vpack/iterator.h"
#include "vpack/options.h"
#include "vpack/slice.h"

namespace sdb::basics {
namespace {

// TODO(mbkkt) try to avoid lookup table?
constexpr int8_t kTypeWeights[256] = {
  0 /* 0x00 */, 5 /* 0x01 */,  5 /* 0x02 */,  5 /* 0x03 */, 5 /* 0x04 */,
  5 /* 0x05 */, 5 /* 0x06 */,  5 /* 0x07 */,  5 /* 0x08 */, 5 /* 0x09 */,
  6 /* 0x0a */, 6 /* 0x0b */,  6 /* 0x0c */,  6 /* 0x0d */, 6 /* 0x0e */,
  6 /* 0x0f */, 6 /* 0x10 */,  6 /* 0x11 */,  6 /* 0x12 */, 5 /* 0x13 */,
  6 /* 0x14 */, -2 /* 0x15 */, -1 /* 0x16 */, 7 /* 0x17 */, 0 /* 0x18 */,
  1 /* 0x19 */, 2 /* 0x1a */,  0 /* 0x1b */,  0 /* 0x1c */, 0 /* 0x1d */,
  0 /* 0x1e */, 3 /* 0x1f */,  3 /* 0x20 */,  3 /* 0x21 */, 3 /* 0x22 */,
  3 /* 0x23 */, 3 /* 0x24 */,  3 /* 0x25 */,  3 /* 0x26 */, 3 /* 0x27 */,
  3 /* 0x28 */, 3 /* 0x29 */,  3 /* 0x2a */,  3 /* 0x2b */, 3 /* 0x2c */,
  3 /* 0x2d */, 3 /* 0x2e */,  3 /* 0x2f */,  3 /* 0x30 */, 3 /* 0x31 */,
  3 /* 0x32 */, 3 /* 0x33 */,  3 /* 0x34 */,  3 /* 0x35 */, 3 /* 0x36 */,
  3 /* 0x37 */, 3 /* 0x38 */,  3 /* 0x39 */,  3 /* 0x3a */, 3 /* 0x3b */,
  3 /* 0x3c */, 3 /* 0x3d */,  3 /* 0x3e */,  3 /* 0x3f */, 0 /* 0x40 */,
  0 /* 0x41 */, 0 /* 0x42 */,  0 /* 0x43 */,  0 /* 0x44 */, 0 /* 0x45 */,
  0 /* 0x46 */, 0 /* 0x47 */,  0 /* 0x48 */,  0 /* 0x49 */, 0 /* 0x4a */,
  0 /* 0x4b */, 0 /* 0x4c */,  0 /* 0x4d */,  0 /* 0x4e */, 0 /* 0x4f */,
  0 /* 0x50 */, 0 /* 0x51 */,  0 /* 0x52 */,  0 /* 0x53 */, 0 /* 0x54 */,
  0 /* 0x55 */, 0 /* 0x56 */,  0 /* 0x57 */,  0 /* 0x58 */, 0 /* 0x59 */,
  0 /* 0x5a */, 0 /* 0x5b */,  0 /* 0x5c */,  0 /* 0x5d */, 0 /* 0x5e */,
  0 /* 0x5f */, 0 /* 0x60 */,  0 /* 0x61 */,  0 /* 0x62 */, 0 /* 0x63 */,
  0 /* 0x64 */, 0 /* 0x65 */,  0 /* 0x66 */,  0 /* 0x67 */, 0 /* 0x68 */,
  0 /* 0x69 */, 0 /* 0x6a */,  0 /* 0x6b */,  0 /* 0x6c */, 0 /* 0x6d */,
  0 /* 0x6e */, 0 /* 0x6f */,  0 /* 0x70 */,  0 /* 0x71 */, 0 /* 0x72 */,
  0 /* 0x73 */, 0 /* 0x74 */,  0 /* 0x75 */,  0 /* 0x76 */, 0 /* 0x77 */,
  0 /* 0x78 */, 0 /* 0x79 */,  0 /* 0x7a */,  0 /* 0x7b */, 0 /* 0x7c */,
  0 /* 0x7d */, 0 /* 0x7e */,  0 /* 0x7f */,  4 /* 0x80 */, 4 /* 0x81 */,
  4 /* 0x82 */, 4 /* 0x83 */,  4 /* 0x84 */,  4 /* 0x85 */, 4 /* 0x86 */,
  4 /* 0x87 */, 4 /* 0x88 */,  4 /* 0x89 */,  4 /* 0x8a */, 4 /* 0x8b */,
  4 /* 0x8c */, 4 /* 0x8d */,  4 /* 0x8e */,  4 /* 0x8f */, 4 /* 0x90 */,
  4 /* 0x91 */, 4 /* 0x92 */,  4 /* 0x93 */,  4 /* 0x94 */, 4 /* 0x95 */,
  4 /* 0x96 */, 4 /* 0x97 */,  4 /* 0x98 */,  4 /* 0x99 */, 4 /* 0x9a */,
  4 /* 0x9b */, 4 /* 0x9c */,  4 /* 0x9d */,  4 /* 0x9e */, 4 /* 0x9f */,
  4 /* 0xa0 */, 4 /* 0xa1 */,  4 /* 0xa2 */,  4 /* 0xa3 */, 4 /* 0xa4 */,
  4 /* 0xa5 */, 4 /* 0xa6 */,  4 /* 0xa7 */,  4 /* 0xa8 */, 4 /* 0xa9 */,
  4 /* 0xaa */, 4 /* 0xab */,  4 /* 0xac */,  4 /* 0xad */, 4 /* 0xae */,
  4 /* 0xaf */, 4 /* 0xb0 */,  4 /* 0xb1 */,  4 /* 0xb2 */, 4 /* 0xb3 */,
  4 /* 0xb4 */, 4 /* 0xb5 */,  4 /* 0xb6 */,  4 /* 0xb7 */, 4 /* 0xb8 */,
  4 /* 0xb9 */, 4 /* 0xba */,  4 /* 0xbb */,  4 /* 0xbc */, 4 /* 0xbd */,
  4 /* 0xbe */, 4 /* 0xbf */,  4 /* 0xc0 */,  4 /* 0xc1 */, 4 /* 0xc2 */,
  4 /* 0xc3 */, 4 /* 0xc4 */,  4 /* 0xc5 */,  4 /* 0xc6 */, 4 /* 0xc7 */,
  4 /* 0xc8 */, 4 /* 0xc9 */,  4 /* 0xca */,  4 /* 0xcb */, 4 /* 0xcc */,
  4 /* 0xcd */, 4 /* 0xce */,  4 /* 0xcf */,  4 /* 0xd0 */, 4 /* 0xd1 */,
  4 /* 0xd2 */, 4 /* 0xd3 */,  4 /* 0xd4 */,  4 /* 0xd5 */, 4 /* 0xd6 */,
  4 /* 0xd7 */, 4 /* 0xd8 */,  4 /* 0xd9 */,  4 /* 0xda */, 4 /* 0xdb */,
  4 /* 0xdc */, 4 /* 0xdd */,  4 /* 0xde */,  4 /* 0xdf */, 4 /* 0xe0 */,
  4 /* 0xe1 */, 4 /* 0xe2 */,  4 /* 0xe3 */,  4 /* 0xe4 */, 4 /* 0xe5 */,
  4 /* 0xe6 */, 4 /* 0xe7 */,  4 /* 0xe8 */,  4 /* 0xe9 */, 4 /* 0xea */,
  4 /* 0xeb */, 4 /* 0xec */,  4 /* 0xed */,  4 /* 0xee */, 4 /* 0xef */,
  4 /* 0xf0 */, 4 /* 0xf1 */,  4 /* 0xf2 */,  4 /* 0xf3 */, 4 /* 0xf4 */,
  4 /* 0xf5 */, 4 /* 0xf6 */,  4 /* 0xf7 */,  4 /* 0xf8 */, 4 /* 0xf9 */,
  4 /* 0xfa */, 4 /* 0xfb */,  4 /* 0xfc */,  4 /* 0xfd */, 4 /* 0xfe */,
  4 /* 0xff */,
};

enum NumberType : uint8_t {
  kDouble = 0,
  kUInt = 1,
  kInt = 2,
};

union NumberValue {
  double d;
  uint64_t u;
  int64_t i;
};

NumberType ComputeType(vpack::Slice in, NumberValue& out) {
  if (in.isDouble()) {
    out.d = in.getDoubleUnchecked();
    return kDouble;
  }
  if (in.isUInt()) {
    out.u = in.getUIntUnchecked();
    return kUInt;
  }
  SDB_ASSERT(in.isInt() || in.isSmallInt());
  out.i = in.getIntUnchecked();
  return kInt;
};

template<typename T>
constexpr auto kMaxDouble = T{1} << 53;

}  // namespace

int VPackHelper::compareDouble(double l, double r) noexcept {
  if (l < r) {
    return -1;
  } else if (l > r) {
    return 1;
  } else if (l == r) {
    return 0;
  }
  if (std::isnan(l)) {
    return std::isnan(r) ? 0 : 1;
  }
  return -1;
}

int VPackHelper::compareIntUInt(int64_t l, uint64_t r) noexcept {
  if (l < 0) {
    return -1;
  }
  return compareSame(static_cast<uint64_t>(l), r);
}

int VPackHelper::compareIntDouble(int64_t l, double r) noexcept {
  if (std::isnan(r)) {
    return -1;
  }

  if (-kMaxDouble<int64_t> <= l && l <= kMaxDouble<int64_t>) {
    return compareSame(static_cast<double>(l), r);
  }

  if (r < number_utils::Min<int64_t>()) {
    return 1;
  }
  if (number_utils::Max<int64_t>() <= r) {
    return -1;
  }
  return compareSame(l, static_cast<int64_t>(r));
}

int VPackHelper::compareUIntDouble(uint64_t l, double r) noexcept {
  if (r < number_utils::Min<uint64_t>()) {
    return 1;
  }
  if (std::isnan(r)) {
    return -1;
  }

  if (l <= kMaxDouble<uint64_t>) {
    return compareSame(static_cast<double>(l), r);
  }

  if (number_utils::Max<uint64_t>() <= r) {
    return -1;
  }
  return compareSame(l, static_cast<uint64_t>(r));
}

/// static initializer for all VPack values
void VPackHelper::initialize() {
  SDB_TRACE("xxxxx", sdb::Logger::FIXME, "initializing vpack");

  // set the attribute translator in the global options
  vpack::Options::gDefaults.unsupported_type_behavior =
    vpack::Options::kConvertUnsupportedType;

  // false here, but will be set when converting to JSON for HTTP xfer
  vpack::Options::gDefaults.escape_unicode = false;

  // allow dumping of Object attributes in "arbitrary" order (i.e. non-sorted
  // order)
  vpack::Options::gDefaults.dump_attributes_in_index_order = false;

  // allow at most 200 levels of nested arrays/objects
  // must be 200 + 1 because in vpack the threshold value is exclusive,
  // not inclusive.
  vpack::Options::gDefaults.nesting_limit = 200 + 1;

  // set up options for validating requests, without UTF-8 validation
  gLooseRequestValidationOptions = vpack::Options::gDefaults;
  gLooseRequestValidationOptions.check_attribute_uniqueness = true;
  gLooseRequestValidationOptions.unsupported_type_behavior =
    vpack::Options::kFailOnUnsupportedType;

  // set up options for validating incoming end-user requests
  gStrictRequestValidationOptions = gLooseRequestValidationOptions;
  // UTF-8 validation controlled by server options
}

int VPackHelper::compareNumbers(vpack::Slice lhs, vpack::Slice rhs) {
  SDB_ASSERT(lhs.isNumber());
  SDB_ASSERT(rhs.isNumber());
  NumberValue l;
  NumberValue r;
  auto lhs_type = ComputeType(lhs, l);
  auto rhs_type = ComputeType(rhs, r);
  auto compute_case = [](auto l, auto r) { return l + r * 4; };
  switch (compute_case(lhs_type, rhs_type)) {
    case compute_case(kDouble, kDouble):
      return compareDouble(l.d, r.d);
    case compute_case(kUInt, kUInt):
      return compareSame(l.u, r.u);
    case compute_case(kInt, kInt):
      return compareSame(l.i, r.i);

    case compute_case(kDouble, kUInt):
      return -compareUIntDouble(r.u, l.d);
    case compute_case(kUInt, kDouble):
      return compareUIntDouble(l.u, r.d);

    case compute_case(kDouble, kInt):
      return -compareIntDouble(r.i, l.d);
    case compute_case(kInt, kDouble):
      return compareIntDouble(l.i, r.d);

    case compute_case(kUInt, kInt):
      return -compareIntUInt(r.i, l.u);
    case compute_case(kInt, kUInt):
      return compareIntUInt(l.i, r.u);

    default:
      SDB_UNREACHABLE();
  }
}

int VPackHelper::compareStrings(vpack::Slice lhs, vpack::Slice rhs) {
  SDB_ASSERT(lhs.isString());
  SDB_ASSERT(rhs.isString());
  auto l = lhs.stringViewUnchecked();
  auto r = rhs.stringViewUnchecked();
  return CompareUtf8(l.data(), l.size(), r.data(), r.size());
}

int VPackHelper::compareArrays(vpack::Slice lhs, vpack::Slice rhs) {
  SDB_ASSERT(lhs.isArray());
  SDB_ASSERT(rhs.isArray());
  vpack::ArrayIterator lhs_it{lhs};
  vpack::ArrayIterator rhs_it{rhs};
  const auto n = std::min(lhs_it.size(), rhs_it.size());
  for (size_t i = 0; i < n; ++i) {
    auto lhs_value = lhs_it.value();
    lhs_it.next();
    auto rhs_value = rhs_it.value();
    rhs_it.next();
    if (int r = compare(lhs_value, rhs_value); r != 0) {
      return r;
    }
  }
  if (lhs_it.valid()) {
    SDB_ASSERT(!lhs_it.value().isNone());
    return compare(lhs_it.value(), vpack::Slice::noneSlice());
  } else if (rhs_it.valid()) {
    SDB_ASSERT(!rhs_it.value().isNone());
    return compare(vpack::Slice::noneSlice(), rhs_it.value());
  }
  return 0;
}

int VPackHelper::compareObjects(vpack::Slice lhs, vpack::Slice rhs) {
  SDB_ASSERT(lhs.isObject());
  SDB_ASSERT(rhs.isObject());
  std::set<std::string_view> keys;
  // TODO(mbkkt) always sort objects => can compare without alloc
  vpack::Collection::unorderedKeys(lhs, keys);
  vpack::Collection::unorderedKeys(rhs, keys);
  for (const auto& key : keys) {
    auto lhs_value = lhs.get(key);
    auto rhs_value = rhs.get(key);
    if (int r = compare(lhs_value, rhs_value); r != 0) {
      return r;
    }
  }
  return 0;
}

std::string_view VPackHelper::checkAndGetString(vpack::Slice slice,
                                                std::string_view name) {
  SDB_ASSERT(slice.isObject());
  slice = slice.get(name);
  if (!slice.isString()) [[unlikely]] {
    SDB_THROW(ERROR_BAD_PARAMETER, "attribute '", name,
              slice.isNone() ? "' was not found" : "' is not a string");
  }
  return slice.stringViewUnchecked();
}

uint64_t VPackHelper::stringUInt64(vpack::Slice slice) {
  if (slice.isString()) {
    auto str = slice.stringViewUnchecked();
    return number_utils::AtoiZero<uint64_t>(str.data(),
                                            str.data() + str.size());
  } else if (slice.isNumber<uint64_t>()) {
    return slice.getNumber<uint64_t>();
  }
  return 0;
}

vpack::Builder VPackHelper::vpackFromFile(const char* path) {
  std::string content;
  if (!SdbSlurpFile(path, content)) {
    SDB_THROW(GetError());
  }
  vpack::Builder builder;
  vpack::Parser parser{builder};
  parser.parse(reinterpret_cast<const uint8_t*>(content.data()),
               content.size());
  return builder;
}

static bool PrintVPack(int fd, vpack::Slice slice, bool append_newline) {
  if (slice.isNone()) {
    return false;
  }

  StrSink sink;
  auto& buffer = sink.Impl();
  try {
    vpack::Dumper dumper(&sink);
    dumper.Dump(slice);
  } catch (...) {
    // Writing failed
    return false;
  }

  if (buffer.empty()) {
    // should not happen
    return false;
  }

  if (append_newline) {
    // add the newline here so we only need one write operation in the ideal
    // case
    buffer.push_back('\n');
  }

  const char* p = buffer.data();
  size_t n = buffer.size();

  while (0 < n) {
    auto m = SERENEDB_WRITE(fd, p, static_cast<size_t>(n));

    if (m <= 0) {
      return false;
    }

    n -= m;
    p += m;
  }

  return true;
}

bool VPackHelper::vpackToFile(const std::string& filename, vpack::Slice slice,
                              bool sync_file) {
  const std::string tmp = filename + ".tmp";

  // remove a potentially existing temporary file
  if (SdbExistsFile(tmp.c_str())) {
    // TODO(mbkkt) why ignore?
    std::ignore = SdbUnlinkFile(tmp.c_str());
  }

  int fd = SERENEDB_CREATE(
    tmp.c_str(), O_CREAT | O_TRUNC | O_EXCL | O_RDWR | SERENEDB_O_CLOEXEC,
    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

  if (fd < 0) {
    SetError(ERROR_SYS_ERROR);
    SDB_WARN("xxxxx", sdb::Logger::FIXME, "cannot create json file '", tmp,
             "': ", SERENEDB_ERRORNO_STR);
    return false;
  }

  if (!PrintVPack(fd, slice, true)) {
    SERENEDB_CLOSE(fd);
    SetError(ERROR_SYS_ERROR);
    SDB_WARN("xxxxx", sdb::Logger::FIXME, "cannot write to json file '", tmp,
             "': ", SERENEDB_ERRORNO_STR);
    // TODO(mbkkt) why ignore?
    std::ignore = SdbUnlinkFile(tmp.c_str());
    return false;
  }

  if (sync_file) {
    SDB_TRACE("xxxxx", sdb::Logger::FIXME, "syncing tmp file '", tmp, "'");

    if (!Sdbfsync(fd)) {
      SERENEDB_CLOSE(fd);
      SetError(ERROR_SYS_ERROR);
      SDB_WARN("xxxxx", sdb::Logger::FIXME, "cannot sync saved json '", tmp,
               "': ", SERENEDB_ERRORNO_STR);
      // TODO(mbkkt) why ignore?
      std::ignore = SdbUnlinkFile(tmp.c_str());
      return false;
    }
  }

  if (int res = SERENEDB_CLOSE(fd); res < 0) {
    SetError(ERROR_SYS_ERROR);
    SDB_WARN("xxxxx", sdb::Logger::FIXME, "cannot close saved file '", tmp,
             "': ", SERENEDB_ERRORNO_STR);
    // TODO(mbkkt) why ignore?
    std::ignore = SdbUnlinkFile(tmp.c_str());
    return false;
  }

  if (auto res = SdbRenameFile(tmp.c_str(), filename.c_str());
      res != ERROR_OK) {
    SetError(res);
    SDB_WARN("xxxxx", sdb::Logger::FIXME, "cannot rename saved file '", tmp,
             "' to '", filename, "': ", SERENEDB_ERRORNO_STR);
    // TODO(mbkkt) why ignore?
    std::ignore = SdbUnlinkFile(tmp.c_str());
    return false;
  }

  if (sync_file) {
    // also sync target directory
    const std::string dir{SdbDirname(filename)};
    fd = SERENEDB_OPEN(dir.c_str(), O_RDONLY | SERENEDB_O_CLOEXEC);
    if (fd < 0) {
      SetError(ERROR_SYS_ERROR);
      SDB_WARN("xxxxx", sdb::Logger::FIXME, "cannot sync directory '", filename,
               "': ", SERENEDB_ERRORNO_STR);
    } else {
      if (fsync(fd) < 0) {
        SetError(ERROR_SYS_ERROR);
        SDB_WARN("xxxxx", sdb::Logger::FIXME, "cannot sync directory '",
                 filename, "': ", SERENEDB_ERRORNO_STR);
      }
      int res = SERENEDB_CLOSE(fd);
      if (res < 0) {
        SetError(ERROR_SYS_ERROR);
        SDB_WARN("xxxxx", sdb::Logger::FIXME, "cannot close directory '",
                 filename, "': ", SERENEDB_ERRORNO_STR);
      }
    }
  }

  return true;
}

int VPackHelper::compare(vpack::Slice lhs, vpack::Slice rhs) {
  // TODO(mbkkt) maybe check lhs/rhs head equality to avoid loading large table?
  const auto l_weight = kTypeWeights[lhs.head()];
  if (const auto r_weight = kTypeWeights[rhs.head()]; l_weight != r_weight) {
    return l_weight < r_weight ? -1 : 1;
  }
  switch (l_weight) {
    case 3:
      return compareNumbers(lhs, rhs);
    case 4:
      return compareStrings(lhs, rhs);
    case 5:
      return compareArrays(lhs, rhs);
    case 6:
      return compareObjects(lhs, rhs);
    default:
      return 0;
  }
}

bool VPackHelper::equalsDouble(double l, double r) noexcept {
  return l == r || (std::isnan(l) && std::isnan(r));
}

bool VPackHelper::equalsIntUInt(int64_t l, uint64_t r) noexcept {
  return 0 <= l && static_cast<uint64_t>(l) == r;
}

bool VPackHelper::equalsIntDouble(int64_t l, double r) noexcept {
  if (-kMaxDouble<int64_t> <= l && l <= kMaxDouble<int64_t>) {
    return static_cast<double>(l) == r;
  }

  if (std::isnan(r) || r < number_utils::Min<int64_t>() ||
      number_utils::Max<int64_t>() <= r) {
    return false;
  }
  return l == static_cast<int64_t>(r);
}

bool VPackHelper::equalsUIntDouble(uint64_t l, double r) noexcept {
  if (r < number_utils::Min<uint64_t>()) {
    return false;
  }

  if (l <= kMaxDouble<uint64_t>) {
    return static_cast<double>(l) == r;
  }

  if (std::isnan(r) || number_utils::Max<uint64_t>() <= r) {
    return false;
  }
  return l == static_cast<uint64_t>(r);
}

bool VPackHelper::equalsNumbers(vpack::Slice lhs, vpack::Slice rhs) {
  SDB_ASSERT(lhs.isNumber());
  SDB_ASSERT(rhs.isNumber());
  NumberValue l;
  NumberValue r;
  auto lhs_type = ComputeType(lhs, l);
  auto rhs_type = ComputeType(rhs, r);
  auto compute_case = [](auto l, auto r) { return l + r * 4; };
  switch (compute_case(lhs_type, rhs_type)) {
    case compute_case(kDouble, kDouble):
      return equalsDouble(l.d, r.d);
    case compute_case(kUInt, kUInt):
      return l.u == r.u;
    case compute_case(kInt, kInt):
      return l.i == r.i;

    case compute_case(kDouble, kUInt):
      return equalsUIntDouble(r.u, l.d);
    case compute_case(kUInt, kDouble):
      return equalsUIntDouble(l.u, r.d);

    case compute_case(kDouble, kInt):
      return equalsIntDouble(r.i, l.d);
    case compute_case(kInt, kDouble):
      return equalsIntDouble(l.i, r.d);

    case compute_case(kUInt, kInt):
      return equalsIntUInt(r.i, l.u);
    case compute_case(kInt, kUInt):
      return equalsIntUInt(l.i, r.u);

    default:
      SDB_UNREACHABLE();
  }
}

bool VPackHelper::equalsStrings(vpack::Slice lhs, vpack::Slice rhs, bool utf8) {
  if (utf8) [[unlikely]] {
    SDB_ASSERT(lhs.isString());
    return rhs.isString() && compareStrings(lhs, rhs) == 0;
  }
  const auto lh = lhs.head();
  const auto rh = rhs.head();
  if (lh != rh) [[likely]] {
    return false;
  }
  SDB_ASSERT(lhs.isString());
  SDB_ASSERT(rhs.isString());

  if (lh == 0xff) [[unlikely]] {
    auto size = vpack::ReadIntegerFixed<uint32_t, 4>(lhs.begin() + 1);
    if (size != vpack::ReadIntegerFixed<uint32_t, 4>(rhs.begin() + 1)) {
      return false;
    }
    return memcmp(lhs.start() + 1 + 4, rhs.start() + 1 + 4, size) == 0;
  }

  auto size = static_cast<uint32_t>(lh - 0x80);
  return memcmp(lhs.start() + 1, rhs.start() + 1, size) == 0;
}

bool VPackHelper::equalsArrays(vpack::Slice lhs, vpack::Slice rhs, bool utf8) {
  SDB_ASSERT(lhs.isArray());
  SDB_ASSERT(rhs.isArray());
  vpack::ArrayIterator l_it{lhs};
  vpack::ArrayIterator r_it{rhs};
  const auto n = l_it.size();
  if (n != r_it.size()) {
    return false;
  }
  for (size_t i = 0; i < n; ++i) {
    auto lhs_value = l_it.value();
    l_it.next();
    auto rhs_value = r_it.value();
    r_it.next();
    if (!equals(lhs_value, rhs_value, utf8)) {
      return false;
    }
  }
  return true;
}

bool VPackHelper::equalsObjects(vpack::Slice lhs, vpack::Slice rhs, bool utf8) {
  SDB_ASSERT(lhs.isObject());
  SDB_ASSERT(rhs.isObject());
  vpack::ObjectIterator l_it{lhs, true};
  vpack::ObjectIterator r_it{rhs, true};
  if (l_it.size() != r_it.size()) {
    return false;
  }
  containers::FlatHashMap<std::string_view, vpack::Slice> keys;
  keys.reserve(l_it.size());
  for (; l_it.valid(); l_it.next()) {
    auto [k, v] = *l_it;
    SDB_ASSERT(k.isString());
    auto key = k.stringViewUnchecked();
    keys.emplace(key, v);
  }
  for (; r_it.valid(); r_it.next()) {
    auto e = *r_it;
    SDB_ASSERT(e.key.isString());
    auto key = e.key.stringViewUnchecked();
    auto it = keys.find(key);
    if (it == keys.end() || !equals(it->second, e.value(), utf8)) {
      return false;
    }
  }
  return true;
}

bool VPackHelper::equals(vpack::Slice lhs, vpack::Slice rhs, bool utf8) {
  if (lhs.isNumber()) {
    return rhs.isNumber() ? equalsNumbers(lhs, rhs) : false;
  } else if (lhs.isString()) {
    return equalsStrings(lhs, rhs, utf8);
  } else if (lhs.isArray()) {
    return rhs.isArray() ? equalsArrays(lhs, rhs, utf8) : false;
  } else if (lhs.isObject()) {
    return rhs.isObject() ? equalsObjects(lhs, rhs, utf8) : false;
  }
  return lhs.head() == rhs.head();
}

size_t VPackHelper::hash(vpack::Slice s) {
  return absl::HashOf(NormalizedSlice{s});
}

uint64_t VPackHelper::extractIdValue(vpack::Slice slice) {
  if (!slice.isObject()) [[unlikely]] {
    return 0;
  }
  const auto id = slice.get(StaticStrings::kDataSourceId);
  // TODO(mbkkt) invalid string or invalid number is not really checked
  if (id.isString()) {
    auto str = id.stringViewUnchecked();  // string "id", e.g. "9988488"
    return number_utils::AtoiZero<uint64_t>(str.data(),
                                            str.data() + str.size());
  } else if (id.isNumber()) {
    return id.getNumber<uint64_t>();  // numeric "id", e.g. 9988488
  } else if (id.isNone()) {
    return 0;
  }
  SDB_THROW(ERROR_BAD_PARAMETER, absl::StrCat("invalid type ", id.typeName(),
                                              " for 'id' attribute"));
}

}  // namespace sdb::basics
