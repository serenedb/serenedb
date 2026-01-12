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

#include "rest/version.h"

#include <absl/strings/internal/ostringstream.h>
#include <openssl/ssl.h>
#include <rocksdb/convenience.h>
#include <rocksdb/version.h>
#include <vpack/builder.h>

#include <cstdint>
#include <sstream>
#include <string_view>

#include "basics/asio_ns.h"
#include "basics/build-date.h"
#include "basics/build-repository.h"
#include "basics/debugging.h"
#include "basics/number_utils.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "basics/utf8_helper.h"
#include "build_id/build_id.h"

using namespace sdb;
using namespace sdb::rest;

std::map<std::string, std::string> Version::gValues;

// parse a version string into major, minor
/// returns -1, -1 when the version string has an invalid format
/// returns major, -1 when only the major version can be determined
std::pair<int, int> Version::parseVersionString(const std::string& str) {
  std::pair<int, int> result{-1, -1};

  if (!str.empty()) {
    const char* p = str.c_str();
    const char* q = p;
    while (*q >= '0' && *q <= '9') {
      ++q;
    }
    if (p != q) {
      result.first = std::stoi(std::string(p, q - p));
      result.second = 0;

      if (*q == '.') {
        ++q;
      }
      p = q;
      while (*q >= '0' && *q <= '9') {
        ++q;
      }
      if (p != q) {
        result.second = std::stoi(std::string(p, q - p));
      }
    }
  }

  return result;
}

/// parse a full version string into major, minor, patch
/// returns -1, -1, -1 when the version string has an invalid format
/// returns major, -1, -1 when only the major version can be determined,
/// returns major, minor, -1 when only the major and minor version can be
/// determined.
FullVersion Version::parseFullVersionString(const std::string& str) {
  FullVersion result{-1, -1, -1};
  int tmp;

  if (!str.empty()) {
    const char* p = str.c_str();
    const char* q = p;
    tmp = 0;
    while (*q >= '0' && *q <= '9') {
      tmp = tmp * 10 + *q++ - '0';
    }
    if (p != q) {
      result.major = tmp;
      tmp = 0;

      if (*q == '.') {
        ++q;
      }
      p = q;
      while (*q >= '0' && *q <= '9') {
        tmp = tmp * 10 + *q++ - '0';
      }
      if (p != q) {
        result.minor = tmp;
        tmp = 0;
        if (*q == '.') {
          ++q;
        }
        p = q;
        while (*q >= '0' && *q <= '9') {
          tmp = tmp * 10 + *q++ - '0';
        }
        if (p != q) {
          result.patch = tmp;
        }
      }
    }
  }

  return result;
}

// initialize
void Version::initialize() {
  if (!gValues.empty()) {
    return;
  }

  gValues["architecture"] =
    (sizeof(void*) == 4 ? "32" : "64") + std::string("bit");
#if defined(__arm__) || defined(__arm64__) || defined(__aarch64__)
  gValues["arm"] = "true";
#else
  gValues["arm"] = "false";
#endif
  gValues["boost-version"] = getBoostVersion();
  gValues["build-date"] = getBuildDate();
  gValues["compiler"] = getCompiler();
#ifdef _DEBUG
  gValues["debug"] = "true";
#else
  gValues["debug"] = "false";
#endif
#ifdef SERENEDB_USE_IPO
  gValues["ipo"] = "true";
#else
  gValues["ipo"] = "false";
#endif
#ifdef NDEBUG
  gValues["ndebug"] = "true";
#else
  gValues["ndebug"] = "false";
#endif
#ifdef USE_COVERAGE
  gValues["coverage"] = "true";
#else
  gValues["coverage"] = "false";
#endif
#ifdef ARCHITECTURE_OPTIMIZATIONS
  gValues["optimization-flags"] = std::string(ARCHITECTURE_OPTIMIZATIONS);
#endif
  gValues["endianness"] = getEndianness();
  gValues["fd-setsize"] = basics::string_utils::Itoa(FD_SETSIZE);
  gValues["full-version-string"] = getVerboseVersionString();
  gValues["icu-version"] = getICUVersion();
  gValues["openssl-version-compile-time"] = getOpenSSLVersion(true);
  gValues["openssl-version-run-time"] = getOpenSSLVersion(false);
#ifdef __pic__
  gValues["pic"] = std::to_string(__pic__);
#else
  gValues["pic"] = "none";
#endif
#ifdef __pie__
  gValues["pie"] = std::to_string(__pie__);
#else
  gValues["pie"] = "none";
#endif
  gValues["platform"] = getPlatform();
  gValues["reactor-type"] = getBoostReactorType();
  gValues["server-version"] = getServerVersion();
  gValues["sizeof int"] = basics::string_utils::Itoa(sizeof(int));
  gValues["sizeof long"] = basics::string_utils::Itoa(sizeof(long));
  gValues["sizeof void*"] = basics::string_utils::Itoa(sizeof(void*));
  // always hard-coded to "false" since 3.12
  gValues["unaligned-access"] = "false";

#if HAVE_SERENEDB_BUILD_REPOSITORY
  gValues["build-repository"] = getBuildRepository();
#endif

  gValues["curl-version"] = "none";

#ifdef SDB_DEV
  gValues["assertions"] = "true";
#else
  gValues["assertions"] = "false";
#endif

  gValues["rocksdb-version"] = getRocksDBVersion();

#ifdef __cplusplus
  gValues["cplusplus"] = std::to_string(__cplusplus);
#else
  gValues["cplusplus"] = "unknown";
#endif

  gValues["asan"] = "false";
#if defined(__SANITIZE_ADDRESS__)
  gValues["asan"] = "true";
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
  gValues["asan"] = "true";
#endif
#endif

  gValues["tsan"] = "false";
#if defined(__SANITIZE_THREAD__)
  gValues["tsan"] = "true";
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
  gValues["tsan"] = "true";
#endif
#endif

  gValues["msan"] = "false";
#if defined(__SANITIZE_MEMORY__)
  gValues["msan"] = "true";
#elif defined(__has_feature)
#if __has_feature(memory_sanitizer)
  gValues["msan"] = "true";
#endif
#endif

#if defined(__SSE4_2__) && !defined(NO_SSE42)
  gValues["sse42"] = "true";
#else
  gValues["sse42"] = "false";
#endif

#ifdef __AVX__
  gValues["avx"] = "true";
#else
  gValues["avx"] = "false";
#endif

#ifdef __AVX2__
  gValues["avx2"] = "true";
#else
  gValues["avx2"] = "false";
#endif

#ifdef SDB_DEV
  gValues["maintainer-mode"] = "true";
#else
  gValues["maintainer-mode"] = "false";
#endif

#ifdef SDB_FAULT_INJECTION
  gValues["failure-tests"] = "true";
#else
  gValues["failure-tests"] = "false";
#endif

#ifdef SERENEDB_HAVE_JEMALLOC
  gValues["jemalloc"] = "true";
#else
  gValues["jemalloc"] = "false";
#endif

#ifdef SERENEDB_HAVE_TCMALLOC
  gValues["tcmalloc"] = "true";
#else
  gValues["tcmalloc"] = "false";
#endif

#ifdef SERENEDB_HAVE_POLL_H
  gValues["fd-client-event-handler"] = "poll";
#else
  gValues["fd-client-event-handler"] = "select";
#endif

  gValues["build-id"] = basics::string_utils::EncodeHex(build_id::GetBuildId());

  for (auto& it : gValues) {
    basics::string_utils::TrimInPlace(it.second);
  }
}

// get numeric server version
int32_t Version::getNumericServerVersion() {
  const char* api_version = SERENEDB_VERSION;
  const char* p = api_version;

  // read major version
  while (*p >= '0' && *p <= '9') {
    ++p;
  }

  SDB_ASSERT(*p == '.');
  int32_t major = number_utils::AtoiPositiveUnchecked<int32_t>(api_version, p);

  api_version = ++p;

  // read minor version
  while (*p >= '0' && *p <= '9') {
    ++p;
  }

  SDB_ASSERT((*p == '.' || *p == '-' || *p == '\0') && p != api_version);
  int32_t minor = number_utils::AtoiPositiveUnchecked<int32_t>(api_version, p);

  int32_t patch = 0;
  if (*p == '.') {
    api_version = ++p;

    // read minor version
    while (*p >= '0' && *p <= '9') {
      ++p;
    }

    if (p != api_version) {
      patch = number_utils::AtoiPositiveUnchecked<int32_t>(api_version, p);
    }
  }

  return (int32_t)(patch + minor * 100L + major * 10000L);
}

// get server version
std::string Version::getServerVersion() {
  return std::string(SERENEDB_VERSION);
}

// get BOOST version
std::string Version::getBoostVersion() {
#ifdef SERENEDB_BOOST_VERSION
  return std::string(SERENEDB_BOOST_VERSION);
#else
  return std::string();
#endif
}

// get boost reactor type
std::string Version::getBoostReactorType() {
#if defined(BOOST_ASIO_HAS_IOCP)
  return std::string("iocp");
#elif defined(BOOST_ASIO_HAS_EPOLL)
  return std::string("epoll");
#elif defined(BOOST_ASIO_HAS_KQUEUE)
  return std::string("kqueue");
#elif defined(BOOST_ASIO_HAS_DEV_POLL)
  return std::string("/dev/poll");
#else
  return std::string("select");
#endif
}

// get RocksDB version
std::string Version::getRocksDBVersion() {
  return std::to_string(ROCKSDB_MAJOR) + "." + std::to_string(ROCKSDB_MINOR) +
         "." + std::to_string(ROCKSDB_PATCH);
}

// get OpenSSL version
std::string Version::getOpenSSLVersion(bool compile_time) {
  if (compile_time) {
#ifdef OPENSSL_VERSION_TEXT
    return std::string(OPENSSL_VERSION_TEXT);
#elif defined(SERENEDB_OPENSSL_VERSION)
    return std::string(SERENEDB_OPENSSL_VERSION);
#else
    return std::string("openssl (unknown version)");
#endif
  } else {
    const char* v = SSLeay_version(SSLEAY_VERSION);

    if (v == nullptr) {
      return std::string("openssl (unknown version)");
    }

    return std::string(v);
  }
}

// get ICU version
std::string Version::getICUVersion() {
  UVersionInfo icu_version;
  char icu_version_string[U_MAX_VERSION_STRING_LENGTH];
  u_getVersion(icu_version);
  u_versionToString(icu_version, icu_version_string);

  return icu_version_string;
}

// get compiler
std::string Version::getCompiler() {
#if defined(__clang__)
  return "clang [" + std::string(__VERSION__) + "]";
#elif defined(__GNUC__) || defined(__GNUG__)
  return "gcc [" + std::string(__VERSION__) + "]";
  return "unknown";
#endif
}

// get endianness
std::string Version::getEndianness() {
  uint64_t value = 0x12345678abcdef99;
  static_assert(sizeof(value) == 8, "unexpected uint64_t size");

  const unsigned char* p = reinterpret_cast<const unsigned char*>(&value);

  if (p[0] == 0x12 && p[1] == 0x34 && p[2] == 0x56 && p[3] == 0x78 &&
      p[4] == 0xab && p[5] == 0xcd && p[6] == 0xef && p[7] == 0x99) {
    return "big";
  }

  if (p[0] == 0x99 && p[1] == 0xef && p[2] == 0xcd && p[3] == 0xab &&
      p[4] == 0x78 && p[5] == 0x56 && p[6] == 0x34 && p[7] == 0x12) {
    return "little";
  }
  return "unknown";
}

std::string Version::getPlatform() { return SERENEDB_PLATFORM; }

std::string Version::getBuildDate() {
// the OpenSuSE build system does not like it, if __DATE__ is used
#ifdef SERENEDB_BUILD_DATE
  return std::string(SERENEDB_BUILD_DATE);
#else
  return std::string(__DATE__).append(" ").append(__TIME__);
#endif
}

std::string Version::getBuildRepository() {
#ifdef HAVE_SERENEDB_BUILD_REPOSITORY
  return std::string(SERENEDB_BUILD_REPOSITORY);
#else
  return std::string();
#endif
}

const std::string& Version::getBuildId() {
  if (auto it = gValues.find("build-id"); it != gValues.end()) {
    return it->second;
  }
  return StaticStrings::kEmpty;
}

std::string Version::getVerboseVersionString() {
  std::string version_str;
  absl::strings_internal::OStringStream version{&version_str};

  version << "SereneDB " << SERENEDB_VERSION_FULL << " "
          << (sizeof(void*) == 4 ? "32" : "64") << "bit";
#ifdef SDB_DEV
  version << " maintainer mode";
#endif

#if defined(__SANITIZE_ADDRESS__)
  version << " with ASAN";
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
  version << " with ASAN";
#endif
#endif

  version << ", using ";
#ifdef SERENEDB_HAVE_JEMALLOC
  version << "jemalloc, ";
#endif

#ifdef HAVE_SERENEDB_BUILD_REPOSITORY
  version << "build " << getBuildRepository() << ", ";
#endif
  version << "RocksDB " << getRocksDBVersion() << ", ICU " << getICUVersion()
          << ", " << getOpenSSLVersion(false);

  if (gValues.contains("build-id")) {
    version << ", build-id: " << gValues["build-id"];
  }

  return version_str;
}

// get detailed version information as a (multi-line) string
std::string Version::getDetailed() {
  std::string result;

  for (const auto& it : gValues) {
    const std::string& value = it.second;

    if (!value.empty()) {
      result.append(it.first);
      result.append(": ");
      result.append(it.second);
      result += "\n";
    }
  }

  return result;
}

// VPack all data
void Version::getVPack(vpack::Builder& dst) {
  SDB_ASSERT(!dst.isClosed());

  for (const auto& it : gValues) {
    const std::string& value = it.second;

    if (!value.empty()) {
      dst.add(it.first, value);
    }
  }
}
