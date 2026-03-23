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

#pragma once

#include <map>
#include <string>

#include "basics/build.h"
#include "basics/operating-system.h"

#ifdef _DEBUG
#define SERENEDB_VERSION_FULL SERENEDB_VERSION " [" SERENEDB_PLATFORM "-DEBUG]"
#else
#define SERENEDB_VERSION_FULL SERENEDB_VERSION " [" SERENEDB_PLATFORM "]"
#endif

namespace vpack {

class Builder;
}

namespace sdb {
namespace rest {

struct FullVersion {
  int major;
  int minor;
  int patch;
};

class Version {
 private:
  // create the version information
  Version() = delete;
  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

 public:
  // parse a version string into major, minor
  /// returns -1, -1 when the version string has an invalid format
  static std::pair<int, int> parseVersionString(const std::string&);

  // parse a full version string into major, minor, patch
  /// returns -1, -1, -1 when the version string has an invalid format
  /// returns major, -1, -1 when only the major version can be determined,
  /// returns major, minor, -1 when only the major and minor version can be
  /// determined.
  static FullVersion parseFullVersionString(const std::string&);

  // initialize
  static void initialize();

  // get numeric server version
  static int32_t getNumericServerVersion();

  // get server version
  static std::string getServerVersion();

  // get BOOST version
  static std::string getBoostVersion();

  // get boost reactor type
  static std::string getBoostReactorType();

  // get RocksDB version
  static std::string getRocksDBVersion();

  // get OpenSSL version
  static std::string getOpenSSLVersion(bool compile_time);

  // get ICU version
  static std::string getICUVersion();

  // get compiler
  static std::string getCompiler();

  // get endianness
  static std::string getEndianness();

  // get plaform
  static std::string getPlatform();

  // get build date
  static std::string getBuildDate();

  static std::string getBuildRepository();

  // get build-id, if set.
  // intentionally returned by reference, so we can avoid a copy
  static const std::string& getBuildId();

  // return a server version string
  static std::string getVerboseVersionString();

  // get detailed version information as a (multi-line) string
  static std::string getDetailed();

  // VPack all data
  static void getVPack(vpack::Builder&);

 public:
  static std::map<std::string, std::string> gValues;
};

}  // namespace rest
}  // namespace sdb
