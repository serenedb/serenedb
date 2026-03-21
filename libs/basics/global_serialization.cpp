////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023-2023 ArangoDB GmbH, Cologne, Germany
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

#include "basics/global_serialization.h"

#include <absl/strings/str_split.h>

#include <mutex>
#include <thread>

#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/string_utils.h"

namespace sdb {
namespace {

std::pair<std::string, std::string> FindSlpProgramPaths() {
  std::string path = "/tmp";
  SdbGETENV("SERENETEST_ROOT_DIR", path);
  std::string prog_path =
    basics::file_utils::BuildFilename(path.c_str(), "globalSLP");
  std::string pc_path =
    basics::file_utils::BuildFilename(path.c_str(), "globalSLP_PC");
  return std::pair(prog_path, pc_path);
}

std::vector<std::string> ReadSlpProgram(const std::string& prog_path) {
  // Read program:
  std::vector<std::string> prog_lines;
  try {
    std::string prog = sdb::basics::file_utils::Slurp(prog_path);
    if (prog.empty()) {
      return prog_lines;
    }
    prog_lines = absl::StrSplit(prog, '\n');
    prog_lines.pop_back();  // Ignore last line, since every line is
                            // ended by \n.
  } catch (const std::exception&) {
    // No program, return empty
  }
  return prog_lines;
}

constinit absl::Mutex gLobalSlpModificationMutex{absl::kConstInit};

}  // namespace

void WaitForGlobalEvent(std::string_view id, std::string_view selector) {
  auto [progPath, pcPath] = FindSlpProgramPaths();
  std::vector<std::string> prog_lines = ReadSlpProgram(progPath);
  if (prog_lines.empty()) {
    return;
  }
  SDB_INFO("xxxxx", Logger::MAINTENANCE, "Waiting for global event ", id,
           " with selector ", selector, "...");
  while (true) {
    // Read pc
    std::vector<std::string> executed_lines = ReadSlpProgram(pcPath);
    if (executed_lines.size() >= prog_lines.size()) {
      return;  // program already finished
    }
    std::string current = prog_lines[executed_lines.size()];
    std::vector<std::string_view> parts = absl::StrSplit(current, ' ');
    if (parts.size() >= 2 && id == parts[0] && selector == parts[1]) {
      // Hurray! We can make progress:
      if (parts.size() >= 3) {
        SDB_INFO("xxxxx", Logger::MAINTENANCE, "Global event ", id,
                 " with selector ", selector, " and comment ", parts[2],
                 " has happened...");
      } else {
        SDB_INFO("xxxxx", Logger::MAINTENANCE, "Global event ", id,
                 " with selector ", selector, " has happened...");
      }
      current.push_back('\n');
      try {
        std::lock_guard guard(gLobalSlpModificationMutex);
        sdb::basics::file_utils::AppendToFile(pcPath.c_str(), current);
      } catch (const std::exception&) {
        // ignore
      }
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
}

void ObserveGlobalEvent(std::string_view id, std::string_view selector) {
  auto [progPath, pcPath] = FindSlpProgramPaths();
  std::vector<std::string> prog_lines = ReadSlpProgram(progPath);
  if (prog_lines.empty()) {
    return;
  }
  std::string path = SdbGetTempPath();
  SDB_INFO("xxxxx", Logger::MAINTENANCE, "Observing global event ", id,
           " with selector ", selector, "...");
  // Read pc
  std::vector<std::string> executed_lines = ReadSlpProgram(pcPath);
  if (executed_lines.size() >= prog_lines.size()) {
    SDB_DEBUG("xxxxx", Logger::MAINTENANCE, "SLP has already finished");
    return;  // program already finished
  }
  std::string current = prog_lines[executed_lines.size()];
  std::vector<std::string_view> parts = absl::StrSplit(current, ' ');
  if (parts.size() >= 2 && id == parts[0] && selector == parts[1]) {
    // Hurray! We can make progress:
    if (parts.size() >= 3) {
      SDB_INFO("xxxxx", Logger::MAINTENANCE, "Global event ", id,
               " with selector ", selector, " and comment ", parts[2],
               " was observed...");
    } else {
      SDB_INFO("xxxxx", Logger::MAINTENANCE, "Global event ", id,
               " with selector ", selector, " was observed...");
    }
    current.push_back('\n');
    try {
      std::lock_guard guard(gLobalSlpModificationMutex);
      sdb::basics::file_utils::AppendToFile(pcPath.c_str(), current);
    } catch (const std::exception&) {
      // ignore
    }
  }
}

}  // namespace sdb
