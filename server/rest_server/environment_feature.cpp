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

#include "environment_feature.h"

#include <absl/strings/str_split.h>

#include "basics/file_utils.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "basics/physical_memory.h"
#include "basics/string_utils.h"

#ifdef __linux__
#include <sys/sysinfo.h>
#include <unistd.h>
#endif

#include <absl/strings/str_cat.h>

// assume that basic integral types can be modified atomically
// without ever requiring locks. if we encounter a target
// platform that does not satisfy these requirements, the code
// won't even compile, so we can find potential performance
// issues quickly.
static_assert(std::atomic<uint16_t>::is_always_lock_free);
static_assert(std::atomic<uint32_t>::is_always_lock_free);
static_assert(std::atomic<uint64_t>::is_always_lock_free);
static_assert(std::atomic<void*>::is_always_lock_free);

namespace {
#ifdef __linux__
std::string_view TrimProcName(std::string_view content) {
  size_t pos = content.find(' ');
  if (pos != std::string_view::npos && pos + 1 < content.size()) {
    size_t pos2 = std::string_view::npos;
    if (content[++pos] == '(') {
      ++pos;
      if (pos + 1 < content.size()) {
        pos2 = content.find(')', pos);
      }
    } else {
      pos2 = content.find(' ', pos);
    }
    if (pos2 != std::string_view::npos) {
      return content.substr(pos, pos2 - pos);
    }
  }
  return {};
}
#endif

uint64_t ActualMaxMappings() {
  uint64_t max_mappings = UINT64_MAX;

  // in case we cannot determine the number of max_map_count, we will
  // assume an effectively unlimited number of mappings
#ifdef __linux__
  // test max_map_count value in /proc/sys/vm
  try {
    std::string value =
      sdb::basics::file_utils::Slurp("/proc/sys/vm/max_map_count");

    max_mappings = sdb::basics::string_utils::Uint64(value);
  } catch (...) {
    // file not found or values not convertible into integers
  }
#endif

  return max_mappings;
}

uint64_t MinimumExpectedMaxMappings() {
#ifdef __linux__
  uint64_t expected = 65530;  // Linux kernel default

  uint64_t nproc = sdb::number_of_cores::GetValue();

  // we expect at most 8 times the number of cores as the effective number of
  // threads, and we want to allow at least 8000 mmaps per thread
  if (nproc * 8 * 8000 > expected) {
    expected = nproc * 8 * 8000;
  }

  return expected;
#else
  return 0;
#endif
}

}  // namespace

using namespace sdb::basics;

namespace sdb {

void PrintEnvironment() {
#ifdef __linux__
  std::string operating_system = "linux";
  try {
    const std::string version_filename("/proc/version");

    if (basics::file_utils::Exists(version_filename)) {
      operating_system =
        basics::string_utils::Trim(basics::file_utils::Slurp(version_filename));
    }
  } catch (...) {
    // ignore any errors as the log output is just informational
  }
#else
  operating_system = "unknown";
#endif

  // find parent process id and name
  std::string parent;
#ifdef __linux__
  try {
    pid_t parent_id = getppid();
    if (parent_id) {
      parent = absl::StrCat(", parent process: ", parent_id);
      const std::string proc_filename =
        absl::StrCat("/proc/", parent_id, "/stat");

      if (basics::file_utils::Exists(proc_filename)) {
        std::string content = basics::file_utils::Slurp(proc_filename);
        std::string_view proc_name = ::TrimProcName(content);
        if (!proc_name.empty()) {
          parent += absl::StrCat(" (", proc_name, ")");
        }
      }
    }
  } catch (...) {
  }
#endif

  SDB_INFO("xxxxx", Logger::FIXME,
           "detected operating system: ", operating_system, parent);

  if (sizeof(void*) == 4) {
    // 32 bit build
    SDB_ERROR("xxxxx", sdb::Logger::MEMORY,
              "this is a 32 bit build of SereneDB, which is unsupported. ",
              "it is recommended to run a 64 bit build instead because it can ",
              "address significantly bigger regions of memory");
  }

#ifdef __linux__
#if defined(__arm__) || defined(__arm64__) || defined(__aarch64__)
  // detect alignment settings for ARM
  {
    // To change the alignment trap behavior, simply echo a number into
    // /proc/cpu/alignment.  The number is made up from various bits:
    //
    // bit             behavior when set
    // ---             -----------------
    //
    // 0               A user process performing an unaligned memory access
    //                 will cause the kernel to print a message indicating
    //                 process name, pid, pc, instruction, address, and the
    //                 fault code.
    //
    // 1               The kernel will attempt to fix up the user process
    //                 performing the unaligned access.  This is of course
    //                 slow (think about the floating point emulator) and
    //                 not recommended for production use.
    //
    // 2               The kernel will send a SIGBUS signal to the user process
    //                 performing the unaligned access.
    bool alignmentDetected = false;

    const std::string filename("/proc/cpu/alignment");
    try {
      if (basics::file_utils::Exists(filename)) {
        const std::string cpuAlignment = basics::file_utils::Slurp(filename);
        auto start = cpuAlignment.find("User faults:");

        if (start != std::string::npos) {
          start += strlen("User faults:");
          size_t end = start;
          while (end < cpuAlignment.size()) {
            if (cpuAlignment[end] == ' ' || cpuAlignment[end] == '\t') {
              ++end;
            } else {
              break;
            }
          }
          while (end < cpuAlignment.size()) {
            if (cpuAlignment[end] < '0' || cpuAlignment[end] > '9') {
              ++end;
              break;
            }
            ++end;
          }

          int64_t alignment =
            std::stol(std::string(cpuAlignment.c_str() + start, end - start));
          if ((alignment & 2) == 0) {
            SDB_FATAL("xxxxx", sdb::Logger::MEMORY,
                      "possibly incompatible CPU alignment settings found in '",
                      filename,
                      "'. this may cause serened to abort with "
                      "SIGBUS. please set the value in '",
                      filename, "' to 2");
          }

          alignmentDetected = true;
        }
      } else {
        // if the file /proc/cpu/alignment does not exist, we should not
        // warn about it
        alignmentDetected = true;
      }

    } catch (...) {
      // ignore that we cannot detect the alignment
      SDB_TRACE(
        "xxxxx", sdb::Logger::MEMORY,
        "unable to detect CPU alignment settings. could not process file '",
        filename, "'");
    }

    if (!alignmentDetected) {
      SDB_WARN(
        "xxxxx", sdb::Logger::MEMORY,
        "unable to detect CPU alignment settings. could not process file '",
        filename,
        "'. this may cause serened to abort with SIGBUS. it may be "
        "necessary to set the value in '",
        filename, "' to 2");
    }

    const std::string cpuInfoFilename("/proc/cpuinfo");
    try {
      if (basics::file_utils::Exists(cpuInfoFilename)) {
        const std::string cpuInfo = basics::file_utils::Slurp(cpuInfoFilename);
        auto start = cpuInfo.find("ARMv6");

        if (start != std::string::npos) {
          SDB_FATAL("xxxxx", sdb::Logger::MEMORY,
                    "possibly incompatible ARMv6 CPU detected.");
        }
      }
    } catch (...) {
      // ignore that we cannot detect the alignment
      SDB_TRACE("xxxxx", sdb::Logger::MEMORY, "unable to detect CPU type '",
                filename, "'");
    }
  }
#endif
#endif

#ifdef __linux__
#ifdef SERENEDB_HAVE_JEMALLOC
  {
    const char* v = getenv("LD_PRELOAD");
    if (v != nullptr && (strstr(v, "/valgrind/") != nullptr ||
                         strstr(v, "/vgpreload") != nullptr)) {
      // smells like Valgrind
      SDB_WARN("xxxxx", sdb::Logger::MEMORY,
               "found LD_PRELOAD env variable value that looks like we are "
               "running under Valgrind. ",
               "this is unsupported in combination with jemalloc and may cause "
               "undefined behavior at least with memcheck!");
    }
  }
#endif
#endif

  {
    const char* v = getenv("MALLOC_CONF");

    if (v != nullptr) {
      // report value of MALLOC_CONF environment variable
      SDB_WARN("xxxxx", sdb::Logger::MEMORY,
               "found custom MALLOC_CONF environment value: ", v);
    }
  }

#ifdef __linux__
  // check overcommit_memory & overcommit_ratio
  try {
    const std::string memory_filename("/proc/sys/vm/overcommit_memory");

    std::string content = basics::file_utils::Slurp(memory_filename);
    uint64_t v = basics::string_utils::Uint64(content);

    if (v == 2) {
#ifdef SERENEDB_HAVE_JEMALLOC
      SDB_WARN("xxxxx", sdb::Logger::MEMORY, memory_filename,
               " is set to a value of 2. this "
               "setting has been found to be problematic");
      SDB_WARN("xxxxx", Logger::MEMORY, "execute 'sudo bash -c \"echo 0 > ",
               memory_filename, "\"'");
#endif

      // from https://www.kernel.org/doc/Documentation/sysctl/vm.txt:
      //
      //   When this flag is 0, the kernel attempts to estimate the amount
      //   of free memory left when userspace requests more memory.
      //   When this flag is 1, the kernel pretends there is always enough
      //   memory until it actually runs out.
      //   When this flag is 2, the kernel uses a "never overcommit"
      //   policy that attempts to prevent any overcommit of memory.
      const std::string ratio_filename("/proc/sys/vm/overcommit_ratio");

      if (basics::file_utils::Exists(ratio_filename)) {
        content = basics::file_utils::Slurp(ratio_filename);
        uint64_t r = basics::string_utils::Uint64(content);
        // from https://www.kernel.org/doc/Documentation/sysctl/vm.txt:
        //
        //  When overcommit_memory is set to 2, the committed address
        //  space is not permitted to exceed swap plus this percentage
        //  of physical RAM.

        struct sysinfo info;
        int res = sysinfo(&info);
        if (res == 0) {
          double swap_space = static_cast<double>(info.totalswap);
          double ram = static_cast<double>(physical_memory::GetValue());
          double rr =
            (ram >= swap_space) ? 100.0 * ((ram - swap_space) / ram) : 0.0;
          if (static_cast<double>(r) < 0.99 * rr) {
            SDB_WARN("xxxxx", Logger::MEMORY, ratio_filename, " is set to '", r,
                     "'. It is recommended to set it to at least '",
                     std::llround(rr),
                     "' (100 * (max(0, (RAM - Swap Space)) / RAM)) to utilize "
                     "all ",
                     "available RAM. Setting it to this value will minimize "
                     "swap ",
                     "usage, but may result in more out-of-memory errors, "
                     "while ",
                     "setting it to 100 will allow the system to use both all ",
                     "available RAM and swap space.");
            SDB_WARN("xxxxx", Logger::MEMORY, "execute 'sudo bash -c \"echo ",
                     std::llround(rr), " > ", ratio_filename, "\"'");
          }
        }
      }
    }
  } catch (...) {
    // file not found or value not convertible into integer
  }
#endif

  // Report memory and CPUs found:
  SDB_INFO(
    "xxxxx", Logger::MEMORY,
    "Available physical memory: ", physical_memory::GetValue(), " bytes",
    (physical_memory::Overridden() ? " (overriden by environment variable)"
                                   : ""),
    ", available cores: ", number_of_cores::GetValue(),
    (number_of_cores::Overridden() ? " (overriden by environment variable)"
                                   : ""));

#ifdef __linux__
  // test local ipv6 support
  try {
    const std::string ip_v6_filename("/proc/net/if_inet6");

    if (!basics::file_utils::Exists(ip_v6_filename)) {
      SDB_INFO("xxxxx", sdb::Logger::COMMUNICATION,
               "IPv6 support seems to be disabled");
    }
  } catch (...) {
    // file not found
  }

  // test local ipv4 port range
  try {
    const std::string port_filename("/proc/sys/net/ipv4/ip_local_port_range");

    if (basics::file_utils::Exists(port_filename)) {
      std::string content = basics::file_utils::Slurp(port_filename);
      std::vector<std::string_view> parts = absl::StrSplit(content, '\t');
      if (parts.size() == 2) {
        uint64_t lower = basics::string_utils::Uint64(parts[0]);
        uint64_t upper = basics::string_utils::Uint64(parts[1]);

        if (lower > upper || (upper - lower) < 16384) {
          SDB_WARN("xxxxx", sdb::Logger::COMMUNICATION,
                   "local port range for ipv4/ipv6 ports is ", lower, " - ",
                   upper,
                   ", which does not look right. it is recommended to make at "
                   "least 16K ports available");
          SDB_WARN("xxxxx", Logger::MEMORY,
                   "execute 'sudo bash -c \"echo -e \\\"32768\\t60999\\\" "
                   "> ",
                   port_filename,
                   "\"' or use an even "
                   "bigger port range");
        }
      }
    }
  } catch (...) {
    // file not found or values not convertible into integers
  }

  // test value tcp_tw_recycle
  // https://vincent.bernat.im/en/blog/2014-tcp-time-wait-state-linux
  // https://stackoverflow.com/questions/8893888/dropping-of-connections-with-tcp-tw-recycle
  try {
    const std::string recycle_filename("/proc/sys/net/ipv4/tcp_tw_recycle");

    if (basics::file_utils::Exists(recycle_filename)) {
      std::string content = basics::file_utils::Slurp(recycle_filename);
      uint64_t v = basics::string_utils::Uint64(content);
      if (v != 0) {
        SDB_WARN(
          "xxxxx", Logger::COMMUNICATION, recycle_filename, " is enabled(", v,
          ") ",
          "'. This can lead to all sorts of \"random\" network problems. ",
          "It is advised to leave it disabled (should be kernel default)");
        SDB_WARN("xxxxx", Logger::COMMUNICATION,
                 "execute 'sudo bash -c \"echo 0 > ", recycle_filename, "\"'");
      }
    }
  } catch (...) {
    // file not found or value not convertible into integer
  }

  // test max_map_count
  uint64_t actual = ActualMaxMappings();
  uint64_t expected = MinimumExpectedMaxMappings();

  if (actual < expected) {
    SDB_WARN("xxxxx", sdb::Logger::MEMORY,
             "maximum number of memory mappings per process is ", actual,
             ", which seems too low. it is recommended to set it to at least ",
             expected);
    SDB_WARN("xxxxx", Logger::MEMORY,
             "execute 'sudo sysctl -w \"vm.max_map_count=", expected, "\"'");
  }

  // test zone_reclaim_mode
  try {
    const std::string reclaim_filename("/proc/sys/vm/zone_reclaim_mode");

    if (basics::file_utils::Exists(reclaim_filename)) {
      std::string content = basics::file_utils::Slurp(reclaim_filename);
      uint64_t v = basics::string_utils::Uint64(content);
      if (v != 0) {
        // from https://www.kernel.org/doc/Documentation/sysctl/vm.txt:
        //
        //    This is value ORed together of
        //    1 = Zone reclaim on
        //    2 = Zone reclaim writes dirty pages out
        //    4 = Zone reclaim swaps pages
        //
        // https://www.poempelfox.de/blog/2010/03/19/
        SDB_WARN("xxxxx", Logger::MEMORY, reclaim_filename, " is set to '", v,
                 "'. It is recommended to set it to a value of 0");
        SDB_WARN("xxxxx", Logger::MEMORY, "execute 'sudo bash -c \"echo 0 > ",
                 reclaim_filename, "\"'");
      }
    }
  } catch (...) {
    // file not found or value not convertible into integer
  }

  std::array<std::string, 2> paths = {
    "/sys/kernel/mm/transparent_hugepage/enabled",
    "/sys/kernel/mm/transparent_hugepage/defrag"};

  for (const auto& file : paths) {
    try {
      if (basics::file_utils::Exists(file)) {
        std::string value = basics::file_utils::Slurp(file);
        size_t start = value.find('[');
        size_t end = value.find(']');

        if (start != std::string::npos && end != std::string::npos &&
            start < end && end - start >= 4) {
          value = value.substr(start + 1, end - start - 1);
          if (value == "always") {
            SDB_WARN("xxxxx", Logger::MEMORY, file, " is set to '", value,
                     "'. It is recommended to set it to a value of 'never' "
                     "or 'madvise'");
            SDB_WARN("xxxxx", Logger::MEMORY,
                     "execute 'sudo bash -c \"echo madvise > ", file, "\"'");
          }
        }
      }
    } catch (...) {
      // file not found
    }
  }

  bool numa = file_utils::Exists("/sys/devices/system/node/node1");

  if (numa) {
    try {
      const std::string maps_filename("/proc/self/numa_maps");

      if (basics::file_utils::Exists(maps_filename)) {
        std::string content = basics::file_utils::Slurp(maps_filename);
        std::vector<std::string_view> values = absl::StrSplit(content, '\n');

        if (!values.empty()) {
          auto first = values[0];
          auto where = first.find(' ');

          if (where != std::string::npos &&
              !first.substr(where).starts_with(" interleave")) {
            SDB_WARN("xxxxx", Logger::MEMORY,
                     "It is recommended to set NUMA to interleaved.");
            SDB_WARN("xxxxx", Logger::MEMORY,
                     "put 'numactl --interleave=all' in front of your command");
          }
        }
      }
    } catch (...) {
      // file not found
    }
  }

  // check kernel ASLR settings
  try {
    const std::string settings_filename("/proc/sys/kernel/randomize_va_space");

    if (basics::file_utils::Exists(settings_filename)) {
      std::string content = basics::file_utils::Slurp(settings_filename);
      uint64_t v = basics::string_utils::Uint64(content);
      // from man proc:
      //
      // 0 – No randomization. Everything is static.
      // 1 – Conservative randomization. Shared libraries, stack, mmap(), VDSO
      // and heap are randomized. 2 – Full randomization. In addition to
      // elements listed in the previous point, memory managed through brk() is
      // also randomized.
      std::string_view s;
      switch (v) {
        case 0:
          s = "nothing";
          break;
        case 1:
          s = "shared libraries, stack, mmap, VDSO and heap";
          break;
        case 2:
          s =
            "shared libraries, stack, mmap, VDSO, heap and memory managed "
            "through brk()";
          break;
      }
      if (!s.empty()) {
        SDB_DEBUG("xxxxx", Logger::FIXME, "host ASLR is in use for ", s);
      }
    }
  } catch (...) {
    // file not found or value not convertible into integer
  }

#endif
}

}  // namespace sdb
