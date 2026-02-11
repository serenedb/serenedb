////////////////////////////////////////////////////////////////////////////////
/// @brief Library to build up VPack documents.
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Max Neunhoeffer
/// @author Jan Steemann
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <fstream>
#include <iostream>
#include <string>

#include "vpack/exception_macros.h"
#include "vpack/vpack.h"

using namespace vpack;

static void Usage(char* argv[]) {
#ifdef __linux__
  std::cout << "Usage: " << argv[0] << " [OPTIONS] INFILE [OUTFILE]"
            << std::endl;
#else
  std::cout << "Usage: " << argv[0] << " [OPTIONS] INFILE OUTFILE" << std::endl;
#endif
  std::cout << "This program reads the VPack INFILE into a string and saves its"
            << std::endl;
  std::cout << "JSON representation in file OUTFILE. Will work only for input"
            << std::endl;
  std::cout << "files up to 2 GB size." << std::endl;
#ifdef __linux__
  std::cout << "If no OUTFILE is specified, the generated JSON value be"
            << std::endl;
  std::cout << "printed to stdout." << std::endl;
#endif
  std::cout << "Available options are:" << std::endl;
  std::cout << " --pretty                  pretty-print JSON output"
            << std::endl;
  std::cout << " --no-pretty               don't pretty print JSON output"
            << std::endl;
  std::cout
    << " --print-unsupported       convert non-JSON types into something else"
    << std::endl;
  std::cout
    << " --no-print-unsupported    fail when encoutering a non-JSON type"
    << std::endl;
  std::cout << " --hex                     try to turn hex-encoded input into "
               "binary vpack"
            << std::endl;
  std::cout << " --validate                validate input VPack data"
            << std::endl;
  std::cout << " --no-validate             don't validate input VPack data"
            << std::endl;
}

static std::string ConvertFromHex(const std::string& value) {
  std::string result;
  result.reserve(value.size());

  const size_t n = value.size();
  int prev = -1;

  for (size_t i = 0; i < n; ++i) {
    int current;

    if (value[i] >= '0' && value[i] <= '9') {
      current = value[i] - '0';
    } else if (value[i] >= 'a' && value[i] <= 'f') {
      current = 10 + (value[i] - 'a');
    } else if (value[i] >= 'A' && value[i] <= 'F') {
      current = 10 + (value[i] - 'A');
    } else {
      prev = -1;
      continue;
    }

    if (prev == -1) {
      // first part of two-byte sequence
      prev = current;
    } else {
      // second part of two-byte sequence
      result.push_back(static_cast<unsigned char>((prev << 4) + current));
      prev = -1;
    }
  }

  return result;
}

static inline bool IsOption(const char* arg, const char* expected) {
  return (strcmp(arg, expected) == 0);
}

int main(int argc, char* argv[]) {
  VPACK_GLOBAL_EXCEPTION_TRY

  const char* infile_name = nullptr;
  const char* outfile_name = nullptr;
  bool allow_flags = true;
  bool pretty = true;
  bool print_unsupported = true;
  bool hex = false;
  bool validate = true;

  int i = 1;
  while (i < argc) {
    const char* p = argv[i];
    if (allow_flags && IsOption(p, "--help")) {
      Usage(argv);
      return EXIT_SUCCESS;
    } else if (allow_flags && IsOption(p, "--pretty")) {
      pretty = true;
    } else if (allow_flags && IsOption(p, "--no-pretty")) {
      pretty = false;
    } else if (allow_flags && IsOption(p, "--print-unsupported")) {
      print_unsupported = true;
    } else if (allow_flags && IsOption(p, "--no-print-unsupported")) {
      print_unsupported = false;
    } else if (allow_flags && IsOption(p, "--hex")) {
      hex = true;
    } else if (allow_flags && IsOption(p, "--validate")) {
      validate = true;
    } else if (allow_flags && IsOption(p, "--no-validate")) {
      validate = false;
    } else if (allow_flags && IsOption(p, "--")) {
      allow_flags = false;
    } else if (infile_name == nullptr) {
      infile_name = p;
    } else if (outfile_name == nullptr) {
      outfile_name = p;
    } else {
      Usage(argv);
      return EXIT_FAILURE;
    }
    ++i;
  }

#ifdef __linux__
  if (infile_name == nullptr) {
    infile_name = "-";
  }
#endif

  if (infile_name == nullptr) {
    Usage(argv);
    return EXIT_FAILURE;
  }

#ifdef __linux__
  // treat missing outfile as stdout
  bool to_std_out = false;
  if (outfile_name == nullptr || strcmp(outfile_name, "+") == 0) {
    outfile_name = "/proc/self/fd/1";
    to_std_out = true;
  }
#else
  bool const toStdOut = false;
  if (outfileName == nullptr) {
    usage(argv);
    return EXIT_FAILURE;
  }
#endif

  // treat "-" as stdin
  std::string infile = infile_name;
#ifdef __linux__
  if (infile == "-") {
    infile = "/proc/self/fd/0";
  }
#endif

  std::string s;
  std::ifstream ifs(infile, std::ifstream::in);

  if (!ifs.is_open()) {
    std::cerr << "Cannot read infile '" << infile << "'" << std::endl;
    return EXIT_FAILURE;
  }

  {
    char buffer[32768];
    s.reserve(sizeof(buffer));

    while (ifs.good()) {
      ifs.read(&buffer[0], sizeof(buffer));
      s.append(buffer, CheckOverflow(ifs.gcount()));
    }
  }
  ifs.close();

  if (hex) {
    s = ConvertFromHex(s);
  }

  Options options;
  options.pretty_print = pretty;
  options.check_attribute_uniqueness = true;
  options.validate_utf8_strings = true;
  options.unsupported_type_behavior =
    (print_unsupported ? Options::kConvertUnsupportedType
                       : Options::kFailOnUnsupportedType);

  if (validate) {
    Validator validator(&options);
    validator.validate(reinterpret_cast<const uint8_t*>(s.data()), s.size(),
                       false);
  }

  const Slice slice(reinterpret_cast<const uint8_t*>(s.data()));

  sdb::basics::StrSink sink;
  sink.Impl().reserve(4095);
  Dumper dumper(&sink, &options);

  try {
    dumper.Dump(slice);
  } catch (const Exception& ex) {
    std::cerr << "An exception occurred while processing infile '" << infile
              << "': " << ex.what() << std::endl;
    return EXIT_FAILURE;
  } catch (...) {
    std::cerr << "An unknown exception occurred while processing infile '"
              << infile << "'" << std::endl;
    return EXIT_FAILURE;
  }

  std::ofstream ofs(outfile_name, std::ofstream::out);

  if (!ofs.is_open()) {
    std::cerr << "Cannot write outfile '" << outfile_name << "'" << std::endl;
    return EXIT_FAILURE;
  }

  // reset stream
  if (!to_std_out) {
    ofs.seekp(0);
  }

  // write into stream
  const char* start = sink.Impl().data();
  ofs.write(start, sink.Impl().size());

  ofs.close();

  if (!to_std_out) {
    std::cout << "Successfully converted JSON infile '" << infile << "'"
              << std::endl;
    std::cout << "VPack Infile size: " << s.size() << std::endl;
    std::cout << "JSON Outfile size: " << sink.Impl().size() << std::endl;
  }

  VPACK_GLOBAL_EXCEPTION_CATCH
}
