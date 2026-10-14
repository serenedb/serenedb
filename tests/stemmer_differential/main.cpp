#include <zlib.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

extern "C" {
#include "include/libstemmer.h"
#include "runtime/api.h"
#include "libstemmer/modules_utf8.h"
}

namespace {

namespace fs = std::filesystem;

constexpr std::array<std::string_view, 37> kAlgorithms = {
    "arabic", "armenian", "basque", "catalan", "czech", "danish", "dutch", "dutch_porter",
    "earlymodernenglish", "english", "esperanto", "estonian", "finnish", "french", "german", "greek",
    "hindi", "hungarian", "indonesian", "irish", "italian", "lithuanian", "nepali", "norwegian",
    "persian", "polish", "porter", "portuguese", "romanian", "russian", "serbian", "sesotho", "spanish",
    "swedish", "tamil", "turkish", "yiddish",
};

struct Options {
  fs::path data;
  std::string algorithm{"all"};
  std::string candidate{"generated"};
  std::size_t max_words{0};
};

[[noreturn]] void fail(std::string message) { throw std::runtime_error(std::move(message)); }

std::size_t parse_size(std::string_view value) {
  std::size_t consumed = 0;
  const auto result = std::stoull(std::string{value}, &consumed);
  if (consumed != value.size()) {
    fail("invalid number: " + std::string{value});
  }
  return result;
}

void usage(std::ostream& out, const char* executable) {
  out << "Usage: " << executable << " --data PATH [options]\n\n"
      << "  --data PATH                 snowball-data checkout\n"
      << "  --algorithm all|NAME        language to check (default: all)\n"
      << "  --candidate generated|broken\n"
      << "                              candidate implementation (default: generated)\n"
      << "  --max-words N               stop after N words per language; 0 means all\n"
      << "  --help                      show this help\n";
}

Options parse_options(int argc, char* argv[]) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg{argv[i]};
    if (arg == "--help") {
      usage(std::cout, argv[0]);
      std::exit(0);
    }
    if (i + 1 >= argc) {
      fail("missing value after " + std::string{arg});
    }
    const std::string_view value{argv[++i]};
    if (arg == "--data") {
      options.data = value;
    } else if (arg == "--algorithm") {
      options.algorithm = value;
    } else if (arg == "--candidate") {
      options.candidate = value;
    } else if (arg == "--max-words") {
      options.max_words = parse_size(value);
    } else {
      fail("unknown option: " + std::string{arg});
    }
  }
  if (options.data.empty()) {
    fail("--data is required");
  }
  if (options.candidate != "generated" && options.candidate != "broken") {
    fail("--candidate must be generated or broken");
  }
  return options;
}

bool is_algorithm(std::string_view name) {
  return std::find(kAlgorithms.begin(), kAlgorithms.end(), name) != kAlgorithms.end();
}

class ReferenceBackend {
 public:
  explicit ReferenceBackend(std::string_view algorithm)
      : stemmer_{sb_stemmer_new(std::string{algorithm}.c_str(), "UTF_8"), &sb_stemmer_delete} {
    if (!stemmer_) {
      fail("reference backend does not support " + std::string{algorithm});
    }
  }

  std::string stem(std::string_view word) {
    const auto* result = sb_stemmer_stem(stemmer_.get(), reinterpret_cast<const sb_symbol*>(word.data()),
                                         static_cast<int>(word.size()));
    if (!result) {
      fail("reference backend failed");
    }
    return {reinterpret_cast<const char*>(result), static_cast<std::size_t>(sb_stemmer_length(stemmer_.get()))};
  }

 private:
  std::unique_ptr<sb_stemmer, decltype(&sb_stemmer_delete)> stemmer_;
};

class CandidateBackend {
 public:
  CandidateBackend(std::string_view algorithm, bool broken) : broken_{broken} {
    for (const stemmer_modules* module = modules; module->name; ++module) {
      if (module->enc == ENC_UTF_8 && algorithm == module->name) {
        module_ = module;
        env_.reset(module_->create());
        break;
      }
    }
    if (!module_ || !env_) {
      fail("generated candidate does not support " + std::string{algorithm});
    }
  }

  std::string stem(std::string_view word) {
    if (SN_set_current(env_.get(), static_cast<int>(word.size()), reinterpret_cast<const symbol*>(word.data())) < 0 ||
        module_->stem(env_.get()) < 0) {
      fail("generated candidate failed");
    }
    std::string result{reinterpret_cast<const char*>(env_->p), static_cast<std::size_t>(env_->l)};
    if (broken_) {
      result.push_back('!');
    }
    return result;
  }

 private:
  struct EnvDeleter {
    void operator()(SN_env* env) const { SN_delete_env(env); }
  };

  const stemmer_modules* module_{nullptr};
  std::unique_ptr<SN_env, EnvDeleter> env_;
  bool broken_;
};

class LineReader {
 public:
  virtual ~LineReader() = default;
  virtual bool next(std::string& line) = 0;
};

class PlainLineReader final : public LineReader {
 public:
  explicit PlainLineReader(const fs::path& path) : input_{path, std::ios::binary} {
    if (!input_) {
      fail("cannot open " + path.string());
    }
  }

  bool next(std::string& line) override { return static_cast<bool>(std::getline(input_, line)); }

 private:
  std::ifstream input_;
};

class GzipLineReader final : public LineReader {
 public:
  explicit GzipLineReader(const fs::path& path) : input_{gzopen(path.c_str(), "rb")} {
    if (!input_) {
      fail("cannot open " + path.string());
    }
  }

  ~GzipLineReader() override { gzclose(input_); }

  bool next(std::string& line) override {
    line.clear();
    std::array<char, 8192> buffer{};
    while (true) {
      const char* result = gzgets(input_, buffer.data(), static_cast<int>(buffer.size()));
      if (!result) {
        return !line.empty();
      }
      line += result;
      if (!line.empty() && line.back() == '\n') {
        line.pop_back();
        return true;
      }
    }
  }

 private:
  gzFile input_;
};

fs::path corpus_path(const fs::path& root, std::string_view algorithm) {
  for (const auto& filename : {"voc.txt", "voc.txt.gz"}) {
    auto path = root / algorithm / filename;
    if (fs::exists(path)) {
      return path;
    }
  }
  fail("no voc.txt or voc.txt.gz for " + std::string{algorithm});
}

std::unique_ptr<LineReader> open_corpus(const fs::path& path) {
  if (path.extension() == ".gz") {
    return std::make_unique<GzipLineReader>(path);
  }
  return std::make_unique<PlainLineReader>(path);
}

std::string escaped(std::string_view value) {
  std::ostringstream out;
  out << '"';
  for (const unsigned char byte : value) {
    if (byte == '\\' || byte == '"') {
      out << '\\' << static_cast<char>(byte);
    } else if (std::isprint(byte)) {
      out << static_cast<char>(byte);
    } else {
      out << "\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte) << std::dec;
    }
  }
  return out.str() + '"';
}

std::string hex(std::string_view value) {
  std::ostringstream out;
  for (std::size_t i = 0; i < value.size(); ++i) {
    if (i) {
      out << ' ';
    }
    out << std::hex << std::setw(2) << std::setfill('0')
        << static_cast<int>(static_cast<unsigned char>(value[i]));
  }
  return out.str();
}

bool check_algorithm(const Options& options, std::string_view algorithm, std::size_t& total) {
  ReferenceBackend reference{algorithm};
  CandidateBackend candidate{algorithm, options.candidate == "broken"};
  const auto path = corpus_path(options.data, algorithm);
  auto corpus = open_corpus(path);

  std::size_t line_number = 0;
  std::string word;
  while ((!options.max_words || line_number < options.max_words) && corpus->next(word)) {
    ++line_number;
    if (!word.empty() && word.back() == '\r') {
      word.pop_back();
    }
    const auto expected = reference.stem(word);
    const auto actual = candidate.stem(word);
    if (expected != actual) {
      std::cerr << "DIFFERENCE\n"
                << "  algorithm: " << algorithm << '\n'
                << "  corpus:    " << path << ':' << line_number << '\n'
                << "  input:     " << escaped(word) << "\n  input hex: " << hex(word) << '\n'
                << "  reference: " << escaped(expected) << "\n  ref hex:   " << hex(expected) << '\n'
                << "  candidate: " << escaped(actual) << "\n  cand hex:  " << hex(actual) << '\n';
      return false;
    }
  }
  total += line_number;
  std::cout << "[OK] " << algorithm << ": " << line_number << " words\n";
  return true;
}

}  // namespace

int main(int argc, char* argv[]) {
  try {
    const auto options = parse_options(argc, argv);
    if (options.algorithm != "all" && !is_algorithm(options.algorithm)) {
      fail("unknown algorithm: " + options.algorithm);
    }

    std::size_t total = 0;
    if (options.algorithm == "all") {
      for (const auto algorithm : kAlgorithms) {
        if (!check_algorithm(options, algorithm, total)) {
          return 1;
        }
      }
    } else if (!check_algorithm(options, options.algorithm, total)) {
      return 1;
    }
    std::cout << "Matched " << total << " words.\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << '\n';
    return 2;
  }
}
