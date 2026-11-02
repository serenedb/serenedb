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

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <benchmark/benchmark.h>
#include <re2/prog.h>
#include <re2/re2.h>
#include <re2/regexp.h>

#include <algorithm>
#include <cstdio>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr int64_t kMaxMem = int64_t{8} << 20;

std::string Filler(size_t size) {
  constexpr std::string_view kWords[] = {
    "lorem ",       "ipsum ",      "dolor ",     "sit ", "amet ",
    "consectetur ", "adipiscing ", "elit ",      "sed ", "do ",
    "eiusmod ",     "tempor ",     "path/to/x ",
  };
  std::string text;
  for (size_t i = 0; text.size() < size; i = (i * 7 + 3) % std::size(kWords)) {
    text += kWords[i];
  }
  text.resize(size);
  return text;
}

struct Case {
  std::string name;
  std::string pattern;
  std::string text;
  bool exhausted = false;
};

std::vector<Case> Cases() {
  const auto filler = Filler(4096);
  return {
    {"url_host", R"(^https?://(?:www\.)?([^/]+)/.*$)",
     "https://www.example.com/path/to/page?query=1&b=2"},
    {"key_value", R"((\w+)=(\w+))", "session user=alice&id=42&role=admin"},
    {"date", R"((\d{4})-(\d{2})-(\d{2}))",
     "event at 2024-05-17T10:20:30Z done"},
    {"email", R"(([a-z0-9._%+-]+)@([a-z0-9.-]+)\.([a-z]{2,}))",
     "contact: john.doe+tag@mail.example.org today"},
    {"alternation", R"((foo|foobar)(bar)?(baz))", "xxfoobarbaz"},
    {"nested_repeat", R"(((a+)(b+)?)+c)", "zzaabaabbbaac"},
    {"greedy_split", R"((.*)_(.*))", "alpha_beta_gamma_delta_epsilon"},
    {"lazy_tag", R"(<(.+?)>)", "<div class=\"x\"><span>hi</span></div>"},
    {"case_insensitive", R"((?i)(hello)\s+(world))", "Say HeLLo   WoRLD now"},
    {"unicode", R"((\p{L}+)\s+(\p{N}+))", "Привет мир 12345 тест"},
    {"access_log",
     R"(^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\w+) ([^ ]+) [^"]*" (\d{3}) (\d+))",
     R"(127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326)"},
    {"anchored_date", R"(^(\d{4})-(\d{2})-(\d{2}))", "2024-05-17T10:20:30Z"},
    {"ipv4_long", R"((\d+)\.(\d+)\.(\d+)\.(\d+))",
     absl::StrCat(filler, " from 192.168.10.254 end")},
    {"key_value_long", R"((\w+)=(\w+))", absl::StrCat(filler, " k=v")},
    {"greedy_split_long", R"((.*)_(.*))", absl::StrCat(filler, "_", filler)},
    {"url_host_nomatch", R"(^https?://(?:www\.)?([^/]+)/.*$)", filler},
    {"date_nomatch_long", R"((\d{4})-(\d{2})-(\d{2}))", filler},
    {"literal_long", "google", absl::StrCat(filler, " google ", filler)},
    {"literal_nomatch_long", "zebra", filler},
    {"prefix", R"(^https?://)", "https://www.example.com/path"},
    {"wildcard_long", R"(google.*\.com)",
     absl::StrCat(filler, " google search .com ", filler)},
    {"literal_alt_long", R"(yandex|google|bing)",
     absl::StrCat(filler, " bing")},
    {"class_run_long", R"([a-z]+ing)", absl::StrCat(filler, " running")},
    {"ci_literal_long", R"((?i)google)", absl::StrCat(filler, " GOOGLE")},
    {"word_boundary", R"(\b(\w+)ing\b)", "they were running fast"},
    {"line_start", R"((?m)^(\w+): (\d+)$)", "header\nkey: 42\nrest"},
    {"end_anchor", R"((\d+)$)", "order number 12345"},
    {"word_boundary_long", R"(\b(\w+)ing\b)", absl::StrCat(filler, " running")},
    {"budget_email", R"(([a-z0-9._%+-]+)@([a-z0-9.-]+)\.([a-z]{2,}))",
     "contact: john.doe+tag@mail.example.org today", true},
    {"budget_access_log",
     R"(^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\w+) ([^ ]+) [^"]*" (\d{3}) (\d+))",
     R"(127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326)",
     true},
    {"budget_anchored_date", R"(^(\d{4})-(\d{2})-(\d{2}))",
     "2024-05-17T10:20:30Z", true},
    {"budget_key_value_long", R"((\w+)=(\w+))", absl::StrCat(filler, " k=v"),
     true},
  };
}

enum class Question {
  kMatch,
  kSpan,
  kCapture,
};

constexpr std::string_view QuestionName(Question q) {
  switch (q) {
    case Question::kMatch:
      return "match";
    case Question::kSpan:
      return "span";
    case Question::kCapture:
      return "capture";
  }
  return "";
}

struct Result {
  bool matched = false;
  std::vector<std::string_view> groups;

  bool operator==(const Result&) const = default;
};

using Engine = std::function<bool(std::string_view, std::string_view*, int)>;

struct Compiled {
  std::unique_ptr<re2::RE2> re;
  re2::Regexp* regexp = nullptr;
  std::unique_ptr<re2::Prog> prog;
  std::unique_ptr<re2::Prog> rprog;

  ~Compiled() {
    if (regexp) {
      regexp->Decref();
    }
  }
};

std::vector<std::pair<std::string, Engine>> Engines(Compiled& c, Question q,
                                                    std::string_view text,
                                                    int n) {
  auto* prog = c.prog.get();
  auto* rprog = c.rprog.get();
  auto* re = c.re.get();
  std::vector<std::pair<std::string, Engine>> engines;
  engines.emplace_back(
    "re2", [re](std::string_view t, std::string_view* m, int n) {
      return re->Match(t, 0, t.size(), re2::RE2::UNANCHORED, m, n);
    });
  engines.emplace_back("nfa",
                       [prog](std::string_view t, std::string_view* m, int n) {
                         return prog->SearchNFA(t, t, re2::Prog::kUnanchored,
                                                re2::Prog::kFirstMatch, m, n);
                       });
  engines.emplace_back(
    "tdfa", [prog](std::string_view t, std::string_view* m, int n) {
      return prog->SearchTDFA(t, t, re2::Prog::kUnanchored,
                              re2::Prog::kFirstMatch, m, n) == 1;
    });
  if (q != Question::kMatch && rprog) {
    engines.emplace_back("dfa+tdfa", [prog, rprog](std::string_view t,
                                                   std::string_view* m, int n) {
      bool failed = false;
      std::string_view span;
      if (!prog->SearchDFA(t, t, re2::Prog::kUnanchored, re2::Prog::kFirstMatch,
                           &span, &failed, nullptr)) {
        return false;
      }
      if (!rprog->SearchDFA(span, t, re2::Prog::kAnchored,
                            re2::Prog::kLongestMatch, &span, &failed,
                            nullptr)) {
        return false;
      }
      return prog->SearchTDFA(span, t, re2::Prog::kAnchored,
                              re2::Prog::kFullMatch, m, n) == 1;
    });
  }
  if (q == Question::kMatch) {
    engines.emplace_back("dfa", [prog](std::string_view t, std::string_view*,
                                       int) {
      bool failed = false;
      return prog->SearchDFA(t, t, re2::Prog::kUnanchored,
                             re2::Prog::kFirstMatch, nullptr, &failed, nullptr);
    });
  }
  if (q == Question::kSpan && rprog) {
    engines.emplace_back(
      "dfa", [prog, rprog](std::string_view t, std::string_view* m, int) {
        bool failed = false;
        if (!prog->SearchDFA(t, t, re2::Prog::kUnanchored,
                             re2::Prog::kFirstMatch, m, &failed, nullptr)) {
          return false;
        }
        return rprog->SearchDFA(m[0], t, re2::Prog::kAnchored,
                                re2::Prog::kLongestMatch, m, &failed, nullptr);
      });
  }
  if (prog->anchor_start() && n <= re2::Prog::kMaxOnePassCapture &&
      prog->IsOnePass()) {
    engines.emplace_back(
      "onepass", [prog](std::string_view t, std::string_view* m, int n) {
        return prog->SearchOnePass(t, t, re2::Prog::kAnchored,
                                   re2::Prog::kFirstMatch, m, n);
      });
  }
  if (prog->CanBitState() && text.size() <= prog->bit_state_text_max_size()) {
    engines.emplace_back(
      "bitstate", [prog](std::string_view t, std::string_view* m, int n) {
        return prog->SearchBitState(t, t, re2::Prog::kUnanchored,
                                    re2::Prog::kFirstMatch, m, n);
      });
  }
  return engines;
}

int Submatches(const Compiled& c, Question q) {
  switch (q) {
    case Question::kMatch:
      return 0;
    case Question::kSpan:
      return 1;
    case Question::kCapture:
      return 1 + c.re->NumberOfCapturingGroups();
  }
  return 0;
}

Result Run(const Engine& engine, std::string_view text, int n) {
  Result r;
  r.groups.resize(n);
  r.matched = engine(text, r.groups.data(), n);
  if (!r.matched) {
    r.groups.clear();
  }
  return r;
}

void Measure(benchmark::State& state, const Engine& engine,
             std::string_view text, int n) {
  std::vector<std::string_view> groups(std::max(n, 1));
  for (auto _ : state) {
    bool matched = engine(text, groups.data(), n);
    benchmark::DoNotOptimize(matched);
    benchmark::DoNotOptimize(groups.data());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(text.size()));
}

int Main(int argc, char** argv) {
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  auto cases = std::make_shared<std::vector<Case>>(Cases());
  std::vector<std::shared_ptr<Compiled>> compiled;
  bool agree = true;
  for (auto& c : *cases) {
    auto comp = std::make_shared<Compiled>();
    comp->re = std::make_unique<re2::RE2>(c.pattern);
    re2::RegexpStatus status;
    comp->regexp =
      re2::Regexp::Parse(c.pattern, re2::Regexp::LikePerl, &status);
    if (!comp->re->ok() || !comp->regexp) {
      std::fprintf(stderr, "[%s] does not compile\n", c.name.c_str());
      return 1;
    }
    comp->prog.reset(comp->regexp->CompileToProg(kMaxMem * 2 / 3));
    comp->rprog.reset(comp->regexp->CompileToReverseProg(kMaxMem / 3));
    if (c.exhausted) {
      comp->prog->set_dfa_mem(0);
      comp->rprog->set_dfa_mem(0);
    }
    comp->prog->IsOnePass();
    comp->prog->ReserveTDFA();
    compiled.push_back(comp);
    for (auto q : {Question::kMatch, Question::kSpan, Question::kCapture}) {
      const int n = Submatches(*comp, q);
      if (q == Question::kCapture && n == 1) {
        continue;
      }
      auto engines = Engines(*comp, q, c.text, n);
      if (c.exhausted) {
        std::erase_if(engines, [](const auto& e) {
          return e.first == "re2" || e.first == "dfa" || e.first == "tdfa" ||
                 e.first == "dfa+tdfa";
        });
      }
      const auto& reference =
        *std::find_if(engines.begin(), engines.end(),
                      [](const auto& e) { return e.first == "nfa"; });
      const Result expected = Run(reference.second, c.text, n);
      for (auto& [name, engine] : engines) {
        const Result got = Run(engine, c.text, n);
        if (q != Question::kMatch && got != expected) {
          std::fprintf(stderr, "[mismatch] %s/%s/%s\n", c.name.c_str(),
                       std::string(QuestionName(q)).c_str(), name.c_str());
          agree = false;
        }
        if (q == Question::kMatch && got.matched != expected.matched) {
          std::fprintf(stderr, "[mismatch] %s/match/%s\n", c.name.c_str(),
                       name.c_str());
          agree = false;
        }
        benchmark::RegisterBenchmark(
          absl::StrCat(c.name, "/", QuestionName(q), "/", name),
          [comp, engine, text = c.text, n](benchmark::State& state) {
            Measure(state, engine, text, n);
          });
      }
    }
  }
  if (!agree) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}

}  // namespace

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
