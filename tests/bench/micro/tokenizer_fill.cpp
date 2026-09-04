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

#include <benchmark/benchmark.h>

#include <array>
#include <cstring>
#include <duckdb.hpp>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/collation_tokenizer.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/geo_analyzer.hpp"
#include "iresearch/analysis/icu_text_tokenizer.hpp"
#include "iresearch/analysis/keyword_tokenizer.hpp"
#include "iresearch/analysis/multi_delimited_tokenizer.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/path_hierarchy_tokenizer.hpp"
#include "iresearch/analysis/pattern_tokenizer.hpp"
#include "iresearch/analysis/pipeline_tokenizer.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/analysis/shingle_tokenizer.hpp"
#include "iresearch/analysis/solr_synonyms_tokenizer.hpp"
#include "iresearch/analysis/sparse_ngram_tokenizer.hpp"
#include "iresearch/analysis/split_by_non_alpha_tokenizer.hpp"
#include "iresearch/analysis/sql_tokenizer.hpp"
#include "iresearch/analysis/stemming_tokenizer.hpp"
#include "iresearch/analysis/stopwords_tokenizer.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/union_tokenizer.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "iresearch/analysis/wordnet_synonyms_tokenizer.hpp"
#include "iresearch/index/inverter/columnar_flush.hpp"
#include "iresearch/index/inverter/fields_inverter.hpp"
#include "test_resources.hpp"

namespace {

using namespace irs;
using namespace irs::analysis;

constexpr size_t kValues = 4096;

const std::vector<std::string>& WordCorpus() {
  static const auto corpus = [] {
    const char* roots[] = {"Running",  "Consideration", "Fox",
                           "Jumping",  "Database",      "Tokenizer",
                           "Grateful", "Performance"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      out.push_back(std::string{roots[i % 8]} + std::to_string(i % 97));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& TextCorpus();

// TextCorpus with a non-ascii sentinel word appended to every value: the
// same maker takes the unicode path by input selection.
const std::vector<std::string>& TextUnicodeCorpus() {
  static const auto corpus = [] {
    auto out = TextCorpus();
    for (auto& v : out) {
      v += " \xCF\x89\xCF\x89\xCF\x89";
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& CjkCorpus() {
  static const auto corpus = [] {
    const char* phrases[] = {"中文测试文本分词性能",
                             "日本語のテキストを分割する",
                             "ภาษาไทยทดสอบการแบ่งคำ",
                             "по вечерам ежик ходил к медвежонку",
                             "mixed 日本語 and English 文本 tokens",
                             "한국어 텍스트 분석 성능 측정",
                             "Ελληνικό κείμενο για δοκιμή"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t w = 0; w < 6; ++w) {
        v += phrases[(i * 5 + w * 3) % 7];
        v += ' ';
      }
      v.pop_back();
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// Mostly-ASCII text with an occasional non-ASCII value: the shape where a
// block-level ascii fact is pessimistic for every ASCII value in the block.
const std::vector<std::string>& TextMixedCorpus() {
  static const auto corpus = [] {
    auto out = TextCorpus();
    for (size_t i = 9; i < out.size(); i += 10) {
      out[i] += " \xCF\x89\xCF\x89\xCF\x89";
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& TextCorpus() {
  static const auto corpus = [] {
    const char* vocab[] = {"the",     "quick", "brown",   "fox",   "jumps",
                           "over",    "lazy",  "dog",     "while", "seven",
                           "wizards", "brew",  "potions", "under", "moonlight",
                           "and",     "watch", "distant", "ships", "sail"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t w = 0; w < 15; ++w) {
        v += vocab[(i * 7 + w * 3) % 20];
        v += ' ';
      }
      v.pop_back();
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// English-like comma-separated tokens with MIXED lengths (1-12): CsvCorpus'
// "fieldNNN" tokens are all 6-8 bytes, which makes size-class branches in
// term-view construction predict perfectly; this corpus is the honest arm
// for anything with a size-dependent branch.
const std::vector<std::string>& MixedCsvCorpus() {
  static const auto corpus = [] {
    constexpr uint32_t kLens[] = {1, 2, 2, 3, 3, 3, 4,  4,  5,  5,
                                  6, 6, 7, 7, 8, 9, 10, 11, 12, 3};
    std::mt19937_64 rng{11};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 25; ++f) {
        // Random draw, NOT a periodic index: periodic length sequences are
        // learned by branch predictors and understate misprediction costs.
        const auto len = kLens[rng() % std::size(kLens)];
        for (uint32_t j = 0; j < len; ++j) {
          v += static_cast<char>('a' + rng() % 26);
        }
        v += ',';
      }
      v.pop_back();
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& CsvCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += "field" + std::to_string((i + f * 11) % 199) + ",";
      }
      v.pop_back();
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& SepCsvCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += "field" + std::to_string((i + f * 11) % 199) + ", ";
      }
      v.resize(v.size() - 2);
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& MultiSepCsvCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += "field" + std::to_string((i + f * 11) % 199);
        v += (f & 1) ? "; " : ", ";
      }
      v.resize(v.size() - 2);
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// One long (15B) separator with a rare first byte: boyer-moore's classic
// friendly regime (skip grows with needle length).
const std::vector<std::string>& LongSepCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 5; ++f) {
        v += "payload_field_value_" + std::to_string((i + f * 11) % 199);
        v += "|====SPLIT====|";
      }
      v.resize(v.size() - 15);
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// memchr worst case / boyer-moore best case: needle a^15|b over pure 'a'
// runs -- every position is a first-byte candidate with a deep failing
// verify.
const std::vector<std::string>& HostileRunCorpus() {
  static const auto corpus = [] {
    const std::string run(48, 'a');
    const std::string needle = std::string(15, 'a') + "b";
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v = run + needle + run + needle + run;
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// Markup-ish text: '<' is frequent, most tags are NOT delimiters (failing
// verifies), several are. Stresses candidate-dense scanning for both the
// one-string and the 16-delimiter sets.
const std::vector<std::string>& MarkupCorpus() {
  static const auto corpus = [] {
    const char* words[] = {"lorem", "ipsum", "dolor", "sit",
                           "amet",  "sed",   "elit"};
    const char* tags[] = {"<div>", "<span>",     "<b>",  "</b>",
                          "<i>",   "</i>",       "<td>", "<tr>",
                          "<br>",  "</section>", "<em>", "<li>"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t w = 0; w < 20; ++w) {
        v += words[(i * 7 + w * 3) % 7];
        v += tags[(i * 5 + w * 11) % 12];
      }
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// The multi-string scan's worst construction: 24 delimiters all sharing '<',
// 6-byte shared prefixes within each family, and a corpus where most tags
// are near-misses -- every candidate position pays deep failing verifies.
const std::vector<std::string>& HardTagCorpus() {
  static const auto corpus = [] {
    const char* words[] = {"data", "text", "node", "item", "cell"};
    const char* near[] = {"</sect99>",  "</sectXY>", "</longer>", "</longXY>",
                          "<metadata>", "<metaXY>",  "</secret>", "</sect>"};
    const char* hits[] = {"</sect03>", "</long05>", "<meta02>", "</sect11>"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t w = 0; w < 16; ++w) {
        v += words[(i + w) % 5];
        if ((i * 3 + w) % 4 == 0) {
          v += hits[(i + w) % 4];
        } else {
          v += near[(i * 7 + w) % 8];
        }
      }
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// Prose with eight mixed separators; " - " and " | " start with a space,
// which is everywhere -- dense first-byte candidates with failing verifies.
const std::vector<std::string>& MixedSepProseCorpus() {
  static const auto corpus = [] {
    const char* seps[] = {", ", "; ", ": ", " - ", " | ", "\t", "--", "\r\n"};
    const char* words[] = {"alpha", "beta gamma", "delta", "epsilon zeta",
                           "eta",   "theta iota", "kappa"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += words[(i * 7 + f * 3) % 7];
        v += seps[(i + f * 5) % 8];
      }
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& GeoJsonPointCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      const double lng = -180.0 + 360.0 * double((i * 131) % 1000) / 1000.0;
      const double lat = -85.0 + 170.0 * double((i * 173) % 997) / 997.0;
      out.push_back(R"({"type":"Point","coordinates":[)" + std::to_string(lng) +
                    "," + std::to_string(lat) + "]}");
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& GeoWkbPointCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      const double lng = -180.0 + 360.0 * double((i * 131) % 1000) / 1000.0;
      const double lat = -85.0 + 170.0 * double((i * 173) % 997) / 997.0;
      std::string wkb;
      const uint8_t le = 1;
      const uint32_t type = 1;
      wkb.append(reinterpret_cast<const char*>(&le), 1);
      wkb.append(reinterpret_cast<const char*>(&type), 4);
      wkb.append(reinterpret_cast<const char*>(&lng), 8);
      wkb.append(reinterpret_cast<const char*>(&lat), 8);
      out.push_back(std::move(wkb));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& GeoPointArrayCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      const double lng = -180.0 + 360.0 * double((i * 131) % 1000) / 1000.0;
      const double lat = -85.0 + 170.0 * double((i * 173) % 997) / 997.0;
      out.push_back("[" + std::to_string(lat) + ", " + std::to_string(lng) +
                    "]");
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& GeoJsonShapeCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    const size_t n = kValues / 8;
    out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      const double lng = -170.0 + 340.0 * double((i * 131) % 499) / 499.0;
      const double lat = -80.0 + 160.0 * double((i * 173) % 503) / 503.0;
      const double d = 0.05 + 0.001 * double(i % 40);
      const auto p = [](double x, double y) {
        return "[" + std::to_string(x) + "," + std::to_string(y) + "]";
      };
      out.push_back(R"({"type":"Polygon","coordinates":[[)" +
                    p(lng - d, lat - d) + "," + p(lng + d, lat - d) + "," +
                    p(lng + d, lat + d) + "," + p(lng - d, lat + d) + "," +
                    p(lng - d, lat - d) + "]]}");
    }
    return out;
  }();
  return corpus;
}

// Like CsvCorpus but with long (>12B, non-inline) tokens, so a producer emits
// string_t views into the value that the accumulator would otherwise copy --
// the case value-view retention targets.
const std::vector<std::string>& LongCsvCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += "longfieldtokenvalue_" + std::to_string((i + f * 11) % 199) +
             "_suffix,";
      }
      v.pop_back();
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& ColonCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += "field" + std::to_string((i + f * 11) % 199) + "::";
      }
      v.resize(v.size() - 2);
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// Horspool's best case: a 48-byte needle whose bytes are absent from the
// payload, so the bad-char rule skips a full needle per step while any
// scan-every-byte filter must still touch all of it.
const std::string& VLongNeedle() {
  static const std::string needle =
    "-----BEGIN~SERENEDB~RECORD~SEPARATOR~MARKER-----";
  return needle;
}

const std::vector<std::string>& VLongSepCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 4; ++f) {
        for (size_t w = 0; w < 24; ++w) {
          v += "payload" + std::to_string((i + f * 7 + w * 11) % 199);
          v += ' ';
        }
        v += VLongNeedle();
      }
      v.resize(v.size() - VLongNeedle().size());
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// The block first+last filter's worst case: first and last needle bytes
// co-occur at exactly stride n-1 everywhere, and the middles share a 30-byte
// prefix, so every candidate survives the mask and pays a deep verify.
const std::string& CollideNeedle() {
  static const std::string needle =
    "x" + std::string(30, 'a') + std::string(8, 'c') + "y";
  return needle;
}

const std::vector<std::string>& CollideCorpus() {
  static const auto corpus = [] {
    const std::string decoy =
      "x" + std::string(30, 'a') + std::string(8, 'd') + "y";
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 12; ++f) {
        v += decoy;
      }
      v += CollideNeedle();
      v += decoy;
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// The realistic form of the collide shape: same-width banners in a log, only
// one of which is the delimiter, separated by ordinary payload. Candidate
// density (~1 per 290B) and shared-prefix length (12B) are what a framed
// format actually produces; CollideCorpus stacks both to their maximum.
const std::string& BannerNeedle() {
  static const std::string needle = "===== BEGIN SECTION =====";
  return needle;
}

const std::vector<std::string>& BannerCorpus() {
  static const auto corpus = [] {
    const char* decoys[] = {"===== BEGIN REQUEST =====",
                            "===== BEGIN PAYLOAD =====",
                            "===== FINAL SEGMENT ====="};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 6; ++f) {
        for (size_t w = 0; w < 24; ++w) {
          v += "payload" + std::to_string((i + f * 7 + w * 11) % 199);
          v += ' ';
        }
        v += (f % 3 == 2) ? BannerNeedle() : decoys[(i + f) % 3];
      }
      v.resize(v.size() - BannerNeedle().size());
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& AccentCorpus() {
  static const auto corpus = [] {
    const char* vocab[] = {"café",        "naïve",   "résumé", "Ångström",
                           "señor",       "crème",   "brûlée", "façade",
                           "jalapeño",    "Zürich",  "über",   "piñata",
                           "déjà",        "fiancée", "cliché", "Ærø",
                           "smörgåsbord", "Þórr",    "œuvre",  "garçon"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t w = 0; w < 15; ++w) {
        v += vocab[(i * 7 + w * 3) % 20];
        v += ' ';
      }
      v.pop_back();
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& PathCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t d = 0; d < 8; ++d) {
        v += "/dir" + std::to_string((i + d * 13) % 211);
      }
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& SynonymCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      if (i % 2 == 0) {
        out.push_back("syn" + std::to_string(i % 64) + "a");
      } else {
        out.push_back("unknown" + std::to_string(i));
      }
    }
    return out;
  }();
  return corpus;
}

std::string SolrSynonymsText() {
  std::string text;
  for (size_t i = 0; i < 64; ++i) {
    const auto n = std::to_string(i);
    text += "syn" + n + "a, syn" + n + "b, syn" + n + "c\n";
  }
  return text;
}

std::string WordnetSynonymsText() {
  std::string text;
  for (size_t i = 0; i < 64; ++i) {
    const auto id = std::to_string(100000000 + i);
    const auto n = std::to_string(i);
    text += "s(" + id + ",1,'syn" + n + "a',a,1,0).\n";
    text += "s(" + id + ",2,'syn" + n + "b',a,1,0).\n";
    text += "s(" + id + ",3,'syn" + n + "c',a,1,0).\n";
  }
  return text;
}

// The 64-group corpora above keep every synonym dictionary in L1 and every
// `dict::WordTable` slot run at 1-3 entries, so they cannot show how the probe
// structure scales. A real WordNet is ~150k lemmas: with 3327 reachable slots
// that is a mean run of ~45, scanned linearly. These build a dictionary of that
// size while keeping the probe stream at kValues, so the arm isolates the
// dictionary's scaling rather than the token volume.
constexpr size_t kLargeSynonymGroups = 50000;

// Word-like keys with English-ish initial letters and lengths 4..12 -- both
// feed WordTable's (length, first byte) slot, so a uniform generator would
// understate the leaf-run skew. Distinct by construction: within one length the
// initial plus a base-26 encoding of a per-length counter is injective, and
// different lengths cannot collide.
std::vector<std::string> MakeVocabulary(size_t n) {
  static constexpr size_t kLenWeight[] = {6, 10, 14, 16, 15, 13, 10, 8, 8};
  static constexpr std::string_view kInitials =
    "ttttaaassssoowwiiccbbpphhffmmddrreeennngglluuvvkjqxyz";
  size_t total = 0;
  for (const auto weight : kLenWeight) {
    total += weight;
  }
  std::array<size_t, std::size(kLenWeight)> counter{};
  std::vector<std::string> out;
  out.reserve(n);
  for (size_t cursor = 0; out.size() < n; ++cursor) {
    size_t r = cursor % total;
    size_t li = 0;
    while (r >= kLenWeight[li]) {
      r -= kLenWeight[li];
      ++li;
    }
    size_t v = counter[li]++;
    std::string word;
    word.reserve(li + 4);
    word.push_back(kInitials[v % kInitials.size()]);
    v /= kInitials.size();
    for (size_t k = 1; k < li + 4; ++k) {
      word.push_back(static_cast<char>('a' + v % 26));
      v /= 26;
    }
    out.push_back(std::move(word));
  }
  return out;
}

// Members first, then kValues non-members drawn from the same generator -- so
// misses land in populated slots instead of being rejected by an empty one.
const std::vector<std::string>& LargeSynonymVocab() {
  static const auto vocab = MakeVocabulary(3 * kLargeSynonymGroups + kValues);
  return vocab;
}

const std::vector<std::string>& LargeSynonymCorpus() {
  static const auto corpus = [] {
    const auto& vocab = LargeSynonymVocab();
    const size_t members = 3 * kLargeSynonymGroups;
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      // ~40% hits, scrambled so the branch predictor cannot learn the pattern.
      const bool hit = ((i * 2654435761u) >> 13) % 5 < 2;
      out.push_back(hit ? vocab[(i * 7919) % members] : vocab[members + i]);
    }
    return out;
  }();
  return corpus;
}

// WordCorpus has 8*97 = 776 distinct words, so the `stem` arm measures a fully
// warm StemCache and never reaches its insert or overflow paths.
constexpr size_t kHighCardValues = 200000;

const std::vector<std::string>& HighCardWordCorpus() {
  static const auto corpus = MakeVocabulary(kHighCardValues);
  return corpus;
}

std::string SolrSynonymsTextLarge() {
  const auto& vocab = LargeSynonymVocab();
  std::string text;
  for (size_t g = 0; g < kLargeSynonymGroups; ++g) {
    text += vocab[3 * g];
    text += ", ";
    text += vocab[3 * g + 1];
    text += ", ";
    text += vocab[3 * g + 2];
    text += "\n";
  }
  return text;
}

std::string WordnetSynonymsTextLarge() {
  const auto& vocab = LargeSynonymVocab();
  std::string text;
  for (size_t g = 0; g < kLargeSynonymGroups; ++g) {
    const auto id = std::to_string(100000000 + g);
    for (size_t k = 0; k < 3; ++k) {
      text += "s(" + id + "," + std::to_string(k + 1) + ",'" +
              vocab[3 * g + k] + "',a,1,0).\n";
    }
  }
  return text;
}

class BenchSink final : public TokenConsumer, public StoreSink {
 public:
  explicit BenchSink(TokenLayout l) : layout{l} { writer.Bind(*this, this); }

  void OnStore(doc_id_t, bytes_view) final {}

  TokenLayout layout;

  // Branch-free per-term touch: folding the raw 16-byte handle keeps every
  // emitted slot observable without GetData's per-token IsInlined branch,
  // which inflated gram-heavy arms by 10-21% of their runtime.
  void Consume(TokenBatch& batch, DocRuns) final {
    uint64_t h = 0;
    for (uint32_t i = 0; i < batch.count; ++i) {
      uint64_t lo;
      uint64_t hi;
      std::memcpy(&lo, &batch.terms[i], sizeof lo);
      std::memcpy(&hi, reinterpret_cast<const char*>(&batch.terms[i]) + 8,
                  sizeof hi);
      h ^= lo + hi;
    }
    benchmark::DoNotOptimize(h);
    consumed += batch.count;
  }

  TokenSink writer;
  size_t consumed = 0;
};

using Factory = std::function<Tokenizer::ptr()>;
using CorpusFn = const std::vector<std::string>& (*)();

std::vector<duckdb::string_t> AsValues(const std::vector<std::string>& corpus) {
  std::vector<duckdb::string_t> values;
  values.reserve(corpus.size());
  for (const auto& v : corpus) {
    values.emplace_back(v.c_str(), static_cast<uint32_t>(v.size()));
  }
  return values;
}

irs::analysis::Tokenizer::ptr MakeBound(Factory make) {
  static duckdb::DuckDB gDb{nullptr};
  static duckdb::Connection gCon{gDb};
  auto stream = make();
  stream->Bind(*gCon.context);
  return stream;
}

void BM_Fill(benchmark::State& state, Factory make, CorpusFn corpus_fn) {
  auto stream = MakeBound(make);
  const auto values = AsValues(corpus_fn());
  BenchSink sink{TokenLayout::Terms};
  for (auto _ : state) {
    for (const auto& v : values) {
      if (!stream->Fill(v, sink.writer, {sink.layout})) {
        continue;
      }
    }
    sink.writer.Finish();
  }
  state.counters["tokens/s"] = benchmark::Counter(
    static_cast<double>(sink.consumed), benchmark::Counter::kIsRate);
}

void FillColumnSpan(analysis::Tokenizer& stream,
                    std::span<const duckdb::string_t> vals, doc_id_t first_doc,
                    TokenSink& sink, TokenLayout layout) {
  duckdb::UnifiedVectorFormat fmt;
  fmt.sel = duckdb::FlatVector::IncrementalSelectionVector();
  fmt.data = reinterpret_cast<duckdb::const_data_ptr_t>(vals.data());
  fmt.physical_type = duckdb::PhysicalType::VARCHAR;
  stream.Fill(fmt, static_cast<uint32_t>(vals.size()), first_doc, sink,
              {layout});
}

void BM_FillColumn(benchmark::State& state, Factory make, CorpusFn corpus_fn) {
  auto stream = MakeBound(make);
  const auto& corpus = corpus_fn();
  std::vector<duckdb::string_t> vals;
  vals.reserve(corpus.size());
  for (size_t i = 0; i < corpus.size(); ++i) {
    vals.emplace_back(corpus[i].data(),
                      static_cast<uint32_t>(corpus[i].size()));
  }
  BenchSink sink{TokenLayout::Terms};
  for (auto _ : state) {
    FillColumnSpan(*stream, vals, 1, sink.writer, sink.layout);
    sink.writer.Finish();
  }
  state.counters["tokens/s"] = benchmark::Counter(
    static_cast<double>(sink.consumed), benchmark::Counter::kIsRate);
}

// Shared consume stage replicating the production resolver contract: byte
// batches resolve term-by-term. Noinline so codegen is identical across
// pipelines.
[[gnu::noinline]] uint64_t ConsumeBatch(TermDictionary& dict, TokenBatch& buf) {
  uint64_t h = 0;
  for (uint32_t i = 0; i < buf.count; ++i) {
    h ^= dict.Insert(buf.terms[i]);
  }
  return h;
}

// Full-pipeline modes: tokens end as dictionary ids, either fused at emit
// (dict-carrying sink) or resolved at flush from the byte batch (the old
// two-pass pipeline).
void BM_FillResolve(benchmark::State& state, Factory make, CorpusFn corpus_fn) {
  auto stream = MakeBound(make);
  const auto values = AsValues(corpus_fn());
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, IndexFeatures::None);
  size_t consumed = 0;
  struct ResolveSink final : TokenConsumer, StoreSink {
    ResolveSink(TermDictionary& d, size_t& c) : dict(d), c(c) {
      writer.Bind(*this, this);
    }

    void OnStore(doc_id_t, bytes_view) final {}
    void Consume(TokenBatch& batch, DocRuns) final {
      benchmark::DoNotOptimize(ConsumeBatch(dict, batch));
      c += batch.count;
    }
    TermDictionary& dict;
    size_t& c;
    TokenSink writer;
  } sink{field->Dictionary(), consumed};
  for (auto _ : state) {
    for (const auto& v : values) {
      if (!stream->Fill(v, sink.writer, {TokenLayout::Terms})) {
        continue;
      }
    }
    sink.writer.Finish();
  }
  state.counters["tokens/s"] = benchmark::Counter(static_cast<double>(consumed),
                                                  benchmark::Counter::kIsRate);
}

Tokenizer::ptr MakeKeyword() { return KeywordTokenizer::Make({}); }

Tokenizer::ptr MakeNorm() {
  NormalizingTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("en");
  opts.case_convert = Case::Lower;
  opts.accent = false;
  return NormalizingTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeNormNfkcAccent() {
  NormalizingTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("en");
  opts.case_convert = Case::Lower;
  opts.accent = false;
  opts.form = NormForm::Nfkc;
  return NormalizingTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeCollation() {
  CollationTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("de");
  return CollationTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeGeoJsonPoint() {
  return GeoJsonAnalyzer::Make({.type = GeoJsonAnalyzer::Type::Point,
                                .coding = GeoJsonAnalyzer::Coding::S2Point});
}

Tokenizer::ptr MakeGeoJsonShape() {
  return GeoJsonAnalyzer::Make({.type = GeoJsonAnalyzer::Type::Shape,
                                .coding = GeoJsonAnalyzer::Coding::Source});
}

Tokenizer::ptr MakeGeoJsonPointWkb() {
  auto stream = MakeGeoJsonPoint();
  static_cast<GeoJsonAnalyzer*>(stream.get())->SetWkbInput(true);
  return stream;
}

Tokenizer::ptr MakeGeoPoint() { return GeoPointAnalyzer::Make({}); }

Tokenizer::ptr MakeGeoPointWkb() {
  auto stream = GeoPointAnalyzer::Make({});
  static_cast<GeoPointAnalyzer*>(stream.get())->SetWkbInput(true);
  return stream;
}

Tokenizer::ptr MakeStem() {
  StemmingTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("en");
  return StemmingTokenizer::Make(std::move(opts));
}

// English-like single words, one per value: realistic (first byte, length)
// collision pressure against an English stopword set -- the regime where the
// negative prefilter earns or loses its keep.
const std::vector<std::string>& LowerWordCorpus() {
  static const auto corpus = [] {
    const char* words[] = {
      "the",     "quick", "brown",     "fox",    "jumps",    "over",
      "lazy",    "dog",   "while",     "seven",  "wizards",  "brew",
      "potions", "under", "moonlight", "and",    "watch",    "distant",
      "ships",   "sail",  "running",   "jumped", "mountain", "river",
      "stone",   "cloud", "wind",      "fire",   "water",    "earth",
      "this",    "those"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      out.push_back(words[(i * 7) % std::size(words)]);
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& ProseWordCorpus() {
  static const auto corpus = [] {
    const char* stop[] = {"the",  "and", "of",  "to",   "is",   "in",
                          "that", "was", "for", "with", "this", "have"};
    const char* content[] = {"quick",    "brown",    "fox",     "jumps",
                             "lazy",     "wizards",  "potions", "moonlight",
                             "database", "mountain", "river",   "distant",
                             "ships",    "stone",    "running", "seven"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      if (i % 5 < 2) {
        out.push_back(stop[(i * 7) % std::size(stop)]);
      } else {
        out.push_back(content[(i * 11) % std::size(content)]);
      }
    }
    return out;
  }();
  return corpus;
}

Tokenizer::ptr MakeStopwordsEn() {
  return StopwordsTokenizer::Make(
    {.mask = {"the",  "and",   "over", "under", "a",    "an",   "as",  "at",
              "be",   "by",    "for",  "from",  "has",  "he",   "in",  "is",
              "it",   "its",   "of",   "on",    "that", "to",   "was", "were",
              "will", "with",  "this", "but",   "they", "have", "had", "what",
              "when", "where", "who",  "which", "why",  "how",  "all", "each"}},
    tests::Cache());
}

Tokenizer::ptr MakeStopwords() {
  return StopwordsTokenizer::Make({.mask = {"the", "and", "over", "under"}},
                                  tests::Cache());
}

Tokenizer::ptr MakeDelimiter() { return DelimitedTokenizer::Make({","}); }

Tokenizer::ptr MakeSqlUpper() {
  return SqlTokenizer::Make({.expression = "upper(input)"});
}

Tokenizer::ptr MakeSqlSplit() {
  return SqlTokenizer::Make({.expression = "string_split(input, ',')"});
}

Tokenizer::ptr MakeSqlSplitLower() {
  return SqlTokenizer::Make({.expression = "string_split(lower(input), ',')"});
}

Tokenizer::ptr MakeUnion2() {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(MakeDelimiter());
  subs.push_back(KeywordTokenizer::Make({}));
  return std::make_unique<UnionTokenizer>(std::move(subs));
}

Tokenizer::ptr MakeShingle() {
  ShingleTokenizer::Options opts;
  opts.min_shingle_size = 2;
  opts.max_shingle_size = 4;
  return std::make_unique<ShingleTokenizer>(MakeDelimiter(), std::move(opts));
}

Tokenizer::ptr MakeShingleFrequent() {
  ShingleTokenizer::Options opts;
  opts.min_shingle_size = 2;
  opts.max_shingle_size = 3;
  for (const char* w : {"the", "and", "over", "under"}) {
    opts.frequent_words.emplace_back(reinterpret_cast<const byte_type*>(w),
                                     std::strlen(w));
  }
  return std::make_unique<ShingleTokenizer>(DelimitedTokenizer::Make({" "}),
                                            std::move(opts));
}

Tokenizer::ptr MakeMultiDelimiterStr() {
  MultiDelimitedTokenizer::Options opts;
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(", "), 2);
  return MultiDelimitedTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeMultiDelimiterStrings() {
  MultiDelimitedTokenizer::Options opts;
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(", "), 2);
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>("; "), 2);
  return MultiDelimitedTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeMultiDelimiterFrom(std::initializer_list<const char*> ds) {
  MultiDelimitedTokenizer::Options opts;
  for (const char* d : ds) {
    opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(d),
                                 std::strlen(d));
  }
  return MultiDelimitedTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeMultiDelimiterStrLong() {
  return MakeMultiDelimiterFrom({"|====SPLIT====|"});
}

Tokenizer::ptr MakeMultiDelimiterStrHostile() {
  return MakeMultiDelimiterFrom({"aaaaaaaaaaaaaaab"});
}

Tokenizer::ptr MakeMultiDelimiterStrVLong() {
  return MakeMultiDelimiterFrom({VLongNeedle().c_str()});
}

Tokenizer::ptr MakeMultiDelimiterStrCollide() {
  return MakeMultiDelimiterFrom({CollideNeedle().c_str()});
}

Tokenizer::ptr MakeMultiDelimiterStrBanner() {
  return MakeMultiDelimiterFrom({BannerNeedle().c_str()});
}

Tokenizer::ptr MakeMultiDelimiterStrTag() {
  return MakeMultiDelimiterFrom({"</section>"});
}

Tokenizer::ptr MakeMultiDelimiterTags() {
  return MakeMultiDelimiterFrom({"</b>", "</i>", "<br>", "<td>", "<tr>", "<em>",
                                 "<li>", "<ul>", "<ol>", "<hr>", "</p>", "</a>",
                                 "</s>", "</u>", "</h2>", "</h3>"});
}

Tokenizer::ptr MakeMultiDelimiterTagsHard() {
  MultiDelimitedTokenizer::Options opts;
  const auto add = [&](const std::string& s) {
    opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(s.data()),
                                 s.size());
  };
  const auto pad2 = [](int k) {
    return (k < 10 ? "0" : "") + std::to_string(k);
  };
  for (int k = 1; k <= 12; ++k) {
    add("</sect" + pad2(k) + ">");
  }
  for (int k = 1; k <= 8; ++k) {
    add("</long" + pad2(k) + ">");
  }
  for (int k = 1; k <= 4; ++k) {
    add("<meta" + pad2(k) + ">");
  }
  return MultiDelimitedTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeMultiDelimiterMixed8() {
  return MakeMultiDelimiterFrom(
    {", ", "; ", ": ", " - ", " | ", "\t", "--", "\r\n"});
}

Tokenizer::ptr MakeMultiDelimiter() {
  MultiDelimitedTokenizer::Options opts;
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(","), 1);
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(";"), 1);
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>("|"), 1);
  return MultiDelimitedTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakePattern() {
  return PatternTokenizer::Make({.pattern = "\\s+", .group = -1});
}

Tokenizer::ptr MakePatternChar() {
  return PatternTokenizer::Make({.pattern = ",", .group = -1});
}

Tokenizer::ptr MakePatternLiteral() {
  return PatternTokenizer::Make({.pattern = "::", .group = -1});
}
Tokenizer::ptr MakePatternLiteralRegex() {
  return PatternTokenizer::Make({.pattern = ":{2}", .group = -1});
}
Tokenizer::ptr MakePatternNonWord() {
  return PatternTokenizer::Make({.pattern = "\\W+", .group = -1});
}
Tokenizer::ptr MakePatternWords() {
  return PatternTokenizer::Make({.pattern = "\\S+", .group = 0});
}
Tokenizer::ptr MakeSplitNonAlpha() {
  return SplitByNonAlphaTokenizer::Make({});
}
Tokenizer::ptr MakeSplitNonAlphaLower() {
  return SplitByNonAlphaTokenizer::Make({.case_convert = Case::Lower});
}

Tokenizer::ptr MakePathHierarchy() { return PathHierarchyTokenizer::Make({}); }

Tokenizer::ptr MakePathHierarchyReverse() {
  return PathHierarchyTokenizer::Make({.reverse = true});
}

Tokenizer::ptr MakePathHierarchyReplace() {
  return PathHierarchyTokenizer::Make({.replacement = "__"});
}

Tokenizer::ptr MakePathHierarchyReverseReplace() {
  return PathHierarchyTokenizer::Make({.replacement = "__", .reverse = true});
}

Tokenizer::ptr MakeNgram(NGramTokenizerBase::InputType input) {
  NGramTokenizerBase::Options opts;
  opts.min_gram = 3;
  opts.max_gram = 3;
  opts.preserve_original = false;
  opts.stream_bytes_type = input;
  return NGramTokenizerBase::Make(std::move(opts));
}

Tokenizer::ptr MakeNgramVariable() {
  NGramTokenizerBase::Options opts;
  opts.min_gram = 2;
  opts.max_gram = 4;
  opts.preserve_original = false;
  opts.stream_bytes_type = NGramTokenizerBase::InputType::Binary;
  return NGramTokenizerBase::Make(std::move(opts));
}

Tokenizer::ptr MakeNgramMode(NGramTokenizerBase::NGramMode mode) {
  NGramTokenizerBase::Options opts;
  opts.min_gram = 2;
  opts.max_gram = 4;
  opts.preserve_original = false;
  opts.stream_bytes_type = NGramTokenizerBase::InputType::Binary;
  opts.ngram_mode = mode;
  return NGramTokenizerBase::Make(std::move(opts));
}

Tokenizer::ptr MakeNgramPrefix() {
  return MakeNgramMode(NGramTokenizerBase::NGramMode::Prefix);
}

Tokenizer::ptr MakeNgramSuffix() {
  return MakeNgramMode(NGramTokenizerBase::NGramMode::Suffix);
}

Tokenizer::ptr MakeNgramPrefixSuffix() {
  return MakeNgramMode(NGramTokenizerBase::NGramMode::PrefixAndSuffix);
}

Tokenizer::ptr MakeNgramVariableMarked() {
  NGramTokenizerBase::Options opts;
  opts.min_gram = 2;
  opts.max_gram = 4;
  opts.preserve_original = true;
  opts.stream_bytes_type = NGramTokenizerBase::InputType::Binary;
  opts.end_marker = irs::ViewCast<irs::byte_type>(std::string_view{"$"});
  return NGramTokenizerBase::Make(std::move(opts));
}

Tokenizer::ptr MakeNgramBinary() {
  return MakeNgram(NGramTokenizerBase::InputType::Binary);
}

Tokenizer::ptr MakeNgramUtf8() {
  return MakeNgram(NGramTokenizerBase::InputType::UTF8);
}

Tokenizer::ptr MakeSparseNgram() { return SparseNGramTokenizer::Make({}); }

Tokenizer::ptr MakeWildcard() {
  return WildcardAnalyzer::Make({}, tests::Cache());
}

Tokenizer::ptr MakeSegmentation() { return SegmentationTokenizer::Make({}); }

Tokenizer::ptr MakeIcuText() {
  return IcuTextTokenizer::Make(
    {.locale = icu::Locale::createFromName("en_US")});
}

Tokenizer::ptr MakeIcuSentence() {
  return IcuTextTokenizer::Make(
    {.separate = IcuTextTokenizer::Options::Separate::Sentence,
     .locale = icu::Locale::createFromName("en_US")});
}

Tokenizer::ptr MakeTextEn() {
  TextTokenizer::Options o;
  o.locale = icu::Locale::createFromName("en_US.UTF-8");
  o.explicit_stopwords = {"the", "and", "of", "a"};
  o.explicit_stopwords_set = true;
  return TextTokenizer::Make(std::move(o), tests::Cache());
}

Tokenizer::ptr MakeSolrSynonyms() {
  return SolrSynonymsTokenizer::Make({.synonyms_text = SolrSynonymsText()},
                                     tests::Cache());
}

Tokenizer::ptr MakeSolrSynonymsLarge() {
  return SolrSynonymsTokenizer::Make({.synonyms_text = SolrSynonymsTextLarge()},
                                     tests::Cache());
}

Tokenizer::ptr MakePipelineTextImpl(bool seg_lower) {
  using Convert = SegmentationTokenizer::Options::Convert;
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(SegmentationTokenizer::Make(
    {.convert = seg_lower ? Convert::Lower : Convert::None}));
  {
    NormalizingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    o.case_convert = seg_lower ? Case::None : Case::Lower;
    o.accent = false;
    subs.push_back(NormalizingTokenizer::Make(std::move(o)));
  }
  {
    StopwordsTokenizer::Options s;
    s.mask = {"the", "and", "of", "a"};
    subs.push_back(StopwordsTokenizer::Make(std::move(s), tests::Cache()));
  }
  {
    StemmingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    subs.push_back(StemmingTokenizer::Make(std::move(o)));
  }
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}

Tokenizer::ptr MakePipelineText() { return MakePipelineTextImpl(false); }
Tokenizer::ptr MakePipelineTextSegLower() { return MakePipelineTextImpl(true); }

Tokenizer::ptr MakePipelineSegNgram() {
  using Convert = SegmentationTokenizer::Options::Convert;
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(SegmentationTokenizer::Make({.convert = Convert::Lower}));
  NGramTokenizerBase::Options o;
  o.min_gram = 3;
  o.max_gram = 3;
  o.preserve_original = false;
  o.stream_bytes_type = NGramTokenizerBase::InputType::UTF8;
  subs.push_back(NGramTokenizerBase::Make(std::move(o)));
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}

Tokenizer::ptr MakePipelineT2() {
  PipelineTokenizer::Options popts;
  auto add = [&](TokenizerConfig cfg) {
    popts.children.push_back(std::make_unique<TokenizerConfig>(std::move(cfg)));
  };
  {
    TokenizerConfig c;
    c.config = DelimitedTokenizer::Options{.delimiter = ","};
    add(std::move(c));
  }
  {
    TokenizerConfig c;
    StopwordsTokenizer::Options s;
    s.mask = {"field1", "field3", "field5", "field7"};
    c.config = std::move(s);
    add(std::move(c));
  }
  TokenizerConfig cfg;
  cfg.config = std::move(popts);
  return CreateTokenizer(std::move(cfg), tests::Cache());
}

Tokenizer::ptr MakePipelineT2Ngram() {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(DelimitedTokenizer::Make({","}));
  NGramTokenizerBase::Options o;
  o.min_gram = 2;
  o.max_gram = 3;
  o.preserve_original = false;
  o.stream_bytes_type = NGramTokenizerBase::InputType::UTF8;
  subs.push_back(NGramTokenizerBase::Make(std::move(o)));
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}

std::string PipelineSynText() {
  std::string text;
  for (size_t i = 0; i < 16; ++i) {
    const auto n = std::to_string(i * 12 + 2);
    text += "field" + n + " => f" + n + "a, f" + n + "b\n";
  }
  return text;
}

Tokenizer::ptr MakePipelineT2Syn() {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(DelimitedTokenizer::Make({","}));
  StopwordsTokenizer::Options s;
  s.mask = {"field1", "field3", "field5", "field7"};
  subs.push_back(StopwordsTokenizer::Make(std::move(s), tests::Cache()));
  subs.push_back(SolrSynonymsTokenizer::Make(
    {.synonyms_text = PipelineSynText()}, tests::Cache()));
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}

Tokenizer::ptr MakePipelineT2RewriteImpl(bool with_stem) {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(DelimitedTokenizer::Make({","}));
  {
    NormalizingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    o.case_convert = Case::Lower;
    o.accent = false;
    subs.push_back(NormalizingTokenizer::Make(std::move(o)));
  }
  if (with_stem) {
    StemmingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    subs.push_back(StemmingTokenizer::Make(std::move(o)));
  }
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}
Tokenizer::ptr MakePipelineT2Coll() {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(DelimitedTokenizer::Make({","}));
  subs.push_back(CollationTokenizer::Make(
    {.locale = icu::Locale::createFromName("en_US.UTF-8")}));
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}

Tokenizer::ptr MakePipelineSegStop() {
  using Convert = SegmentationTokenizer::Options::Convert;
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(SegmentationTokenizer::Make({.convert = Convert::None}));
  {
    StopwordsTokenizer::Options o;
    o.mask = {"the", "and", "of", "a"};
    subs.push_back(StopwordsTokenizer::Make(std::move(o), tests::Cache()));
  }
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}

Tokenizer::ptr MakePipelineT2Norm() { return MakePipelineT2RewriteImpl(false); }
Tokenizer::ptr MakePipelineT2Stem() { return MakePipelineT2RewriteImpl(true); }

Tokenizer::ptr MakePipelineT2Sql() {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(DelimitedTokenizer::Make({","}));
  subs.push_back(SqlTokenizer::Make({.expression = "upper(input)"}));
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}

Tokenizer::ptr MakePipelineSqlSplitNorm() {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(
    SqlTokenizer::Make({.expression = "string_split(input, ',')"}));
  NormalizingTokenizer::Options o;
  o.locale = icu::Locale::createFromName("en");
  o.case_convert = Case::Lower;
  o.accent = false;
  subs.push_back(NormalizingTokenizer::Make(std::move(o)));
  return std::make_unique<PipelineTokenizer>(std::move(subs));
}

Tokenizer::ptr MakeWordnetSynonyms() {
  return WordnetSynonymsTokenizer::Make(
    {.synonyms_text = WordnetSynonymsText()}, tests::Cache());
}

Tokenizer::ptr MakeWordnetSynonymsLarge() {
  return WordnetSynonymsTokenizer::Make(
    {.synonyms_text = WordnetSynonymsTextLarge()}, tests::Cache());
}

#define TOKENIZER_BENCH(name, factory, corpus)               \
  BENCHMARK_CAPTURE(BM_Fill, name, &factory, &corpus)        \
    ->Unit(benchmark::kMillisecond);                         \
  BENCHMARK_CAPTURE(BM_FillColumn, name, &factory, &corpus)  \
    ->Unit(benchmark::kMillisecond);                         \
  BENCHMARK_CAPTURE(BM_FillResolve, name, &factory, &corpus) \
    ->Unit(benchmark::kMillisecond)

TOKENIZER_BENCH(keyword, MakeKeyword, WordCorpus);
TOKENIZER_BENCH(norm, MakeNorm, WordCorpus);
BENCHMARK_CAPTURE(BM_Fill, norm_nfkc_accent, &MakeNormNfkcAccent, &AccentCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, norm_unicode, &MakeNorm, &TextUnicodeCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(collation, MakeCollation, WordCorpus);
TOKENIZER_BENCH(geojson_point, MakeGeoJsonPoint, GeoJsonPointCorpus);
TOKENIZER_BENCH(geojson_point_wkb, MakeGeoJsonPointWkb, GeoWkbPointCorpus);
TOKENIZER_BENCH(geojson_shape, MakeGeoJsonShape, GeoJsonShapeCorpus);
TOKENIZER_BENCH(geopoint, MakeGeoPoint, GeoPointArrayCorpus);
TOKENIZER_BENCH(geopoint_wkb, MakeGeoPointWkb, GeoWkbPointCorpus);
TOKENIZER_BENCH(stem, MakeStem, WordCorpus);
BENCHMARK_CAPTURE(BM_Fill, stem_highcard, &MakeStem, &HighCardWordCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(stopwords, MakeStopwords, WordCorpus);
TOKENIZER_BENCH(stopwords_en, MakeStopwordsEn, LowerWordCorpus);
TOKENIZER_BENCH(stopwords_en_prose, MakeStopwordsEn, ProseWordCorpus);
TOKENIZER_BENCH(delimiter, MakeDelimiter, CsvCorpus);
TOKENIZER_BENCH(sql_upper, MakeSqlUpper, WordCorpus);
TOKENIZER_BENCH(sql_split, MakeSqlSplit, CsvCorpus);
TOKENIZER_BENCH(sql_split_lower, MakeSqlSplitLower, CsvCorpus);
TOKENIZER_BENCH(shingle, MakeShingle, CsvCorpus);
TOKENIZER_BENCH(shingle_frequent, MakeShingleFrequent, TextCorpus);
TOKENIZER_BENCH(union_2, MakeUnion2, CsvCorpus);
TOKENIZER_BENCH(delimiter_mixed, MakeDelimiter, MixedCsvCorpus);
TOKENIZER_BENCH(multi_delimiter, MakeMultiDelimiter, CsvCorpus);
TOKENIZER_BENCH(multi_delimiter_mixed, MakeMultiDelimiter, MixedCsvCorpus);
TOKENIZER_BENCH(multi_delimiter_str, MakeMultiDelimiterStr, SepCsvCorpus);
TOKENIZER_BENCH(multi_delimiter_strings, MakeMultiDelimiterStrings,
                MultiSepCsvCorpus);
TOKENIZER_BENCH(multi_delimiter_str_long, MakeMultiDelimiterStrLong,
                LongSepCorpus);
TOKENIZER_BENCH(multi_delimiter_str_hostile, MakeMultiDelimiterStrHostile,
                HostileRunCorpus);
TOKENIZER_BENCH(multi_delimiter_str_vlong, MakeMultiDelimiterStrVLong,
                VLongSepCorpus);
TOKENIZER_BENCH(multi_delimiter_str_collide, MakeMultiDelimiterStrCollide,
                CollideCorpus);
TOKENIZER_BENCH(multi_delimiter_str_banner, MakeMultiDelimiterStrBanner,
                BannerCorpus);
TOKENIZER_BENCH(multi_delimiter_str_tag, MakeMultiDelimiterStrTag,
                MarkupCorpus);
TOKENIZER_BENCH(multi_delimiter_tags, MakeMultiDelimiterTags, MarkupCorpus);
TOKENIZER_BENCH(multi_delimiter_tags_hard, MakeMultiDelimiterTagsHard,
                HardTagCorpus);
TOKENIZER_BENCH(multi_delimiter_mixed8, MakeMultiDelimiterMixed8,
                MixedSepProseCorpus);
TOKENIZER_BENCH(pattern, MakePattern, TextCorpus);
BENCHMARK_CAPTURE(BM_Fill, pattern_char, &MakePatternChar, &CsvCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pattern_char_sparse, &MakePatternChar, &TextCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pattern_literal, &MakePatternLiteral, &ColonCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pattern_literal_regex, &MakePatternLiteralRegex,
                  &ColonCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pattern_nonword, &MakePatternNonWord, &TextCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pattern_words, &MakePatternWords, &TextCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, split_non_alpha, &MakeSplitNonAlpha, &TextCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, split_non_alpha_lower, &MakeSplitNonAlphaLower,
                  &TextCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(path_hierarchy, MakePathHierarchy, PathCorpus);
TOKENIZER_BENCH(path_hierarchy_reverse, MakePathHierarchyReverse, PathCorpus);
TOKENIZER_BENCH(path_hierarchy_replace, MakePathHierarchyReplace, PathCorpus);
TOKENIZER_BENCH(path_hierarchy_reverse_replace, MakePathHierarchyReverseReplace,
                PathCorpus);
TOKENIZER_BENCH(ngram_binary, MakeNgramBinary, TextCorpus);
TOKENIZER_BENCH(ngram_variable, MakeNgramVariable, TextCorpus);
TOKENIZER_BENCH(ngram_variable_marked, MakeNgramVariableMarked, TextCorpus);
TOKENIZER_BENCH(ngram_prefix, MakeNgramPrefix, TextCorpus);
TOKENIZER_BENCH(ngram_suffix, MakeNgramSuffix, TextCorpus);
TOKENIZER_BENCH(ngram_prefix_suffix, MakeNgramPrefixSuffix, TextCorpus);
TOKENIZER_BENCH(ngram_utf8, MakeNgramUtf8, TextCorpus);
BENCHMARK_CAPTURE(BM_Fill, ngram_utf8_unicode, &MakeNgramUtf8,
                  &TextUnicodeCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(sparse_ngram, MakeSparseNgram, TextCorpus);
TOKENIZER_BENCH(wildcard, MakeWildcard, TextCorpus);
BENCHMARK_CAPTURE(BM_Fill, wildcard_unicode, &MakeWildcard, &TextUnicodeCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(segmentation, MakeSegmentation, TextCorpus);
BENCHMARK_CAPTURE(BM_Fill, segmentation_unicode, &MakeSegmentation,
                  &TextUnicodeCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(icu_text, MakeIcuText, TextCorpus);
BENCHMARK_CAPTURE(BM_Fill, icu_text_unicode, &MakeIcuText, &TextUnicodeCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(icu_text_cjk, MakeIcuText, CjkCorpus);
TOKENIZER_BENCH(icu_text_sentence, MakeIcuSentence, TextCorpus);
TOKENIZER_BENCH(text_en, MakeTextEn, TextCorpus);
TOKENIZER_BENCH(pipeline_text_en, MakePipelineText, TextCorpus);
BENCHMARK_CAPTURE(BM_FillColumn, pipeline_text_en_mixed, &MakePipelineText,
                  &TextMixedCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(pipeline_text_en_seglower, MakePipelineTextSegLower,
                TextCorpus);
TOKENIZER_BENCH(pipeline_seg_ngram, MakePipelineSegNgram, TextCorpus);
TOKENIZER_BENCH(pipeline_seg_stop, MakePipelineSegStop, TextCorpus);
BENCHMARK_CAPTURE(BM_Fill, pipeline_t2coll, &MakePipelineT2Coll, &CsvCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pipeline_text_en_unicode, &MakePipelineText,
                  &TextUnicodeCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pipeline_text_en_seglower_unicode,
                  &MakePipelineTextSegLower, &TextUnicodeCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, text_en_unicode, &MakeTextEn, &TextUnicodeCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(solr_synonyms, MakeSolrSynonyms, SynonymCorpus);
TOKENIZER_BENCH(solr_synonyms_large, MakeSolrSynonymsLarge, LargeSynonymCorpus);
TOKENIZER_BENCH(pipeline_t2, MakePipelineT2, CsvCorpus);
TOKENIZER_BENCH(pipeline_t2_ngram, MakePipelineT2Ngram, CsvCorpus);
BENCHMARK_CAPTURE(BM_FillColumn, pipeline_t2_ngram_long, &MakePipelineT2Ngram,
                  &LongCsvCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(pipeline_t2syn, MakePipelineT2Syn, CsvCorpus);
TOKENIZER_BENCH(pipeline_t2norm, MakePipelineT2Norm, CsvCorpus);
TOKENIZER_BENCH(pipeline_t2stem, MakePipelineT2Stem, CsvCorpus);
TOKENIZER_BENCH(pipeline_t2sql, MakePipelineT2Sql, CsvCorpus);
TOKENIZER_BENCH(pipeline_sqlsplit_norm, MakePipelineSqlSplitNorm, CsvCorpus);
TOKENIZER_BENCH(wordnet_synonyms, MakeWordnetSynonyms, SynonymCorpus);
TOKENIZER_BENCH(wordnet_synonyms_large, MakeWordnetSynonymsLarge,
                LargeSynonymCorpus);

}  // namespace

BENCHMARK_MAIN();
