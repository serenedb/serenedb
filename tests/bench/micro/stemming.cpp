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

// What does one Snowball stem cost, per language and per word shape, and what
// does the StemCache in front of it buy?
//
// The libstemmer version is a property of the checkout, not of an arm here:
// build this same file in two engine trees and compare. The header table it
// prints at startup reports which algorithms the linked library actually has,
// so a version difference shows up as a row rather than as a silent skip.
//
// Word shape is not hand-labelled -- every bucket is derived from what the
// linked stemmer does to the vocabulary at startup:
//
//   All        the whole vocabulary
//   Changed    words the stemmer rewrites, i.e. a suffix rule fires
//   Unchanged  words returned as-is, i.e. every rule was scanned for nothing
//   Short      the shortest third, where most algorithms bail on the region
//              test before looking at a suffix
//   Long       the longest third, the derivational tail that pays the most
//
// Zipf/ runs a Zipf-distributed stream over the same vocabulary, which is what
// prose looks like to the tokenizer, with and without the cache. The ratio
// between those two arms is the answer to "why is there a cache at all".

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <duckdb.hpp>
#include <iresearch/analysis/text/dict/stem_cache.hpp>
#include <iresearch/utils/snowball_stemmer.hpp>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {

using irs::analysis::dict::StemCache;
using irs::analysis::dict::StemUncached;

constexpr std::string_view kEnglish[] = {
  "running",       "runner",          "runs",       "ran",
  "easily",        "happiness",       "happily",    "national",
  "nationalize",   "nationalization", "organize",   "organizing",
  "organized",     "computers",       "computing",  "generalization",
  "generously",    "relational",      "rational",   "conditional",
  "conditionally", "arguments",       "argued",     "arguing",
  "agreed",        "plastered",       "motoring",   "conflated",
  "troubled",      "sized",           "hopping",    "falling",
  "hissing",       "fizzed",          "failing",    "meetings",
  "furthering",    "probate",         "sky",        "news",
  "knowledge",     "universities",    "president",  "described",
  "following",     "information",     "government", "different"};

constexpr std::string_view kGerman[] = {
  "laufen",       "läuft",        "gelaufen",       "häuser",
  "haus",         "mäuse",        "freundlich",     "freundschaft",
  "arbeiten",     "arbeitete",    "gearbeitet",     "kinder",
  "kindern",      "schönste",     "schöner",        "wissenschaftlich",
  "wissenschaft", "gesellschaft", "gesellschaften", "bücher",
  "buch",         "lesen",        "gelesen",        "möglichkeiten",
  "möglichkeit",  "entwicklung",  "entwicklungen",  "verstehen",
  "verstanden",   "gebracht",     "bringen",        "wichtigsten",
  "wichtig",      "sprachen",     "sprache",        "geschichte",
  "geschichten",  "menschen",     "mensch",         "zeiten",
  "zeit",         "wohnungen",    "wohnung",        "fahren",
  "gefahren",     "stadt",        "städte",         "und"};

constexpr std::string_view kFrench[] = {"manger",
                                        "mangeait",
                                        "mangé",
                                        "chanteur",
                                        "chanteuse",
                                        "chantons",
                                        "nationale",
                                        "nationaux",
                                        "national",
                                        "continuer",
                                        "continuellement",
                                        "continuité",
                                        "développement",
                                        "développer",
                                        "développé",
                                        "politiquement",
                                        "politique",
                                        "gouvernement",
                                        "gouverner",
                                        "enfants",
                                        "enfant",
                                        "heureusement",
                                        "heureux",
                                        "travailler",
                                        "travailleur",
                                        "travail",
                                        "maisons",
                                        "maison",
                                        "journée",
                                        "journaux",
                                        "journal",
                                        "premièrement",
                                        "premier",
                                        "grandement",
                                        "grand",
                                        "possibilité",
                                        "possible",
                                        "ancienne",
                                        "ancien",
                                        "écrire",
                                        "écrit",
                                        "lecture",
                                        "lire",
                                        "histoire",
                                        "histoires",
                                        "les",
                                        "avec",
                                        "dans"};

constexpr std::string_view kSpanish[] = {
  "corriendo",  "corrió",       "correr",       "cantante",      "cantaba",
  "cantado",    "nacionalidad", "nacional",     "naciones",      "nación",
  "desarrollo", "desarrollar",  "desarrollado", "políticamente", "política",
  "gobierno",   "gobernar",     "niños",        "niño",          "felicidad",
  "feliz",      "trabajar",     "trabajador",   "trabajo",       "casas",
  "casa",       "periódico",    "periódicos",   "primero",       "primeramente",
  "grande",     "grandemente",  "posibilidad",  "posible",       "antigua",
  "antiguo",    "escribir",     "escrito",      "lectura",       "leer",
  "historia",   "historias",    "tiempos",      "tiempo",        "ciudades",
  "ciudad",     "para",         "con"};

constexpr std::string_view kItalian[] = {
  "correre",     "correva",     "corso",       "cantante",      "cantava",
  "cantato",     "nazionale",   "nazionalità", "nazioni",       "nazione",
  "sviluppo",    "sviluppare",  "sviluppato",  "politicamente", "politica",
  "governo",     "governare",   "bambini",     "bambino",       "felicità",
  "felice",      "lavorare",    "lavoratore",  "lavoro",        "case",
  "casa",        "giornale",    "giornali",    "primo",         "grande",
  "grandemente", "possibilità", "possibile",   "antica",        "antico",
  "scrivere",    "scritto",     "lettura",     "leggere",       "storia",
  "storie",      "tempi",       "tempo",       "città",         "abitanti",
  "abitante",    "della",       "sono"};

constexpr std::string_view kFinnish[] = {
  "juokseminen",  "juoksee",        "juoksi",      "kirja",        "kirjassa",
  "kirjoja",      "kirjoittaa",     "kirjoitti",   "talo",         "talossa",
  "taloissa",     "talojen",        "ihminen",     "ihmiset",      "ihmisiä",
  "ihmisten",     "kaupunki",       "kaupungissa", "kaupungit",    "kysymys",
  "kysymykset",   "opiskelija",     "opiskelijat", "opiskella",    "työ",
  "työssä",       "työtä",          "ystävä",      "ystävät",      "ystävien",
  "suomalainen",  "suomalaiset",    "kieli",       "kielessä",     "kielten",
  "mahdollisuus", "mahdollisuudet", "yhteiskunta", "yhteiskunnan", "tietokone",
  "tietokoneet",  "päivä",          "päivänä",     "päivät",       "vuosi",
  "vuosien",      "vuodet",         "ja"};

constexpr std::string_view kHungarian[] = {
  "futás",       "fut",        "futott",       "könyv",       "könyvek",
  "könyvet",     "ház",        "házak",        "házban",      "ember",
  "emberek",     "embereket",  "város",        "városok",     "városban",
  "kérdés",      "kérdések",   "iskola",       "iskolák",     "dolgozik",
  "dolgozó",     "munka",      "munkák",       "barát",       "barátok",
  "nyelv",       "nyelvek",    "lehetőség",    "lehetőségek", "társadalom",
  "társadalmak", "számítógép", "számítógépek", "nap",         "napok",
  "napján",      "év",         "évek",         "évben",       "gyerek",
  "gyerekek",    "asztal",     "asztalok",     "idő",         "idők",
  "kezdet",      "kezdetek",   "egy"};

constexpr std::string_view kTurkish[] = {
  "koşmak",        "koşuyor",   "koştu",     "kitap",
  "kitaplar",      "kitapları", "ev",        "evler",
  "evlerde",       "insan",     "insanlar",  "insanların",
  "şehir",         "şehirler",  "şehirde",   "soru",
  "sorular",       "okul",      "okullar",   "çalışmak",
  "çalışıyor",     "iş",        "işler",     "arkadaş",
  "arkadaşlar",    "dil",       "diller",    "olanak",
  "olanaklar",     "toplum",    "toplumlar", "bilgisayar",
  "bilgisayarlar", "gün",       "günler",    "yıl",
  "yıllar",        "çocuk",     "çocuklar",  "masa",
  "masalar",       "zaman",     "zamanlar",  "başlangıç",
  "başlangıçlar",  "büyük",     "ve",        "bir"};

constexpr std::string_view kRussian[] = {
  "бежать",    "бежит",       "бежал",       "книга",    "книги",
  "книгам",    "дом",         "дома",        "домов",    "человек",
  "люди",      "людей",       "город",       "города",   "городе",
  "вопрос",    "вопросы",     "школа",       "школы",    "работать",
  "работает",  "работа",      "друзья",      "друг",     "язык",
  "языки",     "возможность", "возможности", "общество", "общества",
  "компьютер", "компьютеры",  "день",        "дни",      "год",
  "годы",      "ребёнок",     "дети",        "стол",     "столы",
  "время",     "времена",     "начало",      "начала",   "большой",
  "большая",   "и",           "не"};

constexpr std::string_view kGreek[] = {
  "τρέχω",       "τρέχει",   "έτρεξε",    "βιβλίο",      "βιβλία",
  "βιβλίων",     "σπίτι",    "σπίτια",    "άνθρωπος",    "άνθρωποι",
  "ανθρώπων",    "πόλη",     "πόλεις",    "ερώτηση",     "ερωτήσεις",
  "σχολείο",     "σχολεία",  "εργασία",   "εργάζομαι",   "δουλειά",
  "φίλος",       "φίλοι",    "γλώσσα",    "γλώσσες",     "δυνατότητα",
  "δυνατότητες", "κοινωνία", "κοινωνίες", "υπολογιστής", "υπολογιστές",
  "ημέρα",       "ημέρες",   "χρόνος",    "χρόνια",      "παιδί",
  "παιδιά",      "τραπέζι",  "τραπέζια",  "αρχή",        "αρχές",
  "μεγάλος",     "μεγάλη",   "γράφω",     "έγραψε",      "ιστορία",
  "ιστορίες",    "και",      "για"};

constexpr std::string_view kArabic[] = {
  "كتاب",   "كتب",     "كتبت",    "يكتب",   "مدرسة",   "مدارس",   "بيت",
  "بيوت",   "رجل",     "رجال",    "مدينة",  "مدن",     "سؤال",    "أسئلة",
  "عمل",    "أعمال",   "يعمل",    "صديق",   "أصدقاء",  "لغة",     "لغات",
  "مجتمع",  "مجتمعات", "حاسوب",   "حواسيب", "يوم",     "أيام",    "سنة",
  "سنوات",  "طفل",     "أطفال",   "طاولة",  "طاولات",  "وقت",     "أوقات",
  "بداية",  "بدايات",  "كبير",    "كبيرة",  "يكتبون",  "المدرسة", "المدينة",
  "الكتاب", "الكتب",   "والكتاب", "للكتاب", "بالكتاب", "في"};

struct Language {
  std::string_view name;
  const char* algorithm;
  std::span<const std::string_view> words;
};

constexpr Language kLanguages[] = {
  {"english", "english", kEnglish},       {"german", "german", kGerman},
  {"french", "french", kFrench},          {"spanish", "spanish", kSpanish},
  {"italian", "italian", kItalian},       {"finnish", "finnish", kFinnish},
  {"hungarian", "hungarian", kHungarian}, {"turkish", "turkish", kTurkish},
  {"russian", "russian", kRussian},       {"greek", "greek", kGreek},
  {"arabic", "arabic", kArabic}};

enum class Bucket : uint8_t {
  All,
  Changed,
  Unchanged,
  Short,
  Long,
};

constexpr std::string_view kBucketNames[] = {"All", "Changed", "Unchanged",
                                             "Short", "Long"};

constexpr size_t kProbes = 4096;

struct Corpus {
  bool available = false;
  std::deque<std::string> pool;
  std::vector<duckdb::string_t> all;
  std::vector<duckdb::string_t> changed;
  std::vector<duckdb::string_t> unchanged;
  std::vector<duckdb::string_t> shortest;
  std::vector<duckdb::string_t> longest;
  std::vector<duckdb::string_t> zipf;
};

duckdb::string_t Handle(const std::string& word) {
  return duckdb::string_t{word.data(), static_cast<uint32_t>(word.size())};
}

const Corpus& GetCorpus(const Language& lang) {
  static std::map<std::string_view, std::unique_ptr<Corpus>> cache;
  const auto it = cache.find(lang.name);
  if (it != cache.end()) {
    return *it->second;
  }

  auto corpus = std::make_unique<Corpus>();
  for (const auto word : lang.words) {
    corpus->pool.emplace_back(word);
  }

  const auto stemmer = irs::make_stemmer_ptr(lang.algorithm, nullptr);
  corpus->available = stemmer != nullptr;

  for (const auto& word : corpus->pool) {
    const auto handle = Handle(word);
    corpus->all.push_back(handle);
    if (!stemmer) {
      continue;
    }
    const auto stem = StemUncached(stemmer.get(), word);
    if (stem && *stem != word) {
      corpus->changed.push_back(handle);
    } else {
      corpus->unchanged.push_back(handle);
    }
  }

  std::vector<size_t> order(corpus->pool.size());
  std::iota(order.begin(), order.end(), size_t{0});
  std::ranges::stable_sort(order, [&](size_t l, size_t r) {
    return corpus->pool[l].size() < corpus->pool[r].size();
  });
  const auto third = std::max<size_t>(1, order.size() / 3);
  for (size_t i = 0; i < third; ++i) {
    corpus->shortest.push_back(corpus->all[order[i]]);
    corpus->longest.push_back(corpus->all[order[order.size() - 1 - i]]);
  }

  std::vector<double> weights(corpus->all.size());
  for (size_t i = 0; i < weights.size(); ++i) {
    weights[i] = 1.0 / static_cast<double>(i + 1);
  }
  std::mt19937_64 rng{0x5EEDULL};
  std::discrete_distribution<size_t> pick{weights.begin(), weights.end()};
  corpus->zipf.reserve(kProbes);
  for (size_t i = 0; i < kProbes; ++i) {
    corpus->zipf.push_back(corpus->all[pick(rng)]);
  }

  return *cache.emplace(lang.name, std::move(corpus)).first->second;
}

std::span<const duckdb::string_t> Select(const Corpus& corpus, Bucket bucket) {
  switch (bucket) {
    case Bucket::All:
      return corpus.all;
    case Bucket::Changed:
      return corpus.changed;
    case Bucket::Unchanged:
      return corpus.unchanged;
    case Bucket::Short:
      return corpus.shortest;
    case Bucket::Long:
      return corpus.longest;
  }
  return {};
}

void Account(benchmark::State& state, std::span<const duckdb::string_t> words) {
  size_t bytes = 0;
  for (const auto& word : words) {
    bytes += word.GetSize();
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(words.size()));
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(bytes));
}

void BmStem(benchmark::State& state, const Language* lang, Bucket bucket) {
  const auto& corpus = GetCorpus(*lang);
  const auto words = Select(corpus, bucket);
  if (!corpus.available || words.empty()) {
    state.SkipWithError("no stemmer for this algorithm");
    return;
  }
  const auto stemmer = irs::make_stemmer_ptr(lang->algorithm, nullptr);
  for (auto _ : state) {
    for (const auto& word : words) {
      auto stem = StemUncached(stemmer.get(), {word.GetData(), word.GetSize()});
      benchmark::DoNotOptimize(stem);
    }
  }
  Account(state, words);
}

void BmZipfUncached(benchmark::State& state, const Language* lang) {
  const auto& corpus = GetCorpus(*lang);
  if (!corpus.available) {
    state.SkipWithError("no stemmer for this algorithm");
    return;
  }
  const auto stemmer = irs::make_stemmer_ptr(lang->algorithm, nullptr);
  for (auto _ : state) {
    for (const auto& word : corpus.zipf) {
      auto stem = StemUncached(stemmer.get(), {word.GetData(), word.GetSize()});
      benchmark::DoNotOptimize(stem);
    }
  }
  Account(state, corpus.zipf);
}

void BmZipfCached(benchmark::State& state, const Language* lang) {
  const auto& corpus = GetCorpus(*lang);
  if (!corpus.available) {
    state.SkipWithError("no stemmer for this algorithm");
    return;
  }
  const auto stemmer = irs::make_stemmer_ptr(lang->algorithm, nullptr);
  StemCache cache;
  for (const auto& word : corpus.all) {
    benchmark::DoNotOptimize(cache.Stem(stemmer.get(), word));
  }
  for (auto _ : state) {
    for (const auto& word : corpus.zipf) {
      auto stem = cache.Stem(stemmer.get(), word);
      benchmark::DoNotOptimize(stem);
    }
  }
  Account(state, corpus.zipf);
}

void PrintAlgorithms() {
  std::printf("%-12s %-10s %6s %8s %10s %7s\n", "language", "algorithm",
              "words", "changed", "unchanged", "bytes");
  for (const auto& lang : kLanguages) {
    const auto& corpus = GetCorpus(lang);
    size_t bytes = 0;
    for (const auto& word : corpus.pool) {
      bytes += word.size();
    }
    std::printf("%-12.*s %-10s %6zu %8zu %10zu %7zu%s\n",
                static_cast<int>(lang.name.size()), lang.name.data(),
                lang.algorithm, corpus.all.size(), corpus.changed.size(),
                corpus.unchanged.size(), bytes,
                corpus.available ? "" : "   *** MISSING ***");
  }
  std::printf("\n");
}

void Register() {
  for (const auto& lang : kLanguages) {
    for (const auto bucket : {Bucket::All, Bucket::Changed, Bucket::Unchanged,
                              Bucket::Short, Bucket::Long}) {
      const auto index = static_cast<size_t>(bucket);
      benchmark::RegisterBenchmark("Stem/" + std::string{lang.name} + "/" +
                                     std::string{kBucketNames[index]},
                                   BmStem, &lang, bucket);
    }
    benchmark::RegisterBenchmark("Zipf/Uncached/" + std::string{lang.name},
                                 BmZipfUncached, &lang);
    benchmark::RegisterBenchmark("Zipf/Cached/" + std::string{lang.name},
                                 BmZipfCached, &lang);
  }
}

}  // namespace

int main(int argc, char** argv) {
  PrintAlgorithms();
  Register();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
