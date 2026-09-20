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
#include <zlib.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <duckdb.hpp>
#include <filesystem>
#include <fstream>
#include <iresearch/analysis/text/dict/stem_cache.hpp>
#include <iresearch/utils/snowball_stemmer.hpp>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#ifdef SERENEDB_STEMMER_GENERATED_C_CANDIDATE
extern "C" {
#include "runtime/api.h"
#include "stem_UTF_8_arabic_candidate.h"
#include "stem_UTF_8_english_candidate.h"
#include "stem_UTF_8_finnish_candidate.h"
#include "stem_UTF_8_french_candidate.h"
#include "stem_UTF_8_german_candidate.h"
#include "stem_UTF_8_greek_candidate.h"
#include "stem_UTF_8_hindi_candidate.h"
#include "stem_UTF_8_hungarian_candidate.h"
#include "stem_UTF_8_italian_candidate.h"
#include "stem_UTF_8_polish_candidate.h"
#include "stem_UTF_8_russian_candidate.h"
#include "stem_UTF_8_spanish_candidate.h"
#include "stem_UTF_8_tamil_candidate.h"
#include "stem_UTF_8_turkish_candidate.h"
}
#endif

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

constexpr std::string_view kPolish[] = {
  "dom", "domy", "domami", "książka", "książki", "człowiek",
  "ludzie", "miasto", "miasta", "pracować", "praca", "język"};

constexpr std::string_view kHindi[] = {
  "किताब", "किताबें", "घर", "घरों", "आदमी", "शहर",
  "काम", "करना", "भाषा", "दिन", "बच्चा", "बच्चों"};

constexpr std::string_view kTamil[] = {
  "புத்தகம்", "புத்தகங்கள்", "வீடு", "வீடுகள்", "மனிதன்", "நகரம்",
  "வேலை", "மொழி", "நாள்", "குழந்தை", "குழந்தைகள்", "நேரம்"};

enum class ScriptClass : uint8_t {
  LatinOrMixed,
  NonAscii,
};

struct Language {
  std::string_view name;
  const char* algorithm;
  std::span<const std::string_view> words;
  ScriptClass script;
};

constexpr Language kLanguages[] = {
  {"english", "english", kEnglish, ScriptClass::LatinOrMixed},
  {"german", "german", kGerman, ScriptClass::LatinOrMixed},
  {"french", "french", kFrench, ScriptClass::LatinOrMixed},
  {"spanish", "spanish", kSpanish, ScriptClass::LatinOrMixed},
  {"italian", "italian", kItalian, ScriptClass::LatinOrMixed},
  {"finnish", "finnish", kFinnish, ScriptClass::LatinOrMixed},
  {"hungarian", "hungarian", kHungarian, ScriptClass::LatinOrMixed},
  {"turkish", "turkish", kTurkish, ScriptClass::LatinOrMixed},
  {"russian", "russian", kRussian, ScriptClass::NonAscii},
  {"greek", "greek", kGreek, ScriptClass::NonAscii},
  {"arabic", "arabic", kArabic, ScriptClass::NonAscii},
  {"polish", "polish", kPolish, ScriptClass::LatinOrMixed},
  {"hindi", "hindi", kHindi, ScriptClass::NonAscii},
  {"tamil", "tamil", kTamil, ScriptClass::NonAscii}};

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
  bool external = false;
  std::string source;
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

size_t ExternalCorpusLimit() {
  constexpr size_t kDefaultLimit = 50000;
  const char* value = std::getenv("SERENEDB_STEMMER_CORPUS_LIMIT");
  if (!value || !*value) {
    return kDefaultLimit;
  }
  errno = 0;
  char* end = nullptr;
  const auto parsed = std::strtoull(value, &end, 10);
  if (errno || end == value || *end != '\0') {
    throw std::runtime_error{
      "SERENEDB_STEMMER_CORPUS_LIMIT must be a non-negative integer"};
  }
  return static_cast<size_t>(parsed);
}

void AddCorpusWord(Corpus& corpus, std::string word, size_t limit) {
  if (limit && corpus.pool.size() >= limit) {
    return;
  }
  if (!word.empty() && word.back() == '\r') {
    word.pop_back();
  }
  corpus.pool.emplace_back(std::move(word));
}

void LoadPlainCorpus(Corpus& corpus, const std::filesystem::path& path,
                     size_t limit) {
  std::ifstream input{path, std::ios::binary};
  if (!input) {
    throw std::runtime_error{"cannot open stemmer corpus: " + path.string()};
  }
  std::string word;
  while ((!limit || corpus.pool.size() < limit) && std::getline(input, word)) {
    AddCorpusWord(corpus, std::move(word), limit);
  }
}

void LoadGzipCorpus(Corpus& corpus, const std::filesystem::path& path,
                    size_t limit) {
  gzFile input = gzopen(path.c_str(), "rb");
  if (!input) {
    throw std::runtime_error{"cannot open stemmer corpus: " + path.string()};
  }
  std::array<char, 8192> buffer{};
  std::string word;
  while (!limit || corpus.pool.size() < limit) {
    const char* chunk = gzgets(input, buffer.data(), buffer.size());
    if (!chunk) {
      break;
    }
    word += chunk;
    if (!word.empty() && word.back() == '\n') {
      word.pop_back();
      AddCorpusWord(corpus, std::move(word), limit);
      word.clear();
    }
  }
  if ((!limit || corpus.pool.size() < limit) && !word.empty()) {
    AddCorpusWord(corpus, std::move(word), limit);
  }
  int error = Z_OK;
  const char* message = gzerror(input, &error);
  const std::string error_message = message ? message : "unknown error";
  gzclose(input);
  if (error != Z_OK && error != Z_STREAM_END) {
    throw std::runtime_error{"error reading stemmer corpus " + path.string() +
                             ": " + error_message};
  }
}

bool LoadExternalCorpus(Corpus& corpus, const Language& lang) {
  const char* root = std::getenv("SERENEDB_STEMMER_CORPUS_ROOT");
  if (!root || !*root) {
    return false;
  }
  const auto directory = std::filesystem::path{root} / lang.name;
  const auto plain = directory / "voc.txt";
  const auto gzip = directory / "voc.txt.gz";
  const auto limit = ExternalCorpusLimit();
  if (std::filesystem::exists(plain)) {
    LoadPlainCorpus(corpus, plain, limit);
    corpus.source = plain.string();
  } else if (std::filesystem::exists(gzip)) {
    LoadGzipCorpus(corpus, gzip, limit);
    corpus.source = gzip.string();
  } else {
    throw std::runtime_error{
      "external stemmer corpus is missing for " + std::string{lang.name} +
      "; expected " + plain.string() + " or " + gzip.string()};
  }
  corpus.external = true;
  return true;
}

const Corpus& GetCorpus(const Language& lang) {
  static std::map<std::string_view, std::unique_ptr<Corpus>> cache;
  const auto it = cache.find(lang.name);
  if (it != cache.end()) {
    return *it->second;
  }

  auto corpus = std::make_unique<Corpus>();
  if (!LoadExternalCorpus(*corpus, lang)) {
    for (const auto word : lang.words) {
      corpus->pool.emplace_back(word);
    }
    corpus->source = "built-in";
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

#ifdef SERENEDB_STEMMER_GENERATED_C_CANDIDATE
using CStemmerCreate = SN_env* (*)();
using CStemmerRun = int (*)(SN_env*);

template<CStemmerCreate Create, CStemmerRun Stem>
class GeneratedCStemmer {
 public:
  GeneratedCStemmer() : env_{Create()} {
    if (!env_) {
      throw std::runtime_error{"failed to create generated C stemmer"};
    }
  }

  ~GeneratedCStemmer() { SN_delete_env(env_); }

  GeneratedCStemmer(const GeneratedCStemmer&) = delete;
  GeneratedCStemmer& operator=(const GeneratedCStemmer&) = delete;

  std::string_view stem(std::string_view input) {
    if (SN_set_current(env_, static_cast<int>(input.size()),
                       reinterpret_cast<const symbol*>(input.data())) < 0 ||
        Stem(env_) < 0) {
      throw std::runtime_error{"generated C stemmer failed"};
    }
    return {reinterpret_cast<const char*>(env_->p),
            static_cast<size_t>(env_->l)};
  }

 private:
  SN_env* env_;
};

template<CStemmerCreate Create, CStemmerRun Stem>
void BmGeneratedCStem(benchmark::State& state, const Language* lang,
                      Bucket bucket) {
  const auto& corpus = GetCorpus(*lang);
  const auto words = Select(corpus, bucket);
  if (words.empty()) {
    state.SkipWithError("empty corpus bucket");
    return;
  }

  GeneratedCStemmer<Create, Stem> stemmer;
  for (auto _ : state) {
    for (const auto& word : words) {
      const auto stem = stemmer.stem(
        {word.GetData(), static_cast<size_t>(word.GetSize())});
      benchmark::DoNotOptimize(stem);
    }
  }
  Account(state, words);
}

template<CStemmerCreate Create, CStemmerRun Stem>
bool RegisterGeneratedC(const Language& lang) {
  const auto& corpus = GetCorpus(lang);
  const auto reference = irs::make_stemmer_ptr(lang.algorithm, nullptr);
  if (!reference) {
    throw std::runtime_error{"missing reference stemmer for " +
                             std::string{lang.name}};
  }

  GeneratedCStemmer<Create, Stem> candidate;
  for (const auto& word : corpus.all) {
    const std::string_view input{word.GetData(),
                                 static_cast<size_t>(word.GetSize())};
    const auto expected = StemUncached(reference.get(), input);
    const auto actual = candidate.stem(input);
    if (!expected || *expected != actual) {
      std::fprintf(stderr,
                   "generated C candidate disabled for %.*s: mismatch on %.*s "
                   "(reference: %.*s, candidate: %.*s)\n",
                   static_cast<int>(lang.name.size()), lang.name.data(),
                   static_cast<int>(input.size()), input.data(),
                   expected ? static_cast<int>(expected->size()) : 7,
                   expected ? expected->data() : "<error>",
                   static_cast<int>(actual.size()), actual.data());
      return false;
    }
  }

  benchmark::RegisterBenchmark(
    "StemGeneratedC/" + std::string{lang.name} + "/All",
    BmGeneratedCStem<Create, Stem>, &lang, Bucket::All);
  return true;
}
#endif

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
  if (const char* root = std::getenv("SERENEDB_STEMMER_CORPUS_ROOT")) {
    std::printf(
      "external corpus root: %s (limit: %zu words per language; 0 means "
      "unlimited)\n\n",
      root, ExternalCorpusLimit());
  }
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

#ifdef SERENEDB_STEMMER_GENERATED_C_CANDIDATE
  RegisterGeneratedC<candidate_english_UTF_8_create_env,
                     candidate_english_UTF_8_stem>(kLanguages[0]);
  RegisterGeneratedC<candidate_german_UTF_8_create_env,
                     candidate_german_UTF_8_stem>(kLanguages[1]);
  RegisterGeneratedC<candidate_french_UTF_8_create_env,
                     candidate_french_UTF_8_stem>(kLanguages[2]);
  RegisterGeneratedC<candidate_spanish_UTF_8_create_env,
                     candidate_spanish_UTF_8_stem>(kLanguages[3]);
  RegisterGeneratedC<candidate_italian_UTF_8_create_env,
                     candidate_italian_UTF_8_stem>(kLanguages[4]);
  RegisterGeneratedC<candidate_finnish_UTF_8_create_env,
                     candidate_finnish_UTF_8_stem>(kLanguages[5]);
  RegisterGeneratedC<candidate_hungarian_UTF_8_create_env,
                     candidate_hungarian_UTF_8_stem>(kLanguages[6]);
  RegisterGeneratedC<candidate_turkish_UTF_8_create_env,
                     candidate_turkish_UTF_8_stem>(kLanguages[7]);
  RegisterGeneratedC<candidate_russian_UTF_8_create_env,
                     candidate_russian_UTF_8_stem>(kLanguages[8]);
  RegisterGeneratedC<candidate_greek_UTF_8_create_env,
                     candidate_greek_UTF_8_stem>(kLanguages[9]);
  RegisterGeneratedC<candidate_arabic_UTF_8_create_env,
                     candidate_arabic_UTF_8_stem>(kLanguages[10]);
  RegisterGeneratedC<candidate_polish_UTF_8_create_env,
                     candidate_polish_UTF_8_stem>(kLanguages[11]);
  RegisterGeneratedC<candidate_hindi_UTF_8_create_env,
                     candidate_hindi_UTF_8_stem>(kLanguages[12]);
  RegisterGeneratedC<candidate_tamil_UTF_8_create_env,
                     candidate_tamil_UTF_8_stem>(kLanguages[13]);
#endif

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
