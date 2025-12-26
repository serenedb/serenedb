#include <absl/strings/str_split.h>

#include <iostream>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/text_format.hpp>
#include <magic_enum/magic_enum.hpp>
#include <string>

enum class QueryType {
  Count,
  UnoptimizedCount,
  Top10,
  Top100,
  Top1000,
  Top10Count,
  Top100Count,
  Top1000Count,
  Unsupported,
};

namespace magic_enum {

template<>
constexpr customize::customize_t customize::enum_name<QueryType>(
  QueryType value) noexcept {
  switch (value) {
    case QueryType::Count:
    case QueryType::UnoptimizedCount:
      return "COUNT";
    case QueryType::Top10:
      return "TOP_10";
    case QueryType::Top100:
      return "TOP_100";
    case QueryType::Top1000:
      return "TOP_1000";
    case QueryType::Top10Count:
      return "TOP_10_COUNT";
    case QueryType::Top100Count:
      return "TOP_100_COUNT";
    case QueryType::Top1000Count:
      return "TOP_1000_COUNT";
    default:
      return "UNSUPPORTED";
  }
}

}  // namespace magic_enum

namespace {

constexpr std::string_view kFormatName = "1_5simd";
constexpr std::string_view kScorer = "bm25";
constexpr std::string_view kScorerOptions = R"({})";
constexpr std::string_view kTokenizer = "segmentation";
constexpr std::string_view kTokenizerOptions = R"({})";

struct Query {
  QueryType type = QueryType::Unsupported;
  std::string_view query;
};

Query ParseQuery(std::string_view str) {
  auto parts = absl::StrSplit(str, '\t');
  auto begin = parts.begin();

  auto type = magic_enum::enum_cast<QueryType>(*begin);
  if (!type) {
    return {};
  }

  ++begin;
  return {*type, *begin};
}

void ExecuteTopK(size_t k, std::string_view query) {}

size_t ExecuteTopKCount(size_t k, std::string_view query) { return 0; }

size_t ExecuteCount(std::string_view query) { return 0; }

size_t ExecuteQuery(Query query) {
  switch (query.type) {
    case QueryType::Count:
      return ExecuteCount(query.query);
    case QueryType::Top10:
      ExecuteTopK(10, query.query);
      return 1;
    case QueryType::Top100:
      ExecuteTopK(100, query.query);
      return 1;
    case QueryType::Top1000:
      ExecuteTopK(1000, query.query);
      return 1;
    case QueryType::Top10Count:
      return ExecuteTopKCount(10, query.query);
    case QueryType::Top100Count:
      return ExecuteTopKCount(100, query.query);
    case QueryType::Top1000Count:
      return ExecuteTopKCount(1000, query.query);
    default:
      std::cout << magic_enum::enum_name(QueryType::Unsupported) << "\n";
      return 0;
  }
}

}  // namespace

int main(int argc, const char* argv[]) {
  irs::analysis::analyzers::Init();
  irs::formats::Init();
  irs::scorers::Init();
  irs::compression::Init();

  auto scorer = irs::scorers::Get(
    kScorer, irs::Type<irs::text_format::Json>::get(), kScorerOptions, false);
  SDB_ASSERT(scorer);
  auto* score = scorer.get();

  auto format = irs::formats::Get(kFormatName, false);
  SDB_ASSERT(format);

  auto tokenizer = irs::analysis::analyzers::Get(
    kTokenizer, irs::Type<irs::text_format::Json>::get(), kTokenizerOptions,
    false);
  SDB_ASSERT(tokenizer);

  const irs::WandContext wand = [&] -> irs::WandContext {
    return {.index = 0, .strict = true};
  }();

  irs::MMapDirectory dir{argv[1]};
  irs::IndexReaderOptions options;
  if (wand.Enabled()) {
    options.scorers = {&score, 1};
  }

  auto reader = irs::DirectoryReader(dir, format, options);
  irs::Scorers order;

  std::string data;
  while (std::getline(std::cin, data)) {
    std::cout << ExecuteQuery(ParseQuery(data)) << "\n";
  }
  return 0;
}
