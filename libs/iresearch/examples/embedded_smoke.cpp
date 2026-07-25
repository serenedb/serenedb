#include <duckdb/main/database.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/doc_collector.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/store/memory_directory.hpp"

namespace {

constexpr irs::field_id kTextField = 1;

struct TextField {
  std::string_view text;
  irs::analysis::Analyzer::ptr tokenizer{
    irs::analysis::DelimitedTokenizer::Make({.delimiter = " "})};

  irs::field_id Id() const noexcept { return kTextField; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  }

  irs::Tokenizer& GetTokens() const {
    tokenizer->reset(text);
    return *tokenizer;
  }
};

bool CheckBm25() {
  duckdb::DuckDB db;
  irs::MemoryDirectory dir;
  irs::IndexWriterOptions options;
  options.db = db.instance.get();
  options.reader_options.db = db.instance.get();

  auto format = irs::formats::Get("1_5simd");
  if (!format) {
    return false;
  }

  auto writer = irs::IndexWriter::Make(dir, format, irs::kOmCreate, options);
  TextField field;
  auto batch = writer->GetBatch();
  constexpr std::array documents{
    std::string_view{"transcript search search search"},
    std::string_view{"transcript search"},
    std::string_view{"unrelated notes"},
  };

  for (const auto text : documents) {
    field.text = text;
    auto doc = batch.Insert();
    if (!doc.Insert(&field)) {
      return false;
    }
  }

  if (!batch.Commit() || !writer->RefreshCommit()) {
    return false;
  }

  irs::ByTerm filter;
  *filter.mutable_field_id() = kTextField;
  filter.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"search"});

  auto scorer = irs::BM25::Make({.b = 0.f});
  constexpr size_t kTopK = 2;
  std::vector<irs::ScoreDoc> results(irs::BlockSize(kTopK));
  const auto count = irs::ExecuteTopKWithCount(
    writer->GetSnapshot(), filter, *scorer, kTopK, std::span{results});

  return count == 2 && results[0].doc == irs::doc_limits::min() &&
         results[0].score > results[1].score;
}

bool CheckIvf() {
  constexpr uint32_t kDimensions = 4;
  constexpr size_t kClusters = 16;
  constexpr size_t kPerCluster = 3;
  std::vector<float> data;
  data.reserve(kClusters * kPerCluster * kDimensions);

  for (size_t cluster = 0; cluster < kClusters; ++cluster) {
    for (size_t point = 0; point < kPerCluster; ++point) {
      for (uint32_t dim = 0; dim < kDimensions; ++dim) {
        const auto bit = static_cast<float>((cluster >> dim) & 1);
        data.push_back(bit * 100.f + static_cast<float>(point) * 0.01f);
      }
    }
  }

  auto builder = irs::CentroidsBuilder::CreateFromSample(
    data, kDimensions, irs::VectorMetric::L2Sqr,
    {.posting_size = 4, .max_fanout = 4});
  if (builder.NumClusters() <= 1) {
    return false;
  }

  auto reordered = data;
  const auto assigned =
    builder.AssignCentroids({reordered.data(), reordered.size()}, kDimensions);
  auto expected = std::numeric_limits<size_t>::max();
  for (size_t i = 0; i < assigned.perm.size(); ++i) {
    if (assigned.perm[i] == 0) {
      expected = assigned.ids[i];
      break;
    }
  }
  if (expected == std::numeric_limits<size_t>::max()) {
    return false;
  }

  irs::MemoryFile file{irs::IResourceManager::gNoop};
  irs::CentroidsSpan serialized;
  {
    irs::MemoryIndexOutput out{file};
    serialized = builder.Serialize(out);
    out.Flush();
  }

  irs::MemoryIndexInput in{file};
  in.Seek(serialized.offset);
  auto tree = irs::CentroidsTree::Deserialize(in, serialized.byte_size);
  std::vector<uint32_t> clusters;
  tree.Search(std::span<const float>{data.data(), kDimensions}, in, 2, clusters,
              nullptr);

  return std::find(clusters.begin(), clusters.end(), expected) !=
         clusters.end();
}

}

int main() {
  try {
    irs::formats::Init();
    irs::InitOptimizeRules();

    const bool bm25 = CheckBm25();
    const bool ivf = CheckIvf();
    std::cout << "bm25=" << (bm25 ? "pass" : "fail")
              << " ivf=" << (ivf ? "pass" : "fail") << '\n';
    return bm25 && ivf ? 0 : 1;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
