#include <simdjson.h>

#include <iostream>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/timer_utils.hpp>
#include <string>

using namespace irs::analysis;

namespace {

static constexpr std::string_view kScorer = "bm25";
static constexpr std::string_view kScorerOptions = R"({})";
static constexpr std::string_view kTokenizer = "segmentation";
static constexpr std::string_view kTokenizerOptions = R"({})";

static constexpr std::string_view kIndexDir = "idx";
static constexpr std::string_view kFormatName = "1_5simd";
static constexpr size_t kSegmentMemMax = 1 << 28;  // 256M
static constexpr size_t kBatchSize = 100000;
static constexpr size_t kIndexerThreads = 10;
static constexpr size_t kCommitIntervalMs = 0;
static constexpr size_t kConsolidationIntervalMsec = 5000;
static constexpr size_t kConsolidationThreads = 0;
static constexpr bool kConsolidateAll = true;

constexpr auto kTextIndexFeatures =
  irs::IndexFeatures::Freq | irs::IndexFeatures::Pos | irs::IndexFeatures::Norm;

struct TextField {
  std::string_view name;
  std::string_view text;
  Analyzer::ptr tokenizer{irs::analysis::analyzers::Get(
    kTokenizer, irs::Type<irs::text_format::Json>::get(), kTokenizerOptions)};

  std::string_view Name() const noexcept { return name; }

  irs::Tokenizer& GetTokens() const {
    tokenizer->reset(text);
    return *tokenizer;
  }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return kTextIndexFeatures;
  }
};

struct Document {
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document json_doc;
  std::array<TextField, 2> fields{
    TextField{.name = "id"},
    TextField{.name = "text"},
  };

  void Fill(std::string_view line) {
    simdjson::padded_string padded_input{line};
    const auto error = parser.iterate(padded_input).get(json_doc);
    SDB_ASSERT(error == simdjson::SUCCESS, "Failed to parse JSON document",
               line);

    fields[0].text = json_doc["id"];
    fields[1].text = json_doc["text"];
  }
};

}  // namespace

int main(int argc, const char* argv[]) {
  irs::timer_utils::InitStats(true);
  irs::Finally output_stats = [] noexcept {
    std::vector<std::tuple<std::string, size_t, size_t>> output;
    irs::timer_utils::Visit(
      [&](const std::string& key, size_t count, size_t time_us) -> bool {
        if (count == 0) {
          return true;
        }
        output.emplace_back(key, count, time_us);
        return true;
      });
    std::sort(output.begin(), output.end());
    for (auto& [key, count, time_us] : output) {
      std::cout << key << " calls:" << count << ", time: " << time_us
                << " us, avg call: "
                << static_cast<double>(time_us) / static_cast<double>(count)
                << " us" << std::endl;
    }
  };

  SCOPED_TIMER("Total indexing time");

  irs::analysis::analyzers::Init();
  irs::formats::Init();
  irs::scorers::Init();
  irs::compression::Init();

  irs::MMapDirectory dir{kIndexDir};
  auto format = irs::formats::Get(kFormatName);
  auto scorer = irs::scorers::Get(
    kScorer, irs::Type<irs::text_format::Json>::get(), kScorerOptions);
  auto scorer_ptr = scorer.get();

  irs::IndexWriterOptions opts;
  opts.reader_options.scorers = {&scorer_ptr, 1};
  opts.segment_pool_size = kIndexerThreads;
  opts.segment_memory_max = kSegmentMemMax;
  opts.features = [](irs::IndexFeatures id) {
    const irs::ColumnInfo info{
      irs::Type<irs::compression::None>::get(), {}, false};

    if (irs::IndexFeatures::Norm == id) {
      return std::make_pair(info, &irs::Norm::MakeWriter);
    }

    return std::make_pair(info, irs::FeatureWriterFactory{});
  };

  auto writer = irs::IndexWriter::Make(dir, format, irs::kOmCreate, opts);

  irs::async_utils::ThreadPool<> thread_pool{kIndexerThreads +
                                             kConsolidationThreads + 1};

  struct {
    absl::CondVar cond;
    std::atomic<bool> done{false};
    bool eof{false};
    absl::Mutex mutex;
    std::vector<std::string> buf;

    bool Swap(std::vector<std::string>& buf) {
      absl::MutexLock lock{&mutex};
      for (;;) {
        this->buf.swap(buf);
        this->buf.resize(0);
        cond.notify_all();

        if (!buf.empty()) {
          return true;
        }

        if (eof) {
          done.store(true);
          return false;
        }

        if (!eof) {
          cond.Wait(&mutex);
        }
      }
    }
  } batch_provider;

  // stream reader thread
  thread_pool.run([&batch_provider] {
    irs::SetThreadName(IR_NATIVE_STRING("reader"));

    absl::MutexLock lock{&batch_provider.mutex};

    for (;;) {
      batch_provider.buf.resize(batch_provider.buf.size() + 1);
      batch_provider.cond.notify_all();

      auto& line = batch_provider.buf.back();

      if (std::getline(std::cin, line).eof()) {
        batch_provider.buf.pop_back();
        break;
      }

      if (kBatchSize && batch_provider.buf.size() >= kBatchSize) {
        batch_provider.cond.Wait(&batch_provider.mutex);
      }
    }

    batch_provider.eof = true;
  });

  absl::Mutex consolidation_mutex;
  absl::CondVar consolidation_cv;

  // commiter thread
  if (kCommitIntervalMs) {
    thread_pool.run(
      [&consolidation_cv, &consolidation_mutex, &batch_provider, &writer] {
        irs::SetThreadName(IR_NATIVE_STRING("committer"));

        while (!batch_provider.done.load()) {
          {
            std::cout << "[COMMIT]" << std::endl;
            writer->Commit();
          }

          // notify consolidation threads
          if (kConsolidationThreads) {
            absl::MutexLock lock{&consolidation_mutex};
            consolidation_cv.notify_all();
          }

          std::this_thread::sleep_for(
            std::chrono::milliseconds(kCommitIntervalMs));
        }
      });
  }

  // consolidation threads
  const irs::index_utils::ConsolidateTier consolidation_options;
  auto policy = irs::index_utils::MakePolicy(consolidation_options);

  for (size_t i = kConsolidationThreads; i; --i) {
    thread_pool.run([&] {
      irs::SetThreadName(IR_NATIVE_STRING("consolidater"));

      while (!batch_provider.done.load()) {
        {
          absl::MutexLock lock{&consolidation_mutex};
          if (consolidation_cv.WaitWithTimeout(
                &consolidation_mutex,
                absl::Milliseconds(kConsolidationIntervalMsec))) {
            continue;
          }
        }

        {
          std::cout << "[CONSOLIDATE]" << std::flush;
          writer->Consolidate(policy);
        }

        irs::directory_utils::RemoveAllUnreferenced(dir);
      }
    });
  }

  // indexer threads
  for (size_t i = kIndexerThreads; i; --i) {
    thread_pool.run([&] {
      irs::SetThreadName(IR_NATIVE_STRING("indexer"));

      std::vector<std::string> buf;
      Document doc;

      while (batch_provider.Swap(buf)) {
        auto ctx = writer->GetBatch();
        for (auto& line : buf) {
          doc.Fill(line);

          auto builder = ctx.Insert();
          for (auto& field : doc.fields) {
            builder.Insert<irs::Action::INDEX>(field);
          }
        }

        std::cout << "." << std::flush;
      }
    });
  }

  thread_pool.stop();

  std::cout << "[COMMIT]" << std::endl;
  writer->Commit();

  if (kConsolidateAll) {
    // merge all segments into a single segment
    std::cout << "Consolidating all segments:" << std::endl;
    writer->Consolidate(
      irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount()));
    writer->Commit();
  }

  if (kConsolidateAll || kConsolidationThreads) {
    irs::directory_utils::RemoveAllUnreferenced(dir);
  }

  irs::IndexReaderOptions options;
  auto reader = irs::DirectoryReader(dir, format, options);
  std::cout << "Number of documents: " << reader.docs_count() << std::endl;

  return 0;
}
