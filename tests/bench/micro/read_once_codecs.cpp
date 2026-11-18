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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_split.h>
#include <benchmark/benchmark.h>
#include <lz4.h>
#include <lz4hc.h>
#include <simdjson.h>
#include <zstd.h>
#include <zxc.h>

#include <bit>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <limits>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "server/utils/file_utils.h"

namespace sdb::bench {
namespace {

constexpr std::string_view kBlobsFlag = "--blobs=";
constexpr std::string_view kRealTime = "/real_time";
constexpr size_t kFrameSizes[] = {size_t{1} << 20, size_t{4} << 20};
constexpr uint32_t kInvalidNode = std::numeric_limits<uint32_t>::max();

struct Codec {
  std::string name;
  size_t (*bound)(size_t raw);
  int64_t (*compress)(std::string_view in, char* out, size_t capacity,
                      int level);
  int64_t (*decompress)(std::string_view in, char* out, size_t raw, int level);
  int level = 0;
};

struct Input {
  std::string key;
  std::vector<std::string_view> frames;
  size_t raw_bytes = 0;
  uint32_t list_slots = 0;
};

size_t Lz4Bound(size_t raw) {
  return static_cast<size_t>(LZ4_compressBound(static_cast<int>(raw)));
}

int64_t Lz4Compress(std::string_view in, char* out, size_t capacity,
                    int level) {
  if (level == 0) {
    return LZ4_compress_default(in.data(), out, static_cast<int>(in.size()),
                                static_cast<int>(capacity));
  }
  return LZ4_compress_HC(in.data(), out, static_cast<int>(in.size()),
                         static_cast<int>(capacity), level);
}

int64_t Lz4Decompress(std::string_view in, char* out, size_t raw, int) {
  return LZ4_decompress_safe(in.data(), out, static_cast<int>(in.size()),
                             static_cast<int>(raw));
}

ZSTD_CCtx* ZstdCompressor() {
  static ZSTD_CCtx* gCctx = ZSTD_createCCtx();
  return gCctx;
}

ZSTD_DCtx* ZstdDecompressor() {
  static ZSTD_DCtx* gDctx = ZSTD_createDCtx();
  return gDctx;
}

size_t ZstdBound(size_t raw) { return ZSTD_compressBound(raw); }

int64_t ZstdCompress(std::string_view in, char* out, size_t capacity,
                     int level) {
  const auto size = ZSTD_compressCCtx(ZstdCompressor(), out, capacity,
                                      in.data(), in.size(), level);
  return ZSTD_isError(size) ? -1 : static_cast<int64_t>(size);
}

int64_t ZstdDecompress(std::string_view in, char* out, size_t raw, int) {
  const auto size =
    ZSTD_decompressDCtx(ZstdDecompressor(), out, raw, in.data(), in.size());
  return ZSTD_isError(size) ? -1 : static_cast<int64_t>(size);
}

size_t ZxcBound(size_t raw) { return zxc_compress_bound(raw); }

int64_t ZxcCompress(std::string_view in, char* out, size_t capacity,
                    int level) {
  return zxc_compress(in.data(), in.size(), out, capacity, level);
}

int64_t ZxcDecompress(std::string_view in, char* out, size_t raw, int) {
  return zxc_decompress(in.data(), in.size(), out, raw);
}

class BitWriter {
 public:
  explicit BitWriter(char* at) : _at{at} {}

  void Put(uint64_t value, uint32_t width) {
    _bits |= value << _filled;
    _filled += width;
    if (_filled >= 32) {
      std::memcpy(_at, &_bits, sizeof(uint32_t));
      _at += sizeof(uint32_t);
      _bits >>= 32;
      _filled -= 32;
    }
  }

  char* Finish() {
    const auto tail = (_filled + 7) / 8;
    std::memcpy(_at, &_bits, tail);
    return _at + tail;
  }

 private:
  char* _at;
  uint64_t _bits = 0;
  uint32_t _filled = 0;
};

class BitReader {
 public:
  BitReader(const char* at, const char* end) : _at{at}, _end{end} {}

  uint32_t Get(uint32_t width) {
    if (_available < width) {
      uint32_t word = 0;
      const auto take =
        std::min<size_t>(sizeof(uint32_t), static_cast<size_t>(_end - _at));
      std::memcpy(&word, _at, take);
      _at += take;
      _bits |= uint64_t{word} << _available;
      _available += 32;
    }
    const auto value =
      static_cast<uint32_t>(_bits & ((uint64_t{1} << width) - 1));
    _bits >>= width;
    _available -= width;
    return value;
  }

 private:
  const char* _at;
  const char* _end;
  uint64_t _bits = 0;
  uint32_t _available = 0;
};

uint32_t IdAt(std::string_view in, size_t i) {
  uint32_t id = 0;
  std::memcpy(&id, in.data() + i * sizeof(uint32_t), sizeof(id));
  return id;
}

void PutId(char* out, size_t i, uint32_t id) {
  std::memcpy(out + i * sizeof(uint32_t), &id, sizeof(id));
}

uint32_t MaxId(std::string_view in) {
  uint32_t max = 0;
  for (size_t i = 0; i < in.size() / sizeof(uint32_t); ++i) {
    if (const auto id = IdAt(in, i); id != kInvalidNode) {
      max = std::max(max, id);
    }
  }
  return max;
}

size_t NodeIdsBound(size_t raw) { return 1 + raw / 2 + raw + sizeof(uint64_t); }

struct Lists {
  size_t count = 0;
  uint32_t slots = 0;
};

Lists ListsOf(std::string_view in, int level) {
  const auto slots = static_cast<uint32_t>(level);
  const auto ids = in.size() / sizeof(uint32_t);
  if (slots == 0 || slots > 255 || ids % slots != 0) {
    return {};
  }
  return {.count = ids / slots, .slots = slots};
}

int64_t StripCompress(std::string_view in, char* out, size_t, int level) {
  const auto lists = ListsOf(in, level);
  if (lists.count == 0) {
    return -1;
  }
  const auto width =
    static_cast<uint32_t>(std::max(1, std::bit_width(MaxId(in))));
  out[0] = static_cast<char>(width);
  auto* counts = out + 1;
  BitWriter writer{counts + lists.count};
  for (size_t list = 0; list < lists.count; ++list) {
    uint32_t count = 0;
    for (uint32_t slot = 0; slot < lists.slots; ++slot) {
      const auto id = IdAt(in, list * lists.slots + slot);
      if (id == kInvalidNode) {
        continue;
      }
      if (count != slot) {
        return -1;
      }
      writer.Put(id, width);
      ++count;
    }
    counts[list] = static_cast<char>(count);
  }
  return writer.Finish() - out;
}

int64_t StripDecompress(std::string_view in, char* out, size_t raw, int level) {
  const auto slots = static_cast<uint32_t>(level);
  const auto lists = raw / sizeof(uint32_t) / slots;
  const auto width = static_cast<uint32_t>(in[0]);
  const auto* counts = reinterpret_cast<const uint8_t*>(in.data() + 1);
  BitReader reader{in.data() + 1 + lists, in.data() + in.size()};
  for (size_t list = 0; list < lists; ++list) {
    const auto first = list * slots;
    uint32_t slot = 0;
    for (; slot < counts[list]; ++slot) {
      PutId(out, first + slot, reader.Get(width));
    }
    for (; slot < slots; ++slot) {
      PutId(out, first + slot, kInvalidNode);
    }
  }
  return static_cast<int64_t>(raw);
}

std::vector<Codec> Codecs(uint32_t list_slots) {
  std::vector<Codec> codecs;
  codecs.push_back({"lz4", Lz4Bound, Lz4Compress, Lz4Decompress, 0});
  for (const int level : {1, 2, 3, 6, 9, 10, 12}) {
    codecs.push_back({absl::StrCat("lz4hc_", level), Lz4Bound, Lz4Compress,
                      Lz4Decompress, level});
  }
  for (const int level : {-1, 1, 2, 3}) {
    codecs.push_back({absl::StrCat("zstd_", level), ZstdBound, ZstdCompress,
                      ZstdDecompress, level});
  }
  for (int level = zxc_min_level(); level <= zxc_max_level(); ++level) {
    codecs.push_back({absl::StrCat("zxc_", level), ZxcBound, ZxcCompress,
                      ZxcDecompress, level});
  }
  if (list_slots != 0) {
    codecs.push_back({"strip", NodeIdsBound, StripCompress, StripDecompress,
                      static_cast<int>(list_slots)});
  }
  return codecs;
}

std::deque<std::string>& Storage() {
  static std::deque<std::string> gStorage;
  return gStorage;
}

std::deque<Input>& Inputs() {
  static std::deque<Input> gInputs;
  return gInputs;
}

std::vector<std::string_view> Split(std::span<const std::string_view> blobs,
                                    size_t frame) {
  std::vector<std::string_view> frames;
  for (const auto blob : blobs) {
    for (size_t at = 0; at < blob.size(); at += frame) {
      frames.push_back(blob.substr(at, frame));
    }
  }
  return frames;
}

uint32_t ListSlots(const std::filesystem::path& bin) {
  constexpr std::string_view kBase = "_graph_base";
  constexpr std::string_view kUpper = "_graph_upper";
  const auto stem = bin.stem().string();
  std::string_view prefix{stem};
  uint64_t per_list = 1;
  if (prefix.ends_with(kBase)) {
    prefix.remove_suffix(kBase.size());
  } else if (prefix.ends_with(kUpper)) {
    prefix.remove_suffix(kUpper.size());
    per_list = 2;
  } else {
    return 0;
  }
  const auto dir = bin.parent_path();
  const auto nodes =
    std::filesystem::file_size(dir / absl::StrCat(prefix, "_graph_levels.bin"));
  const auto base =
    std::filesystem::file_size(dir / absl::StrCat(prefix, kBase, ".bin"));
  return static_cast<uint32_t>(base / sizeof(uint32_t) / nodes / per_list);
}

void LoadRegion(const std::filesystem::path& bin) {
  auto index_path = bin;
  index_path.replace_extension(".jsonl");
  const auto& bytes =
    Storage().emplace_back(utils::file_utils::Slurp(bin.string()));
  const auto index = utils::file_utils::Slurp(index_path.string());
  std::vector<std::string_view> blobs;
  simdjson::ondemand::parser parser;
  for (const std::string_view line :
       absl::StrSplit(index, '\n', absl::SkipEmpty())) {
    const simdjson::padded_string padded{line};
    auto entry = parser.iterate(padded);
    const auto offset = entry["offset"].get_uint64().value();
    const auto size = entry["size"].get_uint64().value();
    blobs.push_back(std::string_view{bytes}.substr(offset, size));
  }
  const auto key =
    absl::StrCat(bin.parent_path().parent_path().filename().string(), "/",
                 bin.stem().string());
  size_t raw = 0;
  size_t largest = 0;
  for (const auto blob : blobs) {
    raw += blob.size();
    largest = std::max(largest, blob.size());
  }
  const auto slots = ListSlots(bin);
  Inputs().push_back({absl::StrCat(key, "/record"), blobs, raw, slots});
  for (const auto frame : kFrameSizes) {
    if (largest > frame) {
      Inputs().push_back({absl::StrCat(key, "/frame_", frame >> 20, "m"),
                          Split(blobs, frame), raw, slots});
    }
  }
  if (blobs.size() > 1) {
    Inputs().push_back(
      {absl::StrCat(key, "/whole"), {std::string_view{bytes}}, raw, slots});
  }
}

struct Compressed {
  std::string bytes;
  std::vector<std::string_view> frames;
};

bool Compress(const Input& input, const Codec& codec, std::string& buffer,
              Compressed* compressed) {
  size_t capacity = 0;
  for (const auto frame : input.frames) {
    capacity += codec.bound(frame.size());
  }
  buffer.resize(capacity);
  size_t used = 0;
  std::vector<size_t> sizes;
  for (const auto frame : input.frames) {
    const auto size = codec.compress(frame, buffer.data() + used,
                                     codec.bound(frame.size()), codec.level);
    if (size <= 0) {
      return false;
    }
    sizes.push_back(static_cast<size_t>(size));
    used += static_cast<size_t>(size);
  }
  if (compressed) {
    compressed->bytes.assign(buffer.data(), used);
    size_t at = 0;
    for (const auto size : sizes) {
      compressed->frames.push_back(
        std::string_view{compressed->bytes}.substr(at, size));
      at += size;
    }
  }
  return true;
}

void CompressBench(benchmark::State& state, const Input& input,
                   const Codec& codec) {
  std::string buffer;
  Compressed compressed;
  if (!Compress(input, codec, buffer, &compressed)) {
    state.SkipWithError("compression failed");
    return;
  }
  for (auto _ : state) {
    benchmark::DoNotOptimize(Compress(input, codec, buffer, nullptr));
  }
  state.SetBytesProcessed(
    static_cast<int64_t>(state.iterations() * input.raw_bytes));
  state.counters["ratio"] = static_cast<double>(input.raw_bytes) /
                            static_cast<double>(compressed.bytes.size());
  state.counters["compressed_bytes"] =
    static_cast<double>(compressed.bytes.size());
}

void DecompressBench(benchmark::State& state, const Input& input,
                     const Codec& codec) {
  std::string buffer;
  Compressed compressed;
  if (!Compress(input, codec, buffer, &compressed)) {
    state.SkipWithError("compression failed");
    return;
  }
  std::string out(input.raw_bytes, '\0');
  const auto decompress = [&] {
    size_t at = 0;
    for (size_t i = 0; i < input.frames.size(); ++i) {
      const auto raw = input.frames[i].size();
      if (codec.decompress(compressed.frames[i], out.data() + at, raw,
                           codec.level) != static_cast<int64_t>(raw)) {
        return false;
      }
      at += raw;
    }
    return true;
  };
  if (!decompress()) {
    state.SkipWithError("decompression failed");
    return;
  }
  size_t at = 0;
  for (const auto frame : input.frames) {
    if (std::memcmp(out.data() + at, frame.data(), frame.size()) != 0) {
      state.SkipWithError("round trip differs");
      return;
    }
    at += frame.size();
  }
  for (auto _ : state) {
    benchmark::DoNotOptimize(decompress());
  }
  state.SetBytesProcessed(
    static_cast<int64_t>(state.iterations() * input.raw_bytes));
}

struct Row {
  double ratio = 0;
  double compressed_bytes = 0;
  double compress_mibps = 0;
  double decompress_mibps = 0;
};

class RatioReporter final : public benchmark::ConsoleReporter {
 public:
  void ReportRuns(const std::vector<Run>& reports) final {
    ConsoleReporter::ReportRuns(reports);
    for (const auto& run : reports) {
      if (run.run_type == Run::RT_Aggregate ||
          run.skipped != benchmark::internal::NotSkipped) {
        continue;
      }
      auto name = run.benchmark_name();
      if (name.ends_with(kRealTime)) {
        name.resize(name.size() - kRealTime.size());
      }
      const auto first = name.find('/');
      const auto last = name.rfind('/');
      auto& row =
        _rows[name.substr(first + 1, last - first - 1)][name.substr(last + 1)];
      const auto mibps =
        run.counters.at("bytes_per_second") / (1024.0 * 1024.0);
      if (name.starts_with("compress/")) {
        row.ratio = run.counters.at("ratio");
        row.compressed_bytes = run.counters.at("compressed_bytes");
        row.compress_mibps = mibps;
      } else {
        row.decompress_mibps = mibps;
      }
    }
  }

  void Finalize() final {
    ConsoleReporter::Finalize();
    auto& out = GetOutputStream();
    for (const auto& [input, rows] : _rows) {
      std::vector<std::pair<std::string, Row>> ranked{rows.begin(), rows.end()};
      absl::c_sort(ranked, [](const auto& a, const auto& b) {
        return a.second.ratio > b.second.ratio;
      });
      out << "\n" << input << ", best ratio first\n";
      out << absl::StrFormat("  %-10s %8s %14s %16s %18s\n", "codec", "ratio",
                             "compressed", "compress MiB/s",
                             "decompress MiB/s");
      for (const auto& [codec, row] : ranked) {
        out << absl::StrFormat("  %-10s %7.3fx %14.0f %16.1f %18.1f\n", codec,
                               row.ratio, row.compressed_bytes,
                               row.compress_mibps, row.decompress_mibps);
      }
    }
    out.flush();
  }

 private:
  std::map<std::string, std::map<std::string, Row>> _rows;
};

int Main(int argc, char** argv) {
  std::vector<char*> args;
  for (int i = 0; i < argc; ++i) {
    const std::string_view arg{argv[i]};
    if (!arg.starts_with(kBlobsFlag)) {
      args.push_back(argv[i]);
      continue;
    }
    const std::filesystem::path dir{arg.substr(kBlobsFlag.size())};
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
      if (entry.path().extension() == ".bin") {
        LoadRegion(entry.path());
      }
    }
  }
  if (Inputs().empty()) {
    absl::FPrintF(stderr,
                  "usage: serenedb-bench-micro read_once_codecs "
                  "--blobs=<measure.py blobs dir> [--blobs=...] "
                  "[benchmark flags]\n");
    return 1;
  }
  for (const auto& input : Inputs()) {
    for (const auto& codec : Codecs(input.list_slots)) {
      benchmark::RegisterBenchmark(
        absl::StrCat("compress/", input.key, "/", codec.name), CompressBench,
        std::cref(input), codec)
        ->UseRealTime()
        ->Unit(benchmark::kMillisecond);
      benchmark::RegisterBenchmark(
        absl::StrCat("decompress/", input.key, "/", codec.name),
        DecompressBench, std::cref(input), codec)
        ->UseRealTime()
        ->Unit(benchmark::kMillisecond);
    }
  }
  int size = static_cast<int>(args.size());
  benchmark::Initialize(&size, args.data());
  RatioReporter reporter;
  benchmark::RunSpecifiedBenchmarks(&reporter);
  benchmark::Shutdown();
  return 0;
}

[[maybe_unused]] const bool kMain = AddMain(SDB_BENCH_MODULE, &Main);

}  // namespace
}  // namespace sdb::bench
