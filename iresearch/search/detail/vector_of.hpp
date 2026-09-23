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

#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <memory>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/walk.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/window.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/docs/walk.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/hits/make.hpp"
#include "iresearch/search/hits/walk.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/posting_docs.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/lead/two_phase_scored.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/two_phase_docs.hpp"
#include "iresearch/search/probe/two_phase_scored.hpp"
#include "iresearch/search/queries/vector_similarity_query.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/walk.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

class VectorBlockReader {
 public:
  VectorBlockReader(IndexInput::ptr&& in, uint32_t record_size) noexcept
    : _in{std::move(in)}, _record_size{record_size} {
    SDB_ASSERT(_in);
  }

  void Reset(uint64_t base_offset) noexcept { _base = base_offset; }

  std::span<const byte_type> Read(size_t index, size_t count) {
    const uint64_t offset = _base + static_cast<uint64_t>(index) * _record_size;
    const size_t bytes = count * size_t{_record_size};
    if (const byte_type* p = _in->ReadVolatile(offset, bytes)) {
      return {p, bytes};
    }
    _buf.resize(bytes);
    _in->ReadData(offset, _buf.data(), bytes);
    return _buf;
  }

 private:
  IndexInput::ptr _in;
  std::vector<byte_type> _buf;
  uint64_t _base = 0;
  uint32_t _record_size;
};

struct RawRecipe {
  const ColumnReader* column = nullptr;
  const ColReader* reader = nullptr;
  std::span<const float> query;
  uint32_t d = 0;
  VectorMetric metric = VectorMetric::L2Sqr;
};

class RawVectorReader {
 public:
  RawVectorReader(const ColumnReader& vector_column,
                  const ColReader& col_reader, uint32_t d)
    : _read_ctx{col_reader, true},
      _vreader{vector_column, _read_ctx},
      _column{&vector_column},
      _d{d} {}

  explicit RawVectorReader(const RawRecipe& recipe)
    : RawVectorReader{*recipe.column, *recipe.reader, recipe.d} {
    SetQuery(recipe.query, recipe.metric);
  }

  void SetQuery(std::span<const float> query, VectorMetric metric) {
    _query.assign(query.begin(), query.end());
    _dist = ResolveScoringDistance(metric);
  }

  void ComputeDistances(std::span<const doc_id_t> docs,
                        std::span<score_t> out) {
    SDB_ASSERT(_dist);
    SDB_ASSERT(out.size() >= docs.size());
    const auto* q = reinterpret_cast<const byte_type*>(_query.data());
    const auto d = static_cast<uint16_t>(_d);
    for (size_t i = 0; i < docs.size();) {
      size_t run = ConsecutiveRunLength(docs, i);
      // A run of one is the scattered case: the row after the next is far
      // enough ahead to be worth naming now.
      if (run == 1 && i + 2 < docs.size()) {
        PrefetchRow(docs[i + 2]);
      }
      while (run != 0) {
        size_t got = 0;
        const auto* base = ReadSome(docs[i], run, got);
        SDB_ASSERT(got != 0 && got <= run);
        // The rows are contiguous and each is several cache lines, so the
        // hardware prefetcher has to recognise the stream afresh every row it
        // strides over. Ninety-six threads streaming at once leave it little
        // room; naming the next row keeps the scan ahead of its own reads.
        const size_t stride = static_cast<size_t>(_d) * sizeof(float);
        constexpr size_t kAhead = 2;
        for (size_t k = 0; k < got; ++k) {
          if (k + kAhead < got) {
            const auto* ahead = base + (k + kAhead) * stride;
            for (size_t off = 0; off < stride; off += 64) {
              __builtin_prefetch(ahead + off, 0, 3);
            }
          }
          out[i + k] = _dist(q, base + k * stride, d);
        }
        i += got;
        run -= got;
      }
    }
  }

 private:
  // Name the lines of a row that is coming but not next, without disturbing
  // the window the scan is reading from. A selective predicate leaves the rows
  // scattered, so each one is its own miss; naming a later row now lets that
  // miss overlap the one being scored instead of following it.
  void PrefetchRow(doc_id_t doc) noexcept {
    const auto* child = _column->Child();
    const uint64_t elem =
      (static_cast<uint64_t>(doc) - doc_limits::min()) * _d;
    const auto w = child->Locate(elem, _win);
    const auto& blocks = child->DataBlocks();
    if (w.block >= blocks.size()) {
      return;
    }
    const auto& m = blocks[w.block];
    const uint64_t span = static_cast<uint64_t>(_d) * sizeof(float);
    if (m.codec->type != duckdb::CompressionType::COMPRESSION_UNCOMPRESSED ||
        elem < w.begin || elem + _d > w.end) {
      return;
    }
    const uint64_t off = m.file_offset + (elem - w.begin) * sizeof(float);
    const auto* q = _read_ctx.TryReadStable(off, span);
    if (q == nullptr) {
      return;
    }
    for (uint64_t o = 0; o < span; o += 64) {
      __builtin_prefetch(reinterpret_cast<const char*>(q) + o, 0, 3);
    }
  }

  // The rows of a run that one stable window can serve, and how many that was.
  //
  // A data block holds a few thousand floats, so a run of rows is spread over
  // several of them: asking for the whole run at once fails the in-place read
  // every time and falls back to copying the lot through the columnstore. On a
  // brute-force scan that copy was 17% of the server's CPU and doubled the
  // memory traffic -- the vectors were read once into a buffer and once again
  // to score. Stopping at the window edge keeps the pointer.
  const byte_type* ReadSome(doc_id_t first, size_t count, size_t& got) {
    const auto* child = _column->Child();
    SDB_ASSERT(child != nullptr);
    const uint64_t elem =
      (static_cast<uint64_t>(first) - doc_limits::min()) * _d;
    _win = child->Locate(elem, _win);
    const auto window = _win;  // by value: the walk below moves the hint
    const auto& blocks = child->DataBlocks();
    const auto& meta = blocks[window.block];
    if (meta.codec->type == duckdb::CompressionType::COMPRESSION_UNCOMPRESSED &&
        elem >= window.begin && elem < window.end) {
      // Blocks of an uncompressed column are written back to back, so the run
      // one read can serve reaches over every block that follows this one
      // contiguously in the file. Stopping at the first block edge instead
      // leaves any row sitting on a seam to the columnstore's scan path -- a
      // pin and an output vector to move one row -- and on a million-row scan
      // those seams were 11% of the server's CPU.
      const uint64_t want = elem + static_cast<uint64_t>(count) * _d;
      uint64_t end = window.end;
      for (auto w = window; end < want;) {
        const auto next = child->Locate(end, w);
        if (next.block == w.block || next.begin != end) {
          break;
        }
        const auto& m = blocks[next.block];
        if (m.codec->type !=
              duckdb::CompressionType::COMPRESSION_UNCOMPRESSED ||
            m.file_offset != meta.file_offset +
                               (next.begin - window.begin) * sizeof(float)) {
          break;
        }
        end = next.end;
        w = next;
        _win = next;  // the next call starts where this run ended
      }
      // Whole rows only: a block boundary that falls inside a row leaves none,
      // and that row goes the slow way so the next one can start clean.
      const auto fit = static_cast<size_t>((end - elem) / _d);
      if (fit != 0) {
        got = std::min(count, fit);
        const size_t bytes = got * _d * sizeof(float);
        const uint64_t offset =
          meta.file_offset + (elem - window.begin) * sizeof(float);
        if (const auto* p = _read_ctx.TryReadStable(offset, bytes)) {
          return reinterpret_cast<const byte_type*>(p);
        }
        _buf.resize(bytes);
        _read_ctx.Read(offset,
                       reinterpret_cast<duckdb::data_ptr_t>(_buf.data()),
                       bytes);
        return _buf.data();
      }
      got = 1;
      return reinterpret_cast<const byte_type*>(_vreader.ReadDocBatch(first, 1));
    }
    got = count;
    return reinterpret_cast<const byte_type*>(
      _vreader.ReadDocBatch(first, count));
  }

  const byte_type* Read(doc_id_t first, size_t count) {
    const auto* child = _column->Child();
    SDB_ASSERT(child != nullptr);
    const uint64_t elem =
      (static_cast<uint64_t>(first) - doc_limits::min()) * _d;
    // The rows a scan hands over ascend, so the block one lands in is almost
    // always the block the last one landed in. Locate takes the previous
    // window as a hint and answers from it instead of searching the block
    // list -- which a selective predicate would otherwise pay for once per
    // row, its runs being a single row each.
    _win = child->Locate(elem, _win);
    const auto& window = _win;
    const auto& meta = child->DataBlocks()[window.block];
    const size_t bytes = count * _d * sizeof(float);
    if (meta.codec->type == duckdb::CompressionType::COMPRESSION_UNCOMPRESSED &&
        elem + count * _d <= window.end) {
      const uint64_t offset =
        meta.file_offset + (elem - window.begin) * sizeof(float);
      if (const auto* p = _read_ctx.TryReadStable(offset, bytes)) {
        return reinterpret_cast<const byte_type*>(p);
      }
      _buf.resize(bytes);
      _read_ctx.Read(offset, reinterpret_cast<duckdb::data_ptr_t>(_buf.data()),
                     bytes);
      return _buf.data();
    }
    return reinterpret_cast<const byte_type*>(
      _vreader.ReadDocBatch(first, count));
  }

  ReadContext _read_ctx;
  irs::BlockWindow _win{};
  IvfVectorReader _vreader;
  const ColumnReader* _column;
  std::vector<byte_type> _buf;
  VectorDistanceFn _dist = nullptr;
  std::vector<float> _query;
  uint32_t _d;
};

struct AcceptAll {
  static constexpr bool kAll = true;

  static bool Inside(score_t, score_t) noexcept { return true; }
};

template<bool Inclusive>
struct RadiusGate {
  static constexpr bool kAll = false;

  static bool Inside(score_t distance, score_t edge) noexcept {
    bool res = distance > edge;
    if constexpr (Inclusive) {
      res |= distance == edge;
    }
    return res;
  }
};

template<typename InputType, typename Gate>
class VectorCluster {
 public:
  static constexpr uint32_t kRun = doc_limits::kBlockSize;

  VectorCluster(const PostingMeta& meta, const IndexInput& doc_in,
                std::unique_ptr<QuantizerReader>&& quantizer,
                VectorBlockReader&& payload, uint32_t lane)
    : _quantizer{std::move(quantizer)},
      _pay{std::move(payload)},
      _total{meta.docs_count},
      _lane{lane} {
    SDB_ASSERT(_quantizer);
    _setting = _quantizer->BlockSetting();
    SDB_ASSERT(_setting.group_size != 0);
    // ServeGroup decodes a whole group at once, so the cache is sized by the
    // format's group, not by the run. They are unrelated numbers: a run is at
    // most kRun docs, while a group is whatever the quantizer packs together
    // -- 32 lanes for fast scan, but 1024 for Panorama, which is eight times
    // kRun. Sizing this by kRun overflowed it by 896 floats, straight over the
    // members below, and the SDB_ASSERT that would have caught it is compiled
    // out of the build that ships.
    _cache.resize(_setting.group_size);
    SDB_ASSERT(_lane < std::max<uint32_t>(1, _setting.group_size));
    _end = _lane + _total;
    _records = static_cast<uint32_t>(_setting.RecordCount(_end));
    _list.Prepare(meta, doc_in, IndexFeatures::None, false);
  }

  VectorCluster(VectorCluster&&) = delete;
  VectorCluster& operator=(VectorCluster&&) = delete;

  void SetThreshold(score_t threshold) noexcept { _threshold = threshold; }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT window) {
    SDB_ASSERT(min < max);
    for (;;) {
      for (; _pos != _len; ++_pos) {
        const auto doc = _docs[_pos];
        if (doc >= max) {
          return doc;
        }
        if (doc < min) {
          continue;
        }
        const auto offset = doc - min;
        SetBit(mask[offset / detail::kWindowBits],
               offset % detail::kWindowBits);
        window[offset] = _dist[_pos];
      }
      if (!Refill()) {
        return doc_limits::eof();
      }
    }
  }

  bool NextRun() {
    if (!Refill()) {
      return false;
    }
    _pos = _len;
    return true;
  }

  std::span<const doc_id_t> RunDocs() const noexcept {
    return {_docs.data(), _len};
  }

  std::span<const score_t> RunScores() const noexcept {
    return {_dist.data(), _len};
  }

 private:
  bool Refill() {
    for (;;) {
      uint32_t len = 0;
      while (len != kRun) {
        const auto doc = _list.Next();
        if (doc_limits::eof(doc)) {
          break;
        }
        _docs[len++] = doc;
      }
      _pos = 0;
      _len = 0;
      if (len == 0) {
        return false;
      }
      ComputeRange(_base, len, _dist.data());
      _base += len;
      if constexpr (Gate::kAll) {
        _len = len;
      } else {
        _len = Keep(len);
      }
      if (_len != 0) {
        return true;
      }
    }
  }

  uint32_t Keep(uint32_t len) noexcept {
    uint32_t kept = 0;
    for (uint32_t i = 0; i != len; ++i) {
      const auto distance = _dist[i];
      _docs[kept] = _docs[i];
      _dist[kept] = distance;
      kept += static_cast<uint32_t>(Gate::Inside(distance, _threshold));
    }
    return kept;
  }

  uint32_t GroupRecords(uint32_t first) const noexcept {
    return std::min(first + _setting.group_size, _records) - first;
  }

  uint32_t ServeGroup(uint32_t lane, uint32_t len, score_t* out) {
    if (lane < _cached_first || lane >= _cached_end) {
      const uint32_t gs = _setting.group_size;
      const uint32_t first = lane / gs * gs;
      const uint32_t records = GroupRecords(first);
      _quantizer->ComputeBlock(_pay.Read(first, records), _threshold,
                               _cache.data());
      _cached_first = first;
      _cached_end = first + std::min<uint32_t>(records, _end - first);
    }
    const uint32_t take = std::min(len, _cached_end - lane);
    std::copy_n(_cache.begin() + (lane - _cached_first), take, out);
    return take;
  }

  void ComputeRange(uint32_t base, uint32_t len, score_t* out) {
    SDB_ASSERT(base + len <= _total);
    uint32_t lane = _lane + base;
    const uint32_t gs = _setting.group_size;
    if (lane % gs != 0) {
      const uint32_t take = ServeGroup(lane, len, out);
      lane += take;
      out += take;
      len -= take;
    }
    if (const uint32_t full = len / gs * gs; full != 0) {
      _quantizer->ComputeBlock(_pay.Read(lane, full), _threshold, out);
      lane += full;
      out += full;
      len -= full;
    }
    if (len == 0) {
      return;
    }
    if (const uint32_t records = GroupRecords(lane); records == len) {
      _quantizer->ComputeBlock(_pay.Read(lane, records), _threshold, out);
      return;
    }
    ServeGroup(lane, len, out);
  }

  std::unique_ptr<QuantizerReader> _quantizer;
  VectorBlockReader _pay;
  detail::PostingLead<InputType> _list;
  std::array<doc_id_t, kRun> _docs;
  std::array<score_t, kRun> _dist;
  /// One decoded group; sized from the quantizer's group_size, not kRun.
  std::vector<score_t> _cache;
  PayloadBlockSetting _setting;
  score_t _threshold = std::numeric_limits<score_t>::lowest();
  uint32_t _total;
  uint32_t _lane;
  uint32_t _end = 0;
  uint32_t _records = 0;
  uint32_t _cached_first = 0;
  uint32_t _cached_end = 0;
  uint32_t _base = 0;
  uint32_t _len = 0;
  uint32_t _pos = 0;
};

template<typename Cluster>
class VectorClusters {
 public:
  template<typename Args>
  VectorClusters(size_t count, Args&& args)
    : _clusters{count, std::piecewise_construct, std::forward<Args>(args)},
      _order{count, [](uint32_t& slot,
                       size_t i) noexcept { slot = static_cast<uint32_t>(i); }},
      _live{count} {}

  VectorClusters(VectorClusters&&) = delete;
  VectorClusters& operator=(VectorClusters&&) = delete;

  doc_id_t Next(doc_id_t doc) {
    const auto target = doc + 1;
    return target <= _doc ? _doc : From(target);
  }

  doc_id_t Seek(doc_id_t target) {
    return target <= _doc ? _doc : From(target);
  }

  score_t Distance() const noexcept { return _window[_doc - _min]; }

  void SetThreshold(score_t threshold) noexcept {
    for (auto& cluster : _clusters) {
      cluster.SetThreshold(threshold);
    }
  }

  size_t size() const noexcept { return _clusters.size(); }

  Cluster& operator[](size_t i) noexcept { return _clusters[i]; }

 private:
  doc_id_t From(doc_id_t target) {
    if (doc_limits::eof(target)) {
      return _doc = doc_limits::eof();
    }
    for (;;) {
      if (!_filled || target >= _min + detail::kWindowDocs) {
        if (_live == 0) {
          return _doc = doc_limits::eof();
        }
        Refill(target);
      }
      if (const auto found = Find(target - _min);
          found != detail::kWindowDocs) {
        return _doc = _min + found;
      }
      if (_live == 0 || !detail::NextWindow(_min, _next, target)) {
        return _doc = doc_limits::eof();
      }
    }
  }

  void Refill(doc_id_t target) {
    for (uint32_t w = 0; w != detail::kWindowWords; ++w) {
      auto word = std::exchange(_mask[w], uint64_t{0});
      const auto base = w * detail::kWindowBits;
      while (word != 0) {
        _window[base + static_cast<uint32_t>(std::countr_zero(word))] = 0;
        word = PopBit(word);
      }
    }
    _min = target;
    _filled = true;
    _next = doc_limits::eof();
    size_t live = 0;
    for (size_t i = 0; i != _live; ++i) {
      const auto slot = _order[i];
      const auto next = _clusters[slot].Fill(_min, _min + detail::kWindowDocs,
                                             _mask.data(), _window);
      if (doc_limits::eof(next)) {
        continue;
      }
      _order[live++] = slot;
      _next = std::min(_next, next);
    }
    _live = live;
  }

  doc_id_t Find(doc_id_t offset) const noexcept {
    auto word = offset / detail::kWindowBits;
    auto bits = _mask[word] & (~uint64_t{0} << (offset % detail::kWindowBits));
    for (;;) {
      if (bits != 0) {
        return static_cast<doc_id_t>(word * detail::kWindowBits +
                                     std::countr_zero(bits));
      }
      if (++word == detail::kWindowWords) {
        return detail::kWindowDocs;
      }
      bits = _mask[word];
    }
  }

  detail::Scratch _mask{};
  ABSL_CACHELINE_ALIGNED score_t _window[detail::kWindowDocs]{};
  containers::Fixed<Cluster> _clusters;
  containers::Fixed<uint32_t> _order;
  size_t _live;
  doc_id_t _min = 0;
  doc_id_t _next = doc_limits::eof();
  doc_id_t _doc = doc_limits::invalid();
  bool _filled = false;
};

template<typename Cluster, bool HasInner, bool Rescore>
class VectorSlots {
 public:
  template<typename Args>
  VectorSlots(size_t count, Args&& args, score_t edge, probe::Node::ptr&& inner,
              const RawRecipe& raw)
    : _clusters{count, std::forward<Args>(args)},
      _inner{std::move(inner)},
      _raw{raw} {
    _clusters.SetThreshold(edge);
  }

  VectorSlots(VectorSlots&&) = delete;
  VectorSlots& operator=(VectorSlots&&) = delete;

  doc_id_t Next(doc_id_t doc) { return _clusters.Next(doc); }

  doc_id_t Seek(doc_id_t target) { return _clusters.Seek(target); }

  doc_id_t Probe(doc_id_t target) { return _clusters.Seek(target); }

  bool Match(doc_id_t doc) {
    const auto distance = _clusters.Distance();
    if constexpr (HasInner) {
      if (_inner.Probe(doc) != doc) {
        return false;
      }
    }
    if constexpr (Rescore) {
      _raw.ComputeDistances({&doc, 1}, {&_distance, 1});
    } else {
      _distance = distance;
    }
    return true;
  }

  score_t Scale() const noexcept { return _distance; }

 private:
  VectorClusters<Cluster> _clusters;
  [[no_unique_address]] utils::Need<HasInner, probe::Erased> _inner;
  [[no_unique_address]] utils::Need<Rescore, RawVectorReader> _raw;
  score_t _distance = 0.f;
};

struct ClusterFeed {
  const VectorState* state;
  IndexInput* payload;
  bool has_centroids;

  using Args =
    std::tuple<const PostingMeta&, const IndexInput&,
               std::unique_ptr<QuantizerReader>, VectorBlockReader, uint32_t>;

  Args operator()(size_t c) const {
    auto quantizer = MakeQuantizerReader(state->codebook);
    SDB_ASSERT(quantizer);
    const float* centroid =
      has_centroids ? state->cluster_centroids.data() + c * state->d : nullptr;
    quantizer->StartCluster(centroid);
    VectorBlockReader pay{payload->Dup(),
                          quantizer->BlockSetting().record_size};
    pay.Reset(state->pay_starts[c]);
    return Args{state->cookies[c], *detail::DocOf(*state->reader),
                std::move(quantizer), std::move(pay), state->pay_lanes[c]};
  }
};

template<typename Query>
RawRecipe RecipeOf(const Query& query) {
  const auto& state = query.State();
  return {.column = state.vector_column,
          .reader = state.col_reader,
          .query = query.Query(),
          .d = state.vector_column != nullptr
                 ? static_cast<uint32_t>(state.vector_column->ArraySize())
                 : state.d,
          .metric = query.Metric()};
}

template<typename Gate, typename Query, typename Emit>
auto ResolveClusters(const Query& query, Emit&& emit) {
  const auto& state = query.State();
  SDB_ASSERT(state.reader != nullptr);
  SDB_ASSERT(state.payload != nullptr);
  SDB_ASSERT(detail::DocOf(*state.reader) != nullptr);
  SDB_ASSERT(!state.cookies.empty());

  const ClusterFeed feed{.state = &state,
                         .payload = state.payload.get(),
                         .has_centroids = state.cluster_centroids.size() ==
                                          state.cookies.size() * state.d};
  const auto count = state.cookies.size();

  return detail::ResolveInput(
    *detail::DocOf(*state.reader), [&]<typename InputType>() {
      return emit.template operator()<VectorCluster<InputType, Gate>>(count,
                                                                      feed);
    });
}

template<typename Gate, bool Rescore, typename Query, typename Emit>
auto ResolveVector(const Query& query, score_t edge, probe::Node::ptr inner,
                   Emit&& emit) {
  const auto recipe = RecipeOf(query);
  return ResolveClusters<Gate>(
    query, [&]<typename Cluster>(size_t count, const ClusterFeed& feed) {
      return ResolveBool(inner != nullptr, [&]<bool HasInner>() {
        using Slots = VectorSlots<Cluster, HasInner, Rescore>;
        return emit.template operator()<Slots>(count, feed, edge,
                                               std::move(inner), recipe);
      });
    });
}

template<template<typename> class Walk, typename Result, typename Gate,
         template<typename> class Two, typename Query, typename... Prefix>
Result MakeVectorDocs(const Query& query, score_t edge, probe::Node::ptr inner,
                      Prefix&&... prefix) {
  return ResolveVector<Gate, false>(
    query, edge, std::move(inner),
    [&]<typename Slots>(auto&&... args) -> Result {
      using Node = Two<Slots>;
      return memory::make_managed<Walk<Node>>(
        std::forward<Prefix>(prefix)..., std::forward<decltype(args)>(args)...);
    });
}

template<template<typename> class Walk, typename Result, typename Gate,
         bool Rescore, template<typename> class Two, typename Query,
         typename... Prefix>
Result MakeVectorScored(const Query& query, const TermReader& field,
                        const detail::ScoreArgs& score, score_t edge,
                        probe::Node::ptr inner, Prefix&&... prefix) {
  const auto& segment = query.Segment();
  return ResolveVector<Gate, Rescore>(
    query, edge, std::move(inner),
    [&]<typename Slots>(auto&&... args) -> Result {
      using Node = Two<Slots>;
      return memory::make_managed<Walk<Node>>(
        std::forward<Prefix>(prefix)..., segment, field, score,
        std::forward<decltype(args)>(args)...);
    });
}

template<typename Query>
probe::Node::ptr InnerProbe(const Query& query) {
  const auto* inner = query.Inner();
  if (inner == nullptr) {
    return {};
  }
  return inner->PlanProbe({}, query.State().estimation);
}

inline score_t Unbounded() noexcept {
  return std::numeric_limits<score_t>::lowest();
}

}  // namespace irs::detail
