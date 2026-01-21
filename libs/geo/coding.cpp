#include "geo/coding.h"

#include <absl/algorithm/container.h>
#include <s2/s2latlng.h>
#include <s2/s2point.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>

#include <ranges>

#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/result.h"
#include "geo/shape_container.h"
#include "geo_json.h"

namespace sdb::geo {

using namespace coding;

namespace {

static_assert(std::endian::native == std::endian::little);

constexpr auto kMaxVal = static_cast<double>(uint64_t{1} << 32);
constexpr auto kMaxVal2 = static_cast<double>(uint64_t{1} << 31);
constexpr auto kMaxValPi = kMaxVal / M_PI;
constexpr auto kMaxVal2Pi = kMaxVal2 / M_PI;
constexpr auto kPiMaxVal = M_PI / kMaxVal;
constexpr auto k2PiMaxVal = M_PI / kMaxVal2;

template<bool WriteBack>
void ConvertLatLngToU32(
  std::conditional_t<WriteBack, S2LatLng&, const S2LatLng&> ll,
  uint32_t& out_lat, uint32_t& out_lng) noexcept {
  SDB_ASSERT(ll.is_valid());

  const auto lat_val = (ll.lat().radians() + M_PI_2) * kMaxValPi;
  SDB_ASSERT(lat_val <= kMaxVal, lat_val);
  out_lat = static_cast<uint32_t>(lat_val + 0.5);

  const auto lng_val = (ll.lng().radians() + M_PI) * kMaxVal2Pi;
  SDB_ASSERT(lng_val <= kMaxVal, lng_val);
  out_lng = static_cast<uint32_t>(lng_val + 0.5);

  if constexpr (WriteBack) {
    const auto lat_restored = static_cast<double>(out_lat) * kPiMaxVal - M_PI_2;
    const auto lng_restored = static_cast<double>(out_lng) * k2PiMaxVal - M_PI;

    ll = S2LatLng::FromRadians(lat_restored, lng_restored);
    SDB_ASSERT(ll.is_valid());
  }
}

void WriteLatLngF64(Encoder& enc, const S2LatLng& ll) noexcept {
  SDB_ASSERT(ll.is_valid());

  SDB_ASSERT(enc.avail() >= 2 * sizeof(double));
  enc.putdouble(ll.lat().radians());
  enc.putdouble(ll.lng().radians());
}

void WriteLatLngU32(Encoder& enc, const S2LatLng& ll) noexcept {
  uint32_t lat_out, lng_out;
  ConvertLatLngToU32<false>(ll, lat_out, lng_out);

  SDB_ASSERT(enc.avail() >= 2 * sizeof(uint32_t));
  enc.put32(lat_out);
  enc.put32(lng_out);
}

void CreatePointFromLatLng(double lat_rad, double lng_rad,
                           S2Point& out) noexcept {
  const auto ll = S2LatLng::FromRadians(lat_rad, lng_rad);
  SDB_ASSERT(ll.is_valid());
  out = ll.ToPoint();
  SDB_ASSERT(S2::IsUnitLength(out));
}

void ReadLatLngU32ToPoint(Decoder& dec, S2Point& out) noexcept {
  SDB_ASSERT(dec.avail() >= 2 * sizeof(uint32_t));
  const auto lat_in = dec.get32();
  const auto lng_in = dec.get32();

  const auto lat_restored = static_cast<double>(lat_in) * kPiMaxVal - M_PI_2;
  const auto lng_restored = static_cast<double>(lng_in) * k2PiMaxVal - M_PI;

  CreatePointFromLatLng(lat_restored, lng_restored, out);
}

void ReadLatLngF64ToPoint(Decoder& dec, S2Point& out) noexcept {
  SDB_ASSERT(dec.avail() >= 2 * sizeof(double));
  const auto lat_rad = dec.getdouble();
  const auto lng_rad = dec.getdouble();

  CreatePointFromLatLng(lat_rad, lng_rad, out);
}

void ReadRawPoint(Decoder& dec, S2Point& out) {
  SDB_ASSERT(dec.avail() >= 3 * sizeof(double));
  out[0] = dec.getdouble();
  out[1] = dec.getdouble();
  out[2] = dec.getdouble();
  SDB_ASSERT(S2::IsUnitLength(out));
}

}  // namespace

template<Parsing P>
bool ParseShape(vpack::Slice vpack, ShapeContainer& region,
                std::vector<S2LatLng>& cache, coding::Options options,
                Encoder* encoder) {
  SDB_ASSERT(encoder == nullptr || encoder->length() == 0);
  Result r;
  if (vpack.isArray()) {
    r = geo::json::ParseCoordinates<P != Parsing::FromIndex>(vpack, region,
                                                             /*geoJson=*/true,
                                                             options, encoder);
  } else if constexpr (P == Parsing::OnlyPoint) {
    auto handle_point = [&] {
      S2LatLng ll;
      r = geo::json::ParsePoint(vpack, ll);
      if (r.ok() && encoder) {
        SDB_ASSERT(options != geo::coding::Options::Invalid);
        SDB_ASSERT(encoder->avail() >= sizeof(uint8_t));
        // to match what ParseCoordinates stores
        encoder->put8(0);
        if (geo::coding::IsOptionsS2(options)) {
          auto pt = ll.ToPoint();
          geo::EncodePoint(*encoder, pt);
          return pt;
        } else {
          geo::EncodeLatLng(*encoder, ll, options);
        }
      } else if (r.ok() && options == geo::coding::Options::S2LatLngU32) {
        geo::ToLatLngU32(ll);
      }
      return ll.ToPoint();
    };
    if (r.ok()) {
      region.reset(handle_point(), options);
    }
  } else {
    r = geo::json::ParseRegion<P != Parsing::FromIndex>(vpack, region, cache,
                                                        options, encoder);
  }
  if constexpr (P != Parsing::FromIndex) {
    if (!r.ok()) {
      return false;
    }
  }
  return true;
}

void ToLatLngU32(S2LatLng& ll) noexcept {
  uint32_t lat_out, lng_out;
  ConvertLatLngToU32<true>(ll, lat_out, lng_out);
}

void EncodeLatLng(Encoder& enc, S2LatLng& ll, Options opts) noexcept {
  if (opts == Options::S2LatLngU32) {
    WriteLatLngU32(enc, ll);
  } else {
    SDB_ASSERT(opts == Options::S2LatLngF64);
    WriteLatLngF64(enc, ll);
  }
}

void EncodePoint(Encoder& enc, const S2Point& pt) noexcept {
  static_assert(sizeof(S2Point) == 3 * sizeof(double));
  SDB_ASSERT(enc.avail() >= sizeof(S2Point));
  SDB_ASSERT(S2::IsUnitLength(pt));
  enc.putn(&pt, sizeof(S2Point));
}

void ToLatLngU32(std::span<S2LatLng> verts) noexcept {
  absl::c_for_each(verts, [](auto& ll) { ToLatLngU32(ll); });
}

void EncodeVertices(Encoder& enc, std::span<const S2Point> verts) {
  enc.Ensure(verts.size() * sizeof(S2Point));
  absl::c_for_each(verts, [&](auto& p) { EncodePoint(enc, p); });
}

void EncodeVertices(Encoder& enc, std::span<S2LatLng> verts, Options opts) {
  SDB_ASSERT(!IsOptionsS2(opts));
  auto write = [&](auto&& write_fn, size_t elem_size) {
    enc.Ensure(verts.size() * elem_size);
    absl::c_for_each(verts, [&](auto& ll) { write_fn(enc, ll); });
  };
  if (opts == Options::S2LatLngU32) {
    write(WriteLatLngU32, 2 * sizeof(uint32_t));
  } else {
    SDB_ASSERT(opts == Options::S2LatLngF64);
    write(WriteLatLngF64, 2 * sizeof(double));
  }
}

bool DecodeVertices(Decoder& dec, std::span<S2Point> verts, uint8_t tag) {
  auto read = [&](auto&& read_fn, size_t elem_size) {
    if (dec.avail() < verts.size() * elem_size) {
      return false;
    }
    absl::c_for_each(verts, [&](auto& v) { read_fn(dec, v); });
    return true;
  };
  switch (ToPoint(tag)) {
    case std::to_underlying(Options::S2LatLngU32):
      return read(ReadLatLngU32ToPoint, 2 * sizeof(uint32_t));
    case std::to_underlying(Options::S2LatLngF64):
      return read(ReadLatLngF64ToPoint, 2 * sizeof(double));
    case std::to_underlying(Options::S2Point):
      return read(ReadRawPoint, 3 * sizeof(double));
    default:
      return false;
  }
}

std::pair<bool, uint8_t> DecodePoint(Decoder& dec, S2Point& pt) {
  if (dec.avail() == 2 * sizeof(uint32_t)) {
    ReadLatLngU32ToPoint(dec, pt);
    return {true, ToTag<Type::Point, Options::S2LatLngU32>()};
  } else if (dec.avail() == 2 * sizeof(double)) {
    ReadLatLngF64ToPoint(dec, pt);
    return {true, ToTag<Type::Point, Options::S2LatLngF64>()};
  } else if (dec.avail() == 3 * sizeof(double)) {
    ReadRawPoint(dec, pt);
    return {true, ToTag<Type::Point, Options::S2Point>()};
  } else {
    return {false, 0xFF};
  }
}

bool DecodePoint(Decoder& dec, S2Point& pt, [[maybe_unused]] uint8_t tag) {
  const auto [ok, detected] = DecodePoint(dec, pt);
  SDB_ASSERT(tag == detected);
  return ok;
}

void EncodePolyline(Encoder& enc, const S2Polyline& polyline, Options opts) {
  SDB_ASSERT(IsOptionsS2(opts));
  SDB_ASSERT(opts != Options::S2PointShapeCompact ||
             opts != Options::S2PointRegionCompact);

  SDB_ASSERT(enc.avail() >= sizeof(uint8_t) + Varint::kMax64);
  enc.put8(ToTag(Type::Polyline, opts));
  const auto verts = polyline.vertices_span();
  enc.put_varint64(verts.size());
  EncodeVertices(enc, verts);
}

bool DecodePolyline(Decoder& dec, S2Polyline& polyline, uint8_t tag,
                    std::vector<S2Point>& cache) {
  uint64_t cnt = 0;
  if (!dec.get_varint64(&cnt)) {
    return false;
  }
  cache.resize(cnt);
  if (!DecodeVertices(dec, cache, tag)) {
    return false;
  }
  polyline.Init(cache);
  return true;
}

void EncodePolygon(Encoder& enc, const S2Polygon& polygon, Options opts) {
  SDB_ASSERT(IsOptionsS2(opts));
  SDB_ASSERT(opts != Options::S2PointRegionCompact ||
             opts != Options::S2PointShapeCompact);

  SDB_ASSERT(enc.avail() >= Varint::kMax64 + sizeof(uint8_t));
  enc.put8(ToTag(Type::Polygon, opts));

  const auto loop_cnt = static_cast<size_t>(polygon.num_loops());
  switch (loop_cnt) {
    case 0: {
      enc.put_varint64(0);
    } break;
    case 1: {
      const auto verts = polygon.loop(0)->vertices_span();
      SDB_ASSERT(!verts.empty());
      enc.put_varint64(verts.size() << 1);
      EncodeVertices(enc, verts);
    } break;
    default: {
      const auto reserve =
        (loop_cnt + 1) * static_cast<size_t>(Varint::kMax64) +
        (static_cast<size_t>(polygon.num_vertices()) * ToSize(opts));
      enc.Ensure(reserve);
      enc.put_varint64((loop_cnt << 1) + 1);
      for (int i : std::views::iota(static_cast<int>(loop_cnt))) {
        auto verts = polygon.loop(i)->vertices_span();
        enc.put_varint64(verts.size());
        EncodeVertices(enc, verts);
      }
    } break;
  }
}

bool DecodePolygon(Decoder& decoder, S2Polygon& polygon, uint8_t tag,
                   std::vector<S2Point>& cache) {
  uint64_t size = 0;
  if (!decoder.get_varint64(&size)) {
    return false;
  }

  if (size == 0) {
    S2Polygon empty;
    empty.set_s2debug_override(S2Debug::DISABLE);
    polygon = std::move(empty);
    return true;
  }

  if (size % 2 == 0) {
    cache.resize(size >> 1);
    if (!DecodeVertices(decoder, cache, tag)) {
      return false;
    }
    polygon.Init(std::make_unique<S2Loop>(cache, S2Debug::DISABLE));
    return true;
  }

  std::vector<std::unique_ptr<S2Loop>> loops(size >> 1);
  SDB_ASSERT(loops.size() >= 2);

  for (auto& loop : loops) {
    if (!decoder.get_varint64(&size)) {
      return false;
    }
    cache.resize(size);
    if (!DecodeVertices(decoder, cache, tag)) {
      return false;
    }
    loop = std::make_unique<S2Loop>(cache, S2Debug::DISABLE);
  }
  polygon.InitNested(std::move(loops));
  return true;
}

void PointToVPack(vpack::Builder& builder, S2LatLng point) {
  SDB_ASSERT(point.is_valid());
  builder.openArray(false);
  builder.add(point.lng().degrees());
  builder.add(point.lat().degrees());
  builder.close();
  SDB_ASSERT(builder.slice().isArray());
  SDB_ASSERT(builder.slice().head() == 0x02);
}

Result GeoOptions::Validate() const noexcept {
  auto check_bounds = [&]<typename T>(auto name, auto min, auto max,
                                      const T& value) -> Result {
    static_assert(std::is_arithmetic_v<T>, "Check supports only numerics");
    if (value < min || max < value) {
      return {ERROR_BAD_PARAMETER,
              "'",
              name,
              "' out of bounds: [",
              min,
              "..",
              max,
              "]."};
    }
    return {};
  };

#define DO_CHECK_BOUNDS(field, min_val, max_val)       \
  res = check_bounds(#field, min_val, max_val, field); \
  if (!res.ok()) {                                     \
    return res;                                        \
  }

  constexpr int32_t kMinCells = 0;
  constexpr int32_t kMaxCells = std::numeric_limits<int32_t>::max();
  constexpr int32_t kMinLevel = 0;
  constexpr int32_t kMaxLevel = S2CellId::kMaxLevel;
  constexpr int32_t kMinLevelMod = 1;
  constexpr int32_t kMaxLevelMod = 3;

  Result res;
  DO_CHECK_BOUNDS(max_cells, kMinCells, kMaxCells)
  DO_CHECK_BOUNDS(min_level, kMinLevel, kMaxLevel)
  DO_CHECK_BOUNDS(max_level, kMinLevel, kMaxLevel)
  DO_CHECK_BOUNDS(level_mod, kMinLevelMod, kMaxLevelMod)
  if (min_level > max_level) {
    return {ERROR_BAD_PARAMETER,
            "'min_level' should be less than or equal to 'max_level'."};
  }
  return {};
}

template bool ParseShape<Parsing::FromIndex>(vpack::Slice slice,
                                             ShapeContainer& shape,
                                             std::vector<S2LatLng>& cache,
                                             coding::Options options,
                                             Encoder* encoder);
template bool ParseShape<Parsing::OnlyPoint>(vpack::Slice slice,
                                             ShapeContainer& shape,
                                             std::vector<S2LatLng>& cache,
                                             coding::Options options,
                                             Encoder* encoder);
template bool ParseShape<Parsing::GeoJson>(vpack::Slice slice,
                                           ShapeContainer& shape,
                                           std::vector<S2LatLng>& cache,
                                           coding::Options options,
                                           Encoder* encoder);

}  // namespace sdb::geo
