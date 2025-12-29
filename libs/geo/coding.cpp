#include "geo/coding.h"
#include "geo/shape_container.h"
#include "geo_json.h"
#include "basics/result.h"

#include <s2/s2latlng.h>
#include <s2/s2point.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>

#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"


namespace sdb::geo {

namespace {

constexpr auto kMax = static_cast<double>(uint64_t{1} << 32);
constexpr auto kMax2 = static_cast<double>(uint64_t{1} << 31);
constexpr auto kMaxPi = kMax / M_PI;
constexpr auto kMax2Pi = kMax2 / M_PI;
constexpr auto kPiMax = M_PI / kMax;
constexpr auto k2PiMax = M_PI / kMax2;

template<bool EncodeBack>
void TranslateLatLngToU32Impl(std::conditional_t<EncodeBack, S2LatLng&, const S2LatLng&> lat_lng, uint32_t& lat_encoded,
                          uint32_t& lng_encoded) noexcept {
  SDB_ASSERT(lat_lng.is_valid());

  const auto lat = (lat_lng.lat().radians() + M_PI_2) * kMaxPi;
  SDB_ASSERT(lat <= kMax, lat);
  lat_encoded = static_cast<uint32_t>(lat + 0.5);
  const auto lat_decoded = static_cast<double>(lat_encoded) * kPiMax - M_PI_2;

  const auto lng = (lat_lng.lng().radians() + M_PI) * kMax2Pi;
  SDB_ASSERT(lng <= kMax, lng);
  lng_encoded = static_cast<uint32_t>(lng + 0.5);
  const auto lng_decoded = static_cast<double>(lng_encoded) * k2PiMax - M_PI;

  if constexpr (EncodeBack) {
    lat_lng = S2LatLng::FromRadians(lat_decoded, lng_decoded);
    SDB_ASSERT(lat_lng.is_valid());
  }
}

void GetLatLngAsU32(const S2LatLng& lat_lng, uint32_t& lat_encoded,
                          uint32_t& lng_encoded) noexcept {
  TranslateLatLngToU32Impl<false>(lat_lng, lat_encoded, lng_encoded);
}

void EncodeLatLngU32(Encoder& encoder, const S2LatLng& lat_lng) noexcept {
  uint32_t lat_encoded, lng_encoded;
  GetLatLngAsU32(lat_lng, lat_encoded, lng_encoded);

  SDB_ASSERT(encoder.avail() >= 2 * sizeof(uint32_t));
  encoder.put32(lat_encoded);
  encoder.put32(lng_encoded);
}

void EncodeLatLngF64(Encoder& encoder, const S2LatLng& lat_lng) noexcept {
  SDB_ASSERT(lat_lng.is_valid());

  SDB_ASSERT(encoder.avail() >= 2 * sizeof(double));
  encoder.putdouble(lat_lng.lat().radians());
  encoder.putdouble(lat_lng.lng().radians());
}

void PointFromLatLng(double lat, double lng, S2Point& point) noexcept {
  const auto lat_lng = S2LatLng::FromRadians(lat, lng);
  SDB_ASSERT(lat_lng.is_valid());
  point = lat_lng.ToPoint();
  SDB_ASSERT(S2::IsUnitLength(point));
}

void DecodeFromLatLngU32(Decoder& decoder, S2Point& point) noexcept {
  SDB_ASSERT(decoder.avail() >= 2 * sizeof(uint32_t));
  const auto lat_encoded = decoder.get32();
  const auto lng_encoded = decoder.get32();

  const auto lat_decoded = static_cast<double>(lat_encoded) * kPiMax - M_PI_2;
  const auto lng_decoded = static_cast<double>(lng_encoded) * k2PiMax - M_PI;

  PointFromLatLng(lat_decoded, lng_decoded, point);
}

void DecodeFromLatLngF64(Decoder& decoder, S2Point& point) noexcept {
  SDB_ASSERT(decoder.avail() >= 2 * sizeof(double));
  const auto lat = decoder.getdouble();
  const auto lng = decoder.getdouble();

  PointFromLatLng(lat, lng, point);
}

void DecodeFromPoint(Decoder& decoder, S2Point& point) {
  SDB_ASSERT(decoder.avail() >= 3 * sizeof(double));
  point[0] = decoder.getdouble();
  point[1] = decoder.getdouble();
  point[2] = decoder.getdouble();
  SDB_ASSERT(S2::IsUnitLength(point));
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
    auto parse_point = [&] {
      S2LatLng lat_lng;
      r = geo::json::ParsePoint(vpack, lat_lng);
      if (r.ok() && encoder != nullptr) {
        SDB_ASSERT(options != geo::coding::Options::Invalid);
        SDB_ASSERT(encoder->avail() >= sizeof(uint8_t));
        // We store type, because ParseCoordinates store it
        encoder->put8(0);  // In store to column we will remove it
        if (geo::coding::IsOptionsS2(options)) {
          auto point = lat_lng.ToPoint();
          geo::EncodePoint(*encoder, point);
          return point;
        } else {
          geo::EncodeLatLng(*encoder, lat_lng, options);
        }
      } else if (r.ok() && options == geo::coding::Options::S2LatLngU32) {
        geo::ToLatLngU32(lat_lng);
      }
      return lat_lng.ToPoint();
    };
    if (r.ok()) {
      region.reset(parse_point(), options);
    }
  } else {
    r = geo::json::ParseRegion<P != Parsing::FromIndex>(vpack, region, cache,
                                                        options, encoder);
  }
  if constexpr (P != Parsing::FromIndex) {
    if (r.fail()) {
      return false;
    }
  }
  return true;
}

using namespace coding;

void CheckEndian() noexcept {
  if constexpr (std::endian::native == std::endian::big) {
    SDB_FATAL("xxxxx", Logger::FIXME,
              "geo coding is not implemented for big-endian architectures");
  }
}

void ToLatLngU32(S2LatLng& lat_lng) noexcept {
  uint32_t lat_encoded, lng_encoded;
  TranslateLatLngToU32Impl<true>(lat_lng, lat_encoded, lng_encoded);
}

void EncodeLatLng(Encoder& encoder, S2LatLng& lat_lng,
                  Options options) noexcept {
  if (options == Options::S2LatLngU32) {
    EncodeLatLngU32(encoder, lat_lng);
  } else {
    SDB_ASSERT(options == Options::S2LatLngF64);
    EncodeLatLngF64(encoder, lat_lng);
  }
}

void EncodePoint(Encoder& encoder, const S2Point& point) noexcept {
  static_assert(sizeof(S2Point) == 3 * sizeof(double));
  SDB_ASSERT(encoder.avail() >= sizeof(S2Point));
  SDB_ASSERT(S2::IsUnitLength(point));
  encoder.putn(&point, sizeof(S2Point));
}

void ToLatLngU32(std::span<S2LatLng> vertices) noexcept {
  for (auto& lat_lng : vertices) {
    ToLatLngU32(lat_lng);
  }
}

void EncodeVertices(Encoder& encoder, std::span<const S2Point> vertices) {
  encoder.Ensure(vertices.size() * sizeof(S2Point));
  for (const auto& point : vertices) {
    EncodePoint(encoder, point);
  }
}

void EncodeVertices(Encoder& encoder, std::span<S2LatLng> vertices,
                    Options options) {
  SDB_ASSERT(!IsOptionsS2(options));
  auto encode = [&](auto&& encode_point, size_t sizeof_point) {
    encoder.Ensure(vertices.size() * sizeof_point);
    for (auto& lat_lng : vertices) {
      encode_point(encoder, lat_lng);
    }
  };
  if (options == Options::S2LatLngU32) {
    encode(EncodeLatLngU32, 2 * sizeof(uint32_t));
  } else {
    SDB_ASSERT(options == Options::S2LatLngF64);
    encode(EncodeLatLngF64, 2 * sizeof(double));
  }
}

bool DecodeVertices(Decoder& decoder, std::span<S2Point> vertices,
                    uint8_t tag) {
  auto decode = [&](auto&& decode_point, size_t sizeof_point) {
    if (decoder.avail() < vertices.size() * sizeof_point) {
      return false;
    }
    for (auto& vertex : vertices) {
      decode_point(decoder, vertex);
    }
    return true;
  };
  switch (ToPoint(tag)) {
    case std::to_underlying(Options::S2LatLngU32):
      return decode(DecodeFromLatLngU32, 2 * sizeof(uint32_t));
    case std::to_underlying(Options::S2LatLngF64):
      return decode(DecodeFromLatLngF64, 2 * sizeof(double));
    case std::to_underlying(Options::S2Point):
      return decode(DecodeFromPoint, 3 * sizeof(double));
    default:
      return false;
  }
}

bool DecodePoint(Decoder& decoder, S2Point& point, uint8_t* tag) {
  CheckEndian();
  if (decoder.avail() == 2 * sizeof(uint32_t)) {
    *tag = ToTag<Type::Point, Options::S2LatLngU32>();
    DecodeFromLatLngU32(decoder, point);
  } else if (decoder.avail() == 2 * sizeof(double)) {
    *tag = ToTag<Type::Point, Options::S2LatLngF64>();
    DecodeFromLatLngF64(decoder, point);
  } else if (decoder.avail() == 3 * sizeof(double)) {
    *tag = ToTag<Type::Point, Options::S2Point>();
    DecodeFromPoint(decoder, point);
  } else {
    return false;
  }
  return true;
}

bool DecodePoint(Decoder& decoder, S2Point& point,
                 [[maybe_unused]] uint8_t tag) {
  // We can use if format will change to detect point type
  // without relying on decoder.avail()
  [[maybe_unused]] uint8_t from = 0xFF;
  const bool r = DecodePoint(decoder, point, &from);
  SDB_ASSERT(tag == from);
  return r;
}

void EncodePolyline(Encoder& encoder, const S2Polyline& polyline,
                    Options options) {
  SDB_ASSERT(IsOptionsS2(options));
  SDB_ASSERT(options != Options::S2PointShapeCompact ||
               options != Options::S2PointRegionCompact,
             "All vertices should be serialized at once.");

  SDB_ASSERT(encoder.avail() >= sizeof(uint8_t) + Varint::kMax64);
  encoder.put8(ToTag(Type::Polyline, options));
  const auto vertices = polyline.vertices_span();
  encoder.put_varint64(vertices.size());
  EncodeVertices(encoder, vertices);
}

bool DecodePolyline(Decoder& decoder, S2Polyline& polyline, uint8_t tag,
                    std::vector<S2Point>& cache) {
  uint64_t size = 0;
  if (!decoder.get_varint64(&size)) {
    return false;
  }
  cache.resize(size);
  if (!DecodeVertices(decoder, cache, tag)) {
    return false;
  }
  polyline.Init(cache);
  return true;
}

/// num_loops <= 1:
///   varint(loop[0]_num_vertices << 1 | 0)
///   loop[0]_vertices_data
/// else:
///   varint(num_loops << 1 | 1)
///     varint(loop[i]_num_vertices)
///     loop[i]_vertices_data

void EncodePolygon(Encoder& encoder, const S2Polygon& polygon,
                   Options options) {
  SDB_ASSERT(IsOptionsS2(options));
  SDB_ASSERT(options != Options::S2PointRegionCompact ||
               options != Options::S2PointShapeCompact,
             "All vertices should be serialized at once.");

  SDB_ASSERT(encoder.avail() >= sizeof(uint8_t) + Varint::kMax64);
  encoder.put8(ToTag(Type::Polygon, options));
  switch (const auto num_loops = static_cast<uint64_t>(polygon.num_loops())) {
    case 0: {
      encoder.put_varint64(0);
    } break;
    case 1: {
      const auto vertices = polygon.loop(0)->vertices_span();
      SDB_ASSERT(!vertices.empty());
      encoder.put_varint64(vertices.size() * 2);
      EncodeVertices(encoder, vertices);
    } break;
    default: {
      encoder.Ensure((1 + num_loops) * Varint::kMax64 +
                     static_cast<size_t>(polygon.num_vertices()) *
                       ToSize(options));
      encoder.put_varint64(num_loops * 2 + 1);
      for (uint64_t i = 0; i != num_loops; ++i) {
        auto vertices = polygon.loop(static_cast<int>(i))->vertices_span();
        encoder.put_varint64(vertices.size());
        EncodeVertices(encoder, vertices);
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
  } else if (size % 2 == 0) {
    cache.resize(static_cast<size_t>(size / 2));
    if (!DecodeVertices(decoder, cache, tag)) {
      return false;
    }
    polygon.Init(std::make_unique<S2Loop>(cache, S2Debug::DISABLE));
    return true;
  }
  const auto num_loops = static_cast<size_t>(size / 2);
  SDB_ASSERT(num_loops >= 2);
  std::vector<std::unique_ptr<S2Loop>> loops;
  loops.reserve(num_loops);
  for (uint64_t i = 0; i != num_loops; ++i) {
    if (!decoder.get_varint64(&size)) {
      return false;
    }
    cache.resize(size);
    if (!DecodeVertices(decoder, cache, tag)) {
      return false;
    }
    loops.push_back(std::make_unique<S2Loop>(cache, S2Debug::DISABLE));
  }
  polygon.InitNested(std::move(loops));
  return true;
}

void PointToVPack(vpack::Builder& builder, S2LatLng point) {
  SDB_ASSERT(point.is_valid());
  // false because with false it's smaller
  // in general we want only doubles, but format requires it should be array
  // so we generate most smaller vpack array
  builder.openArray(false);
  builder.add(point.lng().degrees());
  builder.add(point.lat().degrees());
  builder.close();
  SDB_ASSERT(builder.slice().isArray());
  SDB_ASSERT(builder.slice().head() == 0x02);
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
