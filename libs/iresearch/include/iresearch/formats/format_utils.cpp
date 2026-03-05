////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "format_utils.hpp"

#include "basics/shared.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/store_utils.hpp"

namespace irs {

void ValidateFooter(IndexInput& in) {
  const int64_t remain = in.Length() - in.Position();

  if (remain != format_utils::kFooterLen) {
    throw IndexError{absl::StrCat(
      "While validating footer, error: invalid position '", remain, "'")};
  }

  const int32_t magic = in.ReadI32();

  if (magic != format_utils::kFooterMagic) {
    throw IndexError{absl::StrCat(
      "While validating footer, error: invalid magic number '", magic, "'")};
  }

  const int32_t alg_id = in.ReadI32();

  if (alg_id != 0) {
    throw IndexError{absl::StrCat(
      "While validating footer, error: invalid algorithm '", alg_id, "'")};
  }
}

namespace format_utils {

// TODO(mbkkt) maybe ver is uint32_t
void WriteHeader(IndexOutput& out, std::string_view format, int32_t ver) {
  out.WriteU32(kFormatMagic);
  WriteStr(out, format);
  out.WriteU32(ver);
}

void WriteFooter(IndexOutput& out) {
  out.WriteU32(kFooterMagic);
  out.WriteU32(0);
  // TODO(mbkkt) checksum is uint32_t
  //  But maybe I should investigate more about not crc32c approaches
  out.WriteU64(out.Checksum());
}

size_t HeaderLength(std::string_view format) noexcept {
  return sizeof(int32_t) * 2 + bytes_io<uint64_t>::vsize(format.size()) +
         format.size();
}

int32_t CheckHeader(DataInput& in, std::string_view req_format, int32_t min_ver,
                    int32_t max_ver) {
  const ptrdiff_t left = in.Length() - in.Position();

  if (left < 0) {
    throw IllegalState{"Header has invalid length."};
  }

  const size_t expected = HeaderLength(req_format);

  if (static_cast<size_t>(left) < expected) {
    throw IndexError{absl::StrCat("While checking header, error: only '", left,
                                  "' bytes left out of '", expected, "'")};
  }

  const int32_t magic = in.ReadI32();

  if (kFormatMagic != magic) {
    throw IndexError{absl::StrCat(
      "While checking header, error: invalid magic '", magic, "'")};
  }

  const auto format = ReadString<std::string>(in);

  if (req_format != format) {
    throw IndexError{
      absl::StrCat("While checking header, error: format mismatch '", format,
                   "' != '", req_format, "'")};
  }

  const int32_t ver = in.ReadI32();

  if (ver < min_ver || ver > max_ver) {
    throw IndexError{absl::StrCat(
      "While checking header, error: invalid version '", ver, "'")};
  }

  return ver;
}

int64_t Checksum(const IndexInput& in) {
  auto* stream = &in;

  const auto length = stream->Length();

  if (length < sizeof(uint64_t)) {
    throw IndexError{
      absl::StrCat("failed to read checksum from a file of size ", length)};
  }

  IndexInput::ptr dup;
  if (0 != in.Position()) {
    dup = in.Dup();

    if (!dup) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "failed to duplicate input");

      throw IoError{"failed to duplicate input"};
    }

    dup->Seek(0);
    stream = dup.get();
  }

  SDB_ASSERT(0 == stream->Position());
  return stream->Checksum(length - sizeof(uint64_t));
}

}  // namespace format_utils
}  // namespace irs
