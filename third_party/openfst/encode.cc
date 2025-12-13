// Copyright 2005-2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.
//
// Definitions for encode table header.

#include <fst/encode.h>

#include <cstddef>
#include <cstdint>
#include <istream>
#include <ostream>
#include <string>

#include <fst/log.h>
#include <fst/util.h>
#include <string_view>

namespace fst {

bool EncodeTableHeader::Read(std::istream &strm, std::string_view source) {
  int32_t magic_number;
  ReadType(strm, &magic_number);
  switch (magic_number) {
    case internal::kEncodeMagicNumber: {
      ReadType(strm, &arctype_);
      ReadType(strm, &flags_);
      ReadType(strm, &size_);
      break;
    }
    case internal::kEncodeDeprecatedMagicNumber: {
      LOG(ERROR) << "This old-style Encoder is written in a deprecated "
                      "format and will soon cease to be readable. Please read "
                      "and re-write it in order to be future-proof.";
      // TODO(b/141172858): deprecated, remove by 2020-01-01.
      uint32_t flags;
      ReadType(strm, &flags);
      flags_ = flags;
      int64_t size;
      ReadType(strm, &size);
      size_ = size;
      break;
    }
    default: {
      LOG(ERROR) << "EncodeTableHeader::Read: Bad encode table header: "
                 << source;
      return false;
    }
  }
  if (!strm) {
    LOG(ERROR) << "EncodeTableHeader::Read: Read failed: " << source;
    return false;
  }
  return true;
}

bool EncodeTableHeader::Write(std::ostream &strm,
                              std::string_view source) const {
  WriteType(strm, internal::kEncodeMagicNumber);
  WriteType(strm, arctype_);
  WriteType(strm, flags_);
  WriteType(strm, size_);
  strm.flush();
  if (!strm) {
    LOG(ERROR) << "EncodeTableHeader::Write: Write failed: " << source;
    return false;
  }
  return true;
}

}  // namespace fst
