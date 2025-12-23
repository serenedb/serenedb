////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <basics/buffer.h>

#include "basics/common.h"
#include "basics/encoding_utils.h"
#include "basics/errors.h"
#include "basics/string_buffer.h"
#include "gtest/gtest.h"
#include "vpack/common.h"

using namespace sdb;

namespace {
constexpr char kShortString[] =
  "this is a text that is going to be compressed in various ways";
constexpr char kMediumString[] =
  "ジャパン は、イギリスのニュー・ウェーヴバンド。デヴィッド・ ... "
  "を構築していった。 "
  "日本では初来日でいきなり武道館での公演を行うなど、爆発的な人気を誇ったが、"
  "英国ではなかなか人気が出ず、初期は典型的な「ビッグ・イン・ジャパン」状態で"
  "あった。日本最大級のポータルサイト。検索、オークション、ニュース、メール、"
  "コミュニティ、ショッピング、など80以上のサービスを展開。あなたの生活をより"
  "豊かにする「ライフ・エンジン」を目指していきます。デヴィッド・シルヴィアン"
  "とその弟スティーヴ・ジャンセン、デヴィッドの親友であったミック・カーンを中"
  "心に結成。ミック・カーンの兄の結婚式にバンドとして最初のお披露目をした。当"
  "初はミック・カーンをリードボーカルとして練習していたが、本番直前になって怖"
  "じ気づいたミックがデヴィッド・シルヴィアンに無理矢理頼み込んでボーカルを代"
  "わってもらい、以降デヴィッドがリードボーカルとなった。その後高校の同級であ"
  "ったリチャード・バルビエリを誘い、更にオーディションでロブ・ディーンを迎え"
  "入れ、デビュー当初のバンドの形態となった。デビュー当初はアイドルとして宣伝"
  "されたグループだったが、英国の音楽シーンではほとんど人気が無かった。初期の"
  "サウンドは主に黒人音楽やグラムロックをポスト・パンク的に再解釈したものであ"
  "ったが、作品を重ねるごとに耽美的な作風、退廃的な歌詞やシンセサイザーの利用"
  "など独自のスタイルを構築していった。日本では初来日でいきなり武道館での公演"
  "を行うなど、爆発的な人気を誇ったが、英国ではなかなか人気が出ず、初期は典型"
  "的な「ビッグ・イン・ジャパン」状態であった。";
}  // namespace

TEST(EncodingUtilsTest, testVPackBufferZlibInflateDeflate) {
  vpack::BufferUInt8 buffer;

  // test with an empty input
  {
    vpack::BufferUInt8 deflated;
    EXPECT_EQ(sdb::ERROR_OK,
              encoding::ZLibDeflate(buffer.data(), buffer.size(), deflated));
    EXPECT_EQ(0, deflated.size());
    EXPECT_EQ(9095565465875628745ULL,
              VPACK_HASH(deflated.data(), deflated.size(), 0xdeadbeef));
  }

  // test with a short string first
  {
    buffer.append(::kShortString, strlen(::kShortString));

    EXPECT_EQ(61, buffer.size());
    EXPECT_EQ(8692035728977180514ULL,
              VPACK_HASH(buffer.data(), buffer.size(), 0xdeadbeef));

    // deflate the string
    vpack::BufferUInt8 deflated;
    EXPECT_EQ(sdb::ERROR_OK,
              encoding::ZLibDeflate(buffer.data(), buffer.size(), deflated));

    EXPECT_EQ(61, deflated.size());
    EXPECT_EQ(11939602800036708587ULL,
              VPACK_HASH(deflated.data(), deflated.size(), 0xdeadbeef));

    // now inflate it. we should be back at the original size & content
    vpack::BufferUInt8 inflated;
    EXPECT_EQ(ERROR_OK, encoding::ZLibInflate(deflated.data(), deflated.size(),
                                              inflated));

    EXPECT_EQ(61, inflated.size());
    EXPECT_EQ(8692035728977180514ULL,
              VPACK_HASH(inflated.data(), inflated.size(), 0xdeadbeef));
  }

  // now try a longer string
  buffer.clear();
  {
    buffer.append(::kMediumString, strlen(::kMediumString));

    EXPECT_EQ(2073, buffer.size());
    EXPECT_EQ(7801811617998092707ULL,
              VPACK_HASH(buffer.data(), buffer.size(), 0xdeadbeef));

    // deflate the string
    vpack::BufferUInt8 deflated;
    EXPECT_EQ(sdb::ERROR_OK,
              encoding::ZLibDeflate(buffer.data(), buffer.size(), deflated));

    EXPECT_EQ(907, deflated.size());
    EXPECT_EQ(11264577993052485727ULL,
              VPACK_HASH(deflated.data(), deflated.size(), 0xdeadbeef));

    // now inflate it. we should be back at the original size & content
    vpack::BufferUInt8 inflated;
    EXPECT_EQ(ERROR_OK, encoding::ZLibInflate(deflated.data(), deflated.size(),
                                              inflated));

    EXPECT_EQ(2073, inflated.size());
    EXPECT_EQ(7801811617998092707ULL,
              VPACK_HASH(inflated.data(), inflated.size(), 0xdeadbeef));
  }

  // now with a 1 MB string
  buffer.clear();
  {
    for (size_t i = 0; i < 1024 * 1024; ++i) {
      buffer.push_back(char(i));
    }
    EXPECT_EQ(1024 * 1024, buffer.size());
    EXPECT_EQ(8549693651586153351ULL,
              VPACK_HASH(buffer.data(), buffer.size(), 0xdeadbeef));

    // deflate the string
    vpack::BufferUInt8 deflated;
    EXPECT_EQ(sdb::ERROR_OK,
              encoding::ZLibDeflate(buffer.data(), buffer.size(), deflated));

    EXPECT_EQ(4396, deflated.size());
    EXPECT_EQ(16555912008391160024ULL,
              VPACK_HASH(deflated.data(), deflated.size(), 0xdeadbeef));

    // now inflate it. we should be back at the original size & content
    vpack::BufferUInt8 inflated;
    EXPECT_EQ(ERROR_OK, encoding::ZLibInflate(deflated.data(), deflated.size(),
                                              inflated));

    EXPECT_EQ(1024 * 1024, inflated.size());
    EXPECT_EQ(8549693651586153351ULL,
              VPACK_HASH(inflated.data(), inflated.size(), 0xdeadbeef));
  }

  // test deflating with empty input
  buffer.clear();
  {
    vpack::BufferUInt8 deflated;
    EXPECT_EQ(sdb::ERROR_OK,
              encoding::ZLibDeflate(buffer.data(), buffer.size(), deflated));
    EXPECT_EQ(0, deflated.size());
    EXPECT_EQ(9095565465875628745ULL,
              VPACK_HASH(deflated.data(), deflated.size(), 0xdeadbeef));
  }

  // test inflating with broken input
  buffer.clear();
  {
    buffer.append("this-is-broken-deflated-content");

    vpack::BufferUInt8 inflated;
    EXPECT_EQ(sdb::ERROR_INTERNAL,
              encoding::ZLibInflate(buffer.data(), buffer.size(), inflated));
    EXPECT_EQ(0, inflated.size());
    EXPECT_EQ(9095565465875628745ULL,
              VPACK_HASH(inflated.data(), inflated.size(), 0xdeadbeef));
  }
}
