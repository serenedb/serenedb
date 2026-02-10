////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <basics/crc.hpp>

#include "iresearch/store/memory_directory.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/encryption.hpp"
#include "tests_param.hpp"
#include "tests_shared.hpp"

namespace {

using irs::bstring;

std::string ToString(irs::bytes_view bytes) {
  std::string s = "' ";
  for (auto b : bytes) {
    absl::StrAppend(&s, static_cast<int>(b), " ");
  }
  absl::StrAppend(&s, "'");
  return s;
}

void AssertEncryption(size_t block_size, size_t header_lenght) {
  tests::Rot13Encryption enc(block_size, header_lenght);

  bstring encrypted_header;
  encrypted_header.resize(enc.header_length());
  ASSERT_TRUE(enc.create_header("encrypted", &encrypted_header[0]));
  ASSERT_EQ(header_lenght, enc.header_length());

  bstring header = encrypted_header;
  auto cipher = enc.create_stream("encrypted", &header[0]);
  ASSERT_NE(nullptr, cipher);
  ASSERT_EQ(block_size, cipher->block_size());

  // unencrypted part of the header: counter+iv
  ASSERT_EQ(irs::bytes_view(encrypted_header.c_str(), 2 * cipher->block_size()),
            irs::bytes_view(header.c_str(), 2 * cipher->block_size()));

  // encrypted part of the header
  bool cond =
    encrypted_header.size() == 2 * cipher->block_size() ||
    (irs::bytes_view(encrypted_header.data() + 2 * cipher->block_size(),
                     encrypted_header.size() - 2 * cipher->block_size()) !=
     irs::bytes_view(header.data() + 2 * cipher->block_size(),
                     header.size() - 2 * cipher->block_size()));
  EXPECT_TRUE(cond) << "block_size: " << block_size
                    << ", header_lenght: " << header_lenght
                    << ", encrypted_header: " << ToString(encrypted_header)
                    << ", header: " << ToString(header);
  if (!cond) {
    return;
  }

  const bstring data(
    reinterpret_cast<const irs::byte_type*>(
      "4jLFtfXSuSdsGXbXqH8IpmPqx5n6IWjO9Pj8nZ0yD2ibKvZxPdRaX4lNsz8N"),
    30);

  // encrypt less than block size
  {
    bstring source(data.c_str(), 7);

    {
      size_t offset = 0;
      ASSERT_TRUE(cipher->Encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->Decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(irs::bytes_view(data.c_str(), source.size()),
                irs::bytes_view(source));
    }

    {
      size_t offset = 4;
      ASSERT_TRUE(cipher->Encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->Decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(irs::bytes_view(data.c_str(), source.size()),
                irs::bytes_view(source));
    }

    {
      size_t offset = 1023;
      ASSERT_TRUE(cipher->Encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->Decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(irs::bytes_view(data.c_str(), source.size()),
                irs::bytes_view(source));
    }
  }

  // encrypt size of the block
  {
    bstring source(data.c_str(), 13);

    {
      size_t offset = 0;
      ASSERT_TRUE(cipher->Encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->Decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(irs::bytes_view(data.c_str(), source.size()),
                irs::bytes_view(source));
    }

    {
      size_t offset = 4;
      ASSERT_TRUE(cipher->Encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->Decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(irs::bytes_view(data.c_str(), source.size()),
                irs::bytes_view(source));
    }

    {
      size_t offset = 1023;
      ASSERT_TRUE(cipher->Encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->Decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(irs::bytes_view(data.c_str(), source.size()),
                irs::bytes_view(source));
    }
  }

  // encrypt more than size of the block
  {
    bstring source = data;

    {
      size_t offset = 0;
      ASSERT_TRUE(cipher->Encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->Decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(irs::bytes_view(data.c_str(), source.size()),
                irs::bytes_view(source));
    }

    {
      size_t offset = 4;
      ASSERT_TRUE(cipher->Encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->Decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(irs::bytes_view(data.c_str(), source.size()),
                irs::bytes_view(source));
    }

    {
      size_t offset = 1023;
      ASSERT_TRUE(cipher->Encrypt(offset, &source[0], source.size()));
      ASSERT_TRUE(cipher->Decrypt(offset, &source[0], source.size()));
      ASSERT_EQ(irs::bytes_view(data.c_str(), source.size()),
                irs::bytes_view(source));
    }
  }
}

TEST(ctr_encryption_test, static_consts) {
  static_assert("encryption" == irs::Type<irs::Encryption>::name());
  static_assert(4096 == irs::CtrEncryption::kDefaultHeaderLength);
  static_assert(sizeof(uint64_t) == irs::CtrEncryption::kMinHeaderLength);
}

TEST(ctr_encryption_test, create_header_stream) {
  AssertEncryption(1, irs::CtrEncryption::kDefaultHeaderLength);
  AssertEncryption(13, irs::CtrEncryption::kDefaultHeaderLength);
  AssertEncryption(16, irs::CtrEncryption::kDefaultHeaderLength);
  AssertEncryption(1, sizeof(uint64_t));
  AssertEncryption(4, sizeof(uint64_t));
  AssertEncryption(8, 2 * sizeof(uint64_t));

  // block_size == 0
  {
    tests::Rot13Encryption enc(0);

    bstring encrypted_header;
    ASSERT_FALSE(enc.create_header("encrypted", &encrypted_header[0]));
    ASSERT_FALSE(enc.create_stream("encrypted", &encrypted_header[0]));
  }

  // header is too small (< MIN_HEADER_LENGTH)
  {
    tests::Rot13Encryption enc(1, 7);

    bstring encrypted_header;
    encrypted_header.resize(enc.header_length());
    ASSERT_FALSE(enc.create_header("encrypted", &encrypted_header[0]));
    ASSERT_FALSE(enc.create_stream("encrypted", &encrypted_header[0]));
  }

  // header is too small (< 2*block_size)
  {
    tests::Rot13Encryption enc(13, 25);

    bstring encrypted_header;
    encrypted_header.resize(enc.header_length());
    ASSERT_FALSE(enc.create_header("encrypted", &encrypted_header[0]));
    ASSERT_FALSE(enc.create_stream("encrypted", &encrypted_header[0]));
  }
}

class EncryptionTestCase : public tests::DirectoryTestCaseBase<> {
 protected:
  void AssertEcnryptedStreams(size_t block_size, size_t header_length,
                              size_t buf_size) {
    const std::vector<std::string> data{
      "spM42fEO88t2",        "jNIvCMksYwpoxN", "Re5eZWCkQexrZn",
      "jjj003oxVAIycv",      "N9IJuRjFSlO8Pa", "OPGG6Ic3JYJyVY",
      "ZDGVji8xtjh9zI",      "DvBDXbjKgIfPIk", "bZyCbyByXnGvlL",
      "vjGDbNcZGDmQ2",       "J7by8eYg0ZGbYw", "6UZ856mrVW9DeD",
      "Ny6bZIbGQ43LSU",      "oaYAsO0tXnNBkR", "Fr97cjyQoTs9Pf",
      "7rLKaQN4REFIgn",      "EcFMetqynwG87T", "oshFa26WK3gGSl",
      "8keZ9MLvnkec8Q",      "HuiOGpLtqn79GP", "Qnlj0JiQjBR3YW",
      "k64uvviemlfM8p",      "32X34QY6JaCH3L", "NcAU3Aqnn87LJW",
      "Q4LLFIBU9ci40O",      "M5xpjDYIfos22t", "Te9ZhWmGt2cTXD",
      "HYO3hJ1C4n1DvD",      "qVRj2SyXcKQz3Z", "vwt41rzEW7nkoi",
      "cLqv5U8b8kzT2H",      "tNyCoJEOm0POyC", "mLw6cl4HxmOHa",
      "2eTVXvllcGmZ0e",      "NFF9SneLv6pX8h", "kzCvqOVYlYA3QT",
      "mxCkaGg0GeLxYq",      "PffuwSr8h3acP0", "zDm0rAHgzhHsmv",
      "8LYMjImx00le9c",      "Ju0FM0mJmqkue1", "uNwn8A2SH4OSZW",
      "R1Dm21RTJzb0aS",      "sUpQGy1n6TiH82", "fhkCGcuQ5VnrEa",
      "b6Xsq05brtAr88",      "NXVkmxvLmhzFRY", "s9OuZyZX28uux0",
      "DQaD4HyDMGkbg3",      "Fr2L3V4UzCZZcJ", "7MgRPt0rLo6Cp4",
      "c8lK5hjmKUuc3e",      "jzmu3ZcP3PF62X", "pmUUUvAS00bPfa",
      "lonoswip3le6Hs",      "TQ1G0ruVvknb8A", "4XqPhpJv",
      "gY0QFCjckHp1JI",      "v2a0yfs9yN5lY4", "l1XKKtBXtktOs2",
      "AGotoLgRxPe4Pr",      "x9zPgBi3Bw8DFD", "OhX85k7OhY3FZM",
      "riRP6PRhkq0CUi",      "1ToW1HIephPBlz", "C8xo1SMWPZW8iE",
      "tBa3qiFG7c1wiD",      "BRXFbUYzw646PS", "mbR0ECXCash1rF",
      "AVDjHnwujjOGAK",      "16bmhl4gvDpj44", "OLa0D9RlpBLRgK",
      "PgCSXvlxyHQFlQ",      "sMyrmGRcVTwg53", "Fa6Fo687nt9bDV",
      "P0lUFttS64mC7s",      "rxTZUQIpOPYkPp", "oNEsVpak9SNgLh",
      "iHmFTSjGutROen",      "aTMmlghno9p91a", "tpb3rHs9ZWtL5m",
      "iG0xrYN7gXXPTs",      "KsEl2f8WtF6Ylv", "triXFZM9baNltC",
      "MBFTh22Yos3vGt",      "DTuFyue5f9Mk3x", "v2zm4kYxfar0J7",
      "xtpwVgOMT0eIFS",      "8Wz7MrtXkSH9CA", "FuURHWmPLb",
      "YpIFnExqjgpSh0",      "2oaIkTM6EJ",     "s16qvfbrycGnVP",
      "yUb2fcGIDRSujG",      "9rIfsuCyTCTiLY", "HXTg5jWrVZNLNP",
      "maLjUi6Oo6wsJr",      "C6iHChfoJHGxzO", "6LxzytT8iSzNHZ",
      "ex8znLIzbatFCo",      "HiYTSzZhBHgtaP", "H5EpiJw2L5UgD1",
      "ZhPvYoUMMFkoiL",      "y6014BfgqbE3ke", "XXutx8GrPYt7Rq",
      "DjYwLMixhS80an",      "aQxh91iigWOt4x", "1J9ZC2r7CCfGIH",
      "Sg9PzDCOb5Ezym",      "4PB3SukHVhA6SB", "BfVm1XGLDOhabZ",
      "ChEvexTp1CrLUL",      "M5nlO4VcxIOrxH", "YO9rnNNFwzRphV",
      "KzQhfZSnQQGhK9",      "r7Ez7Zwr0bn",    "fQipSie8ZKyT62",
      "3yyLqJMcShXG9z",      "UTb12lz3k5xPPt", "JjcWQnBnRFJ2Mv",
      "zsKEX7BLJQTjCx",      "g0oPvTcOhiev1k", "8P6HF4I6t1jwzu",
      "LaOiJIU47kagqu",      "pyY9sV9WQ5YuQC", "MCgpgJhEwrGKWM",
      "Hq5Wgc3Am8cjWw",      "FnITVHg0jw03Bm", "0Jq2YEnFf52861",
      "y0FT03yG9Uvg6I",      "S6uehKP8uj6wUe", "usC8CZobBmuk6",
      "LrZuchHNpSs282",      "PsmFFySt7fKFOv", "mXe9j6xNYttnSy",
      "al9J6AZYlhAlWU",      "3v8PsohUeKegJI", "QZCwr1URS1OWzX",
      "UVCg1mVWmSBWRT",      "pO2lnQ4L6yHQic", "w5EtZl2gZhj2ca",
      "04B62aNIpnBslQ",      "0Sz6UCGXBwi7At", "l49gEiyDkc3J00",
      "2T9nyWrRwuZj9W",      "CTtHTPRhSAPRIW", "sJZI3K8vP96JPm",
      "HYEy1brtrtrBJskEYa2", "UKb3uiFuGEi7m9", "yeRCnG0EEZ8Vrr"};
    const size_t magic = 0x43219876;

    ASSERT_EQ(nullptr, dir().attributes().encryption());
    dir().attributes() = irs::DirectoryAttributes{
      std::make_unique<tests::Rot13Encryption>(block_size, header_length)};
    auto* enc = dir().attributes().encryption();
    ASSERT_NE(nullptr, enc);

    uint64_t fp_magic = 0;
    uint64_t encrypted_length = 0;
    uint64_t checksum = 0;

    // write encrypted data
    {
      bstring header(enc->header_length(), 0);
      ASSERT_TRUE(enc->create_header("encrypted", &header[0]));

      auto out = dir().create("encrypted");
      auto raw_out = dir().create("raw");
      ASSERT_NE(nullptr, out);
      irs::WriteStr(*out, header);
      ASSERT_EQ(irs::bytes_io<uint64_t>::vsize(header.size()) + header.size(),
                out->Position());
      auto cipher = enc->create_stream("encrypted", &header[0]);
      ASSERT_NE(nullptr, cipher);
      ASSERT_EQ(block_size, cipher->block_size());

      irs::EncryptedOutput encryptor(std::move(out), *cipher, buf_size);
      ASSERT_EQ(nullptr, out);
      ASSERT_EQ(
        enc->header_length() + irs::bytes_io<uint64_t>::vsize(header.size()),
        encryptor.Stream().Position());
      ASSERT_EQ(std::max(buf_size, size_t(1)) * cipher->block_size(),
                encryptor.BufferSize());
      ASSERT_EQ(0, encryptor.Position());

      for (auto& str : data) {
        irs::WriteStr(encryptor, str);
        irs::WriteStr(*raw_out, str);
      }

      fp_magic = encryptor.Position();

      encryptor.WriteU64(magic);
      raw_out->WriteU64(magic);

      for (size_t i = 0, step = 321; i < data.size(); ++i) {
        auto value = 9886 + step;
        encryptor.WriteV64(value);
        raw_out->WriteV64(value);

        step += step;
        value = 9886 + step;

        encryptor.WriteU64(value);
        raw_out->WriteU64(value);

        step += step;
      }

      encryptor.Flush();
      ASSERT_EQ(raw_out->Position(), encryptor.Position());
      encrypted_length = encryptor.Position();
      checksum = raw_out->Checksum();
      out = encryptor.Release();
      ASSERT_NE(nullptr, out);
      ASSERT_EQ(out->Position(), irs::bytes_io<uint64_t>::vsize(header.size()) +
                                   header.size() + encrypted_length);
    }

    // read encrypted data
    {
      auto in = _dir->open("encrypted", irs::IOAdvice::NORMAL);
      bstring header = irs::ReadString<bstring>(*in);
      ASSERT_EQ(irs::bytes_io<uint64_t>::vsize(header.size()) + header.size() +
                  encrypted_length,
                in->Length());
      ASSERT_EQ(enc->header_length(), header.size());
      ASSERT_EQ(
        enc->header_length() + irs::bytes_io<uint64_t>::vsize(header.size()),
        in->Position());

      auto cipher = enc->create_stream("encrypted", &header[0]);
      ASSERT_NE(nullptr, cipher);
      ASSERT_EQ(block_size, cipher->block_size());

      irs::EncryptedInput decryptor(std::move(in), *cipher, buf_size);
      ASSERT_EQ(nullptr, in);
      ASSERT_EQ(
        enc->header_length() + irs::bytes_io<uint64_t>::vsize(header.size()),
        decryptor.stream().Position());
      ASSERT_EQ(std::max(buf_size, size_t(1)) * cipher->block_size(),
                decryptor.buffer_size());
      ASSERT_EQ(0, decryptor.Position());

      decryptor.Seek(fp_magic);
      ASSERT_EQ(magic, decryptor.ReadI64());
      ASSERT_EQ(fp_magic + sizeof(uint64_t), decryptor.Position());
      decryptor.Seek(0);

      // check dup
      {
        auto dup = decryptor.Dup();
        dup->Seek(fp_magic);
        ASSERT_EQ(magic, dup->ReadI64());
        ASSERT_EQ(fp_magic + sizeof(uint64_t), dup->Position());
        for (size_t i = 0, step = 321; i < data.size(); ++i) {
          ASSERT_EQ(9886 + step, dup->ReadV64());
          step += step;
          ASSERT_EQ(9886 + step, dup->ReadI64());
          step += step;
        }
      }

      // checksum
      {
        auto dup = decryptor.Reopen();
        dup->Seek(0);
        ASSERT_EQ(checksum, dup->Checksum(dup->Length()));
        ASSERT_EQ(0, dup->Position());  // checksum doesn't change position
        ASSERT_EQ(checksum, dup->Checksum(std::numeric_limits<size_t>::max()));
        ASSERT_EQ(0, dup->Position());  // checksum doesn't change position
      }

      // check reopen
      {
        auto dup = decryptor.Reopen();
        dup->Seek(fp_magic);
        ASSERT_EQ(magic, dup->ReadI64());
        ASSERT_EQ(fp_magic + sizeof(uint64_t), dup->Position());
        for (size_t i = 0, step = 321; i < data.size(); ++i) {
          ASSERT_EQ(9886 + step, dup->ReadV64());
          step += step;
          ASSERT_EQ(9886 + step, dup->ReadI64());
          step += step;
        }
      }

      // checksum
      {
        auto dup = decryptor.Reopen();
        dup->Seek(0);
        ASSERT_EQ(checksum, dup->Checksum(dup->Length()));
        ASSERT_EQ(0, dup->Position());  // checksum doesn't change position
        ASSERT_EQ(checksum, dup->Checksum(std::numeric_limits<size_t>::max()));
        ASSERT_EQ(0, dup->Position());  // checksum doesn't change position
      }

      for (auto& str : data) {
        const auto fp = decryptor.Position();
        ASSERT_EQ(str, irs::ReadString<std::string>(decryptor));
        decryptor.Seek(fp);
        ASSERT_EQ(str, irs::ReadString<std::string>(decryptor));
      }
      ASSERT_EQ(fp_magic, decryptor.Position());
      ASSERT_EQ(magic, decryptor.ReadI64());

      for (size_t i = 0, step = 321; i < data.size(); ++i) {
        ASSERT_EQ(9886 + step, decryptor.ReadV64());
        step += step;
        ASSERT_EQ(9886 + step, decryptor.ReadI64());
        step += step;
      }

      const auto fp = decryptor.Position();
      in = decryptor.release();
      ASSERT_NE(nullptr, in);
      ASSERT_EQ(
        in->Position(),
        header_length + irs::bytes_io<uint64_t>::vsize(header.size()) + fp);
    }
  }
};

TEST_P(EncryptionTestCase, encrypted_io_0) {
  AssertEcnryptedStreams(13, irs::CtrEncryption::kDefaultHeaderLength, 0);
}

TEST_P(EncryptionTestCase, encrypted_io_1) {
  AssertEcnryptedStreams(13, irs::CtrEncryption::kDefaultHeaderLength, 1);
}

TEST_P(EncryptionTestCase, encrypted_io_2) {
  AssertEcnryptedStreams(7, irs::CtrEncryption::kDefaultHeaderLength, 5);
}

TEST_P(EncryptionTestCase, encrypted_io_3) {
  AssertEcnryptedStreams(16, irs::CtrEncryption::kDefaultHeaderLength, 64);
}

TEST_P(EncryptionTestCase, encrypted_io_4) {
  AssertEcnryptedStreams(2048, irs::CtrEncryption::kDefaultHeaderLength, 1);
}

TEST(encryption_test_case, ensure_no_double_bufferring) {
  class BufferedOutput final : public irs::IndexOutput {
   public:
    BufferedOutput(irs::IndexOutput& out) noexcept
      : IndexOutput{_buf, std::end(_buf)}, _out{&out} {
      _offset = out.Position();
    }

    uint32_t Checksum() final {
      const auto checksum = _out->Checksum();
      _offset = _out->Position();
      return checksum;
    }

    using irs::BufferedOutput::Remain;

    void Flush() final {
      WriteHandle(_buf, Length());
      _pos = _buf;
    }

    uint64_t CloseImpl() final {
      SDB_ASSERT(false);
      return 0;
    }

    size_t LastWrittenSize() const noexcept { return _last_written_size; }

   private:
    void WriteHandle(const irs::byte_type* b, size_t len) {
      _last_written_size = len;
      _out->WriteBytes(b, len);
      _offset = _out->Position();
    }

    void WriteDirect(const irs::byte_type* b, size_t len) final {
      SDB_ASSERT(len == irs::kDefaultEncryptionBufferSize);
      WriteHandle(b, len);
    }

    irs::byte_type _buf[irs::kDefaultEncryptionBufferSize];
    irs::IndexOutput* _out;
    size_t _last_written_size{};
  };

  class BufferedInput : public irs::BufferedIndexInput {
   public:
    BufferedInput(IndexInput& in) noexcept : _in(&in) {
      irs::BufferedIndexInput::reset(_buf, sizeof _buf, 0);
    }

    uint64_t Length() const final { return _in->Length(); }

    IndexInput::ptr Dup() const final { throw irs::NotImplError(); }

    IndexInput::ptr Reopen() const final { throw irs::NotImplError(); }

    uint32_t Checksum(size_t offset) const final {
      return _in->Checksum(offset);
    }

    using irs::BufferedIndexInput::Remain;

    size_t LastReadSize() const noexcept { return _last_read_size; }

   protected:
    void SeekInternal(size_t pos) final { _in->Seek(pos); }

    size_t ReadInternal(irs::byte_type* b, size_t size) final {
      _last_read_size = size;
      return _in->ReadBytes(b, size);
    }

   private:
    irs::byte_type _buf[irs::kDefaultEncryptionBufferSize];
    IndexInput* _in;
    size_t _last_read_size{};
  };

  tests::Rot13Encryption enc(16);
  irs::MemoryOutput out(irs::IResourceManager::gNoop);

  bstring encrypted_header;
  encrypted_header.resize(enc.header_length());
  ASSERT_TRUE(enc.create_header("encrypted", &encrypted_header[0]));
  ASSERT_EQ(size_t(tests::Rot13Encryption::kDefaultHeaderLength),
            enc.header_length());

  bstring header = encrypted_header;
  auto cipher = enc.create_stream("encrypted", &header[0]);
  ASSERT_NE(nullptr, cipher);
  ASSERT_EQ(16, cipher->block_size());

  {
    BufferedOutput buf_out(out.stream);
    irs::EncryptedOutput enc_out(
      buf_out, *cipher,
      irs::kDefaultEncryptionBufferSize / cipher->block_size());
    ASSERT_EQ(nullptr, enc_out.Release());  // unmanaged instance

    for (size_t i = 0; i < 2 * irs::kDefaultEncryptionBufferSize + 1; ++i) {
      enc_out.WriteV32(i);
      // ensure no buffering
      ASSERT_EQ(size_t(irs::kDefaultEncryptionBufferSize), buf_out.Remain());
      if (buf_out.Position() >= irs::kDefaultEncryptionBufferSize) {
        ASSERT_EQ(size_t(irs::kDefaultEncryptionBufferSize),
                  buf_out.LastWrittenSize());
      }
    }

    enc_out.Flush();
    buf_out.Flush();
    ASSERT_EQ(enc_out.Position() - 3 * irs::kDefaultEncryptionBufferSize,
              buf_out.LastWrittenSize());
  }

  out.stream.Flush();

  {
    irs::MemoryIndexInput in(out.file);
    BufferedInput buf_in(in);
    irs::EncryptedInput enc_in(
      buf_in, *cipher,
      irs::kDefaultEncryptionBufferSize / cipher->block_size());

    for (size_t i = 0; i < 2 * irs::kDefaultEncryptionBufferSize + 1; ++i) {
      ASSERT_EQ(i, enc_in.ReadV32());
      ASSERT_EQ(0, buf_in.Remain());  // ensure no buffering
      if (buf_in.Position() <= 3 * irs::kDefaultEncryptionBufferSize) {
        ASSERT_EQ(size_t(irs::kDefaultEncryptionBufferSize),
                  buf_in.LastReadSize());
      } else {
        ASSERT_EQ(buf_in.Length() - 3 * irs::kDefaultEncryptionBufferSize,
                  buf_in.LastReadSize());
      }
    }
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(encryption_test, EncryptionTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs)),
                         tests::DirectoryTestCaseBase<>::to_string);

}  // namespace
