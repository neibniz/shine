#include "config/yaml_loader.hpp"
#include "redis/fake.hpp"
#include "redis/resp.hpp"
#include <gtest/gtest.h>
#include <zlib.h>

using namespace shine;
using namespace shine::redis;

static Value command(std::initializer_list<std::string> args) {
  Value result;
  result.type = '*';
  for (auto &arg : args) {
    Value value;
    value.type = '$';
    value.text = arg;
    result.array.push_back(std::move(value));
  }
  return result;
}

TEST(RedisCodec, CompressionAndLimits) {
  std::string repeated(4096, 'A');
  auto encoded = encodePayload(repeated, true);
  ASSERT_TRUE(encoded.ok());
  ASSERT_EQ((*encoded)[0], 1);
  auto decoded = decodePayload(*encoded);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(*decoded, repeated);
  auto raw = encodePayload("small", true);
  ASSERT_TRUE(raw.ok());
  EXPECT_EQ((*raw)[0], 0);
  EXPECT_FALSE(decodePayload("").ok());
  EXPECT_FALSE(decodePayload(std::string("\2unknown", 8)).ok());
  EXPECT_FALSE(decodePayload(std::string("\1invalid", 8)).ok());
  EXPECT_FALSE(encodePayload(std::string(kMaxPayload + 1, 'x'), true).ok());
  std::string huge(kMaxPayload + 1, 'x');
  uLongf n = compressBound(huge.size());
  std::string bomb(n + 1, '\0');
  bomb[0] = 1;
  ASSERT_EQ(compress2(reinterpret_cast<Bytef *>(bomb.data() + 1), &n,
                      reinterpret_cast<const Bytef *>(huge.data()), huge.size(),
                      Z_DEFAULT_COMPRESSION),
            Z_OK);
  bomb.resize(n + 1);
  EXPECT_FALSE(decodePayload(bomb).ok());
}

TEST(RedisFake, AuthAndDatabases) {
  Fake server("",
              {{"first", "a@example.com", 0}, {"second", "b@example.com", 0}});
  FakeSession a, b;
  a.id = 1;
  b.id = 2;
  EXPECT_EQ(server.reply(command({"PING"}), a).wire,
            "-NOAUTH Authentication required.\r\n");
  EXPECT_EQ(server.reply(command({"AUTH", "incorrect"}), a).wire.substr(0, 10),
            "-WRONGPASS");
  EXPECT_EQ(server.reply(command({"AUTH", "first"}), a).wire, "+OK\r\n");
  EXPECT_EQ(server.reply(command({"AUTH", "default", "second"}), b).wire,
            "+OK\r\n");
  EXPECT_EQ(server.reply(command({"SET", "key", "value", "NX"}), a).wire,
            "+OK\r\n");
  EXPECT_EQ(server.reply(command({"SET", "key", "other", "NX"}), a).wire,
            "$-1\r\n");
  EXPECT_EQ(server.reply(command({"GET", "key"}), b).wire, bulk("value"));
  EXPECT_EQ(server.reply(command({"SELECT", "1"}), b).wire, "+OK\r\n");
  EXPECT_EQ(server.reply(command({"GET", "key"}), b).wire, "$-1\r\n");
  EXPECT_EQ(server.reply(command({"EXPIRE", "key", "0"}), a).wire, ":1\r\n");
  EXPECT_EQ(server.reply(command({"TTL", "key"}), a).wire, ":-2\r\n");
  EXPECT_EQ(
      server.reply(command({"SET", "long", "v", "PX", "9223372036854"}), a)
          .wire,
      "+OK\r\n");
  EXPECT_EQ(server.reply(command({"GET", "long"}), a).wire, bulk("v"));
  EXPECT_FALSE(
      server.reply(command({"PUBLISH", "name", "payload"}), a).handled);
}

TEST(RedisConfig, UsersAndDefaultTransport) {
  auto make = [](const std::string &fields) {
    return config::loadFromString("inbounds:\n  - tag: in\n    protocol: "
                                  "shine\n    listen: 127.0.0.1:1234\n" +
                                  fields +
                                  "outbounds:\n  - tag: direct\n    protocol: "
                                  "direct\nroute:\n  default: direct\n");
  };
  auto good = make(
      "    users:\n      - auth: first\n        email: a@example.com\n      - "
      "auth: second\n        email: b@example.com\n    compression: true\n");
  ASSERT_TRUE(good.ok()) << good.status();
  EXPECT_EQ(good->inbounds[0].transport, "redis");
  EXPECT_EQ(good->inbounds[0].users.size(), 2);
  EXPECT_FALSE(make("    users: []\n").ok());
  EXPECT_FALSE(make("    users: null\n").ok());
  EXPECT_FALSE(
      make("    users:\n      - auth: same\n      - auth: same\n").ok());
  EXPECT_FALSE(
      make("    password: legacy\n    users:\n      - auth: first\n").ok());
  EXPECT_FALSE(make("    transport: tcp\n    compression: true\n").ok());
}
