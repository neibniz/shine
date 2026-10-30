#include "redis/resp.hpp"

#include <absl/strings/str_cat.h>
#include <boost/asio/read.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/write.hpp>
#include <zlib.h>

#include <charconv>
#include <istream>
#include <limits>

namespace shine::redis {

awaitable<StatusOr<std::string>> Reader::line() {
  boost::system::error_code ec;
  auto n = co_await asio::async_read_until(
      *socket_, input_, "\r\n", asio::redirect_error(use_awaitable, ec));
  if (ec)
    co_return absl::UnavailableError(ec.message());
  if (n < 2 || n > 4096)
    co_return absl::InvalidArgumentError("redis: invalid line");
  std::string result(n - 2, '\0');
  std::istream stream(&input_);
  if (!result.empty())
    stream.read(result.data(), static_cast<std::streamsize>(result.size()));
  stream.ignore(2);
  co_return result;
}

awaitable<StatusOr<i64>> Reader::number() {
  auto text = co_await line();
  if (!text.ok())
    co_return text.status();
  i64 number = 0;
  auto [end, ec] =
      std::from_chars(text->data(), text->data() + text->size(), number);
  if (ec != std::errc{} || end != text->data() + text->size())
    co_return absl::InvalidArgumentError("redis: invalid integer");
  co_return number;
}

awaitable<StatusOr<Value>> Reader::read(std::size_t limit) {
  int nodes = 2048;
  co_return co_await value(limit, nodes, 0);
}

awaitable<StatusOr<Value>> Reader::value(std::size_t &budget, int &nodes,
                                         int depth) {
  if (depth > 2 || nodes-- <= 0)
    co_return absl::InvalidArgumentError("redis: nesting limit");
  boost::system::error_code ec;
  if (input_.size() < 1) {
    co_await asio::async_read(*socket_, input_, asio::transfer_exactly(1),
                              asio::redirect_error(use_awaitable, ec));
    if (ec)
      co_return absl::UnavailableError(ec.message());
  }
  std::istream source(&input_);
  Value result;
  result.type = static_cast<char>(source.get());
  if (result.type == '+' || result.type == '-') {
    auto s = co_await line();
    if (!s.ok())
      co_return s.status();
    if (s->size() > budget)
      co_return absl::ResourceExhaustedError("redis: frame too large");
    budget -= s->size();
    result.text = std::move(*s);
    co_return result;
  }
  if (result.type == ':') {
    auto n = co_await number();
    if (!n.ok())
      co_return n.status();
    result.number = *n;
    co_return result;
  }
  if (result.type == '$') {
    auto n = co_await number();
    if (!n.ok())
      co_return n.status();
    if (*n == -1) {
      result.is_null = true;
      co_return result;
    }
    if (*n < 0 || static_cast<u64>(*n) > budget)
      co_return absl::ResourceExhaustedError("redis: bulk too large");
    budget -= static_cast<std::size_t>(*n);
    const auto need = static_cast<std::size_t>(*n) + 2;
    if (input_.size() < need) {
      co_await asio::async_read(*socket_, input_,
                                asio::transfer_exactly(need - input_.size()),
                                asio::redirect_error(use_awaitable, ec));
      if (ec)
        co_return absl::UnavailableError(ec.message());
    }
    result.text.resize(static_cast<std::size_t>(*n));
    std::istream stream(&input_);
    if (*n)
      stream.read(result.text.data(), static_cast<std::streamsize>(*n));
    if (stream.get() != '\r' || stream.get() != '\n')
      co_return absl::InvalidArgumentError("redis: bad bulk terminator");
    co_return result;
  }
  if (result.type == '*') {
    auto n = co_await number();
    if (!n.ok())
      co_return n.status();
    if (*n == -1) {
      result.is_null = true;
      co_return result;
    }
    if (*n < 0 || *n > 1024 || *n > nodes)
      co_return absl::InvalidArgumentError("redis: invalid array length");
    result.array.reserve(static_cast<std::size_t>(*n));
    for (i64 i = 0; i < *n; ++i) {
      auto item = co_await value(budget, nodes, depth + 1);
      if (!item.ok())
        co_return item.status();
      result.array.push_back(std::move(*item));
    }
    co_return result;
  }
  co_return absl::InvalidArgumentError("redis: invalid RESP type");
}

std::string bulk(absl::string_view text) {
  return absl::StrCat("$", text.size(), "\r\n", text, "\r\n");
}
std::string array(const std::vector<absl::string_view> &parts) {
  std::string out = absl::StrCat("*", parts.size(), "\r\n");
  for (auto part : parts)
    out.append(bulk(part));
  return out;
}
std::string subscribeAck(absl::string_view channel) {
  return absl::StrCat("*3\r\n", bulk("subscribe"), bulk(channel), ":1\r\n");
}
awaitable<Status> writeAll(tcp::socket &socket, absl::string_view wire) {
  boost::system::error_code ec;
  co_await asio::async_write(socket, asio::buffer(wire.data(), wire.size()),
                             asio::redirect_error(use_awaitable, ec));
  if (ec)
    co_return absl::UnavailableError(ec.message());
  co_return absl::OkStatus();
}
bool bulkArray(const Value &v, std::size_t count) {
  if (v.type != '*' || v.array.size() != count || v.is_null)
    return false;
  for (const auto &e : v.array)
    if (e.type != '$' || e.is_null)
      return false;
  return true;
}

StatusOr<std::string> encodePayload(absl::string_view data, bool compress) {
  if (data.size() > kMaxPayload)
    return absl::ResourceExhaustedError("redis: payload too large");
  if (compress && data.size() >= 128) {
    uLongf length = compressBound(static_cast<uLong>(data.size()));
    std::string encoded(length + 1, '\0');
    encoded[0] = 1;
    int code =
        compress2(reinterpret_cast<Bytef *>(encoded.data() + 1), &length,
                  reinterpret_cast<const Bytef *>(data.data()),
                  static_cast<uLong>(data.size()), Z_DEFAULT_COMPRESSION);
    if (code != Z_OK)
      return absl::InternalError("redis: zlib compression failed");
    encoded.resize(static_cast<std::size_t>(length) + 1);
    if (encoded.size() + 16 <= data.size() + 1)
      return encoded;
  }
  std::string result(1, '\0');
  result.append(data.data(), data.size());
  return result;
}
StatusOr<std::string> decodePayload(absl::string_view data) {
  if (data.empty())
    return absl::InvalidArgumentError("redis: empty envelope");
  if (data[0] == 0) {
    if (data.size() - 1 > kMaxPayload)
      return absl::ResourceExhaustedError("redis: payload too large");
    return std::string(data.data() + 1, data.size() - 1);
  }
  if (data[0] != 1)
    return absl::InvalidArgumentError("redis: unknown envelope");
  std::string result(kMaxPayload + 1, '\0');
  uLongf size = static_cast<uLongf>(result.size());
  int code = uncompress(reinterpret_cast<Bytef *>(result.data()), &size,
                        reinterpret_cast<const Bytef *>(data.data() + 1),
                        static_cast<uLong>(data.size() - 1));
  if (code != Z_OK || size > kMaxPayload)
    return absl::InvalidArgumentError(
        "redis: invalid or oversized compressed payload");
  result.resize(static_cast<std::size_t>(size));
  return result;
}

} // namespace shine::redis
