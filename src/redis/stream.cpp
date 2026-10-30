#include "redis/stream.hpp"

#include <absl/strings/str_cat.h>
#include <boost/asio/connect.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/uuid/random_generator.hpp>

#include <algorithm>
#include <cctype>
#include <cstring>

namespace shine::redis {

Stream::Stream(asio::any_io_executor executor, std::shared_ptr<Reader> up,
               std::shared_ptr<Reader> down, std::string up_channel,
               std::string down_channel, bool client, bool compression,
               std::shared_ptr<WriteGate> down_gate,
               std::function<void()> close_handler)
    : executor_(std::move(executor)), up_(std::move(up)),
      down_(std::move(down)), up_channel_(std::move(up_channel)),
      down_channel_(std::move(down_channel)), client_(client),
      compression_(compression), down_gate_(std::move(down_gate)),
      close_handler_(std::move(close_handler)) {}

awaitable<StatusOr<std::size_t>> Stream::read(std::span<u8> buffer) {
  if (buffer.empty())
    co_return std::size_t{0};
  while (offset_ == leftover_.size()) {
    leftover_.clear();
    offset_ = 0;
    auto frame = co_await (client_ ? down_ : up_)->read();
    if (!frame.ok()) {
      close();
      co_return frame.status();
    }
    if (!client_ && !frame->array.empty())
      for (auto &c : frame->array[0].text)
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    if (!bulkArray(*frame, 3) ||
        frame->array[0].text != (client_ ? "message" : "PUBLISH") ||
        frame->array[1].text != (client_ ? down_channel_ : up_channel_)) {
      close();
      co_return absl::InvalidArgumentError("redis: invalid carrier message");
    }
    if (!client_) {
      auto st = co_await writeAll(*up_->socket(), ":1\r\n");
      if (!st.ok()) {
        close();
        co_return st;
      }
    }
    auto decoded = decodePayload(frame->array[2].text);
    if (!decoded.ok()) {
      close();
      co_return decoded.status();
    }
    leftover_ = std::move(*decoded);
  }
  const auto n = std::min(buffer.size(), leftover_.size() - offset_);
  std::memcpy(buffer.data(), leftover_.data() + offset_, n);
  offset_ += n;
  co_return n;
}

awaitable<Status> Stream::write(std::span<const u8> data) {
  while (!data.empty()) {
    const auto size = std::min(data.size(), kMaxPayload);
    auto payload = encodePayload(
        absl::string_view(reinterpret_cast<const char *>(data.data()), size),
        compression_);
    if (!payload.ok())
      co_return payload.status();
    auto wire = client_ ? array({"PUBLISH", up_channel_, *payload})
                        : array({"message", down_channel_, *payload});
    if (client_) {
      auto st = co_await writeAll(*up_->socket(), wire);
      if (!st.ok()) {
        close();
        co_return st;
      }
      auto ack = co_await up_->read();
      if (!ack.ok()) {
        close();
        co_return ack.status();
      }
      if (ack->type != ':' || ack->number != 1) {
        close();
        co_return absl::InvalidArgumentError("redis: invalid publish response");
      }
    } else {
      boost::system::error_code ec;
      co_await down_gate_->async_receive(
          asio::redirect_error(use_awaitable, ec));
      if (ec)
        co_return absl::UnavailableError(ec.message());
      auto st = co_await writeAll(*down_->socket(), wire);
      down_gate_->try_send(boost::system::error_code{});
      if (!st.ok()) {
        close();
        co_return st;
      }
    }
    data = data.subspan(size);
  }
  co_return absl::OkStatus();
}

awaitable<void> Stream::shutdownWrite() {
  close();
  co_return;
}
void Stream::close() {
  if (closed_.exchange(true))
    return;
  auto up = up_->socket();
  auto down = down_->socket();
  auto handler = close_handler_;
  asio::post(executor_, [up, down, handler = std::move(handler)]() {
    if (handler)
      handler();
    boost::system::error_code ec;
    up->close(ec);
    down->close(ec);
  });
}

namespace {
awaitable<Status> authenticate(Reader &reader, const std::string &password) {
  if (password.empty())
    co_return absl::OkStatus();
  auto wire = array({"AUTH", password});
  auto st = co_await writeAll(*reader.socket(), wire);
  if (!st.ok())
    co_return st;
  auto reply = co_await reader.read();
  if (!reply.ok())
    co_return reply.status();
  if (reply->type != '+' || reply->text != "OK")
    co_return absl::UnauthenticatedError("redis: authentication failed");
  co_return absl::OkStatus();
}
} // namespace

awaitable<StatusOr<Carrier>> dial(asio::any_io_executor executor,
                                  tcp::endpoint endpoint, std::string password,
                                  bool compression,
                                  std::chrono::seconds timeout) {
  auto down = std::make_shared<tcp::socket>(executor),
       up = std::make_shared<tcp::socket>(executor);
  auto timer = std::make_shared<asio::steady_timer>(executor);
  timer->expires_after(timeout);
  timer->async_wait([timer, down, up](const boost::system::error_code &ec) {
    if (!ec) {
      boost::system::error_code ignored;
      down->close(ignored);
      up->close(ignored);
    }
  });
  // Cancels the timeout even on early return.
  struct CancelTimer {
    std::shared_ptr<asio::steady_timer> value;
    ~CancelTimer() { value->cancel(); }
  } cancel{timer};
  boost::system::error_code ec;
  co_await down->async_connect(endpoint,
                               asio::redirect_error(use_awaitable, ec));
  if (ec)
    co_return absl::UnavailableError(ec.message());
  auto down_reader = std::make_shared<Reader>(down);
  auto st = co_await authenticate(*down_reader, password);
  if (!st.ok())
    co_return st;
  boost::uuids::uuid random;
  try {
    boost::uuids::random_generator_pure source;
    random = source();
  } catch (...) {
    co_return absl::UnavailableError("redis: random source unavailable");
  }
  static constexpr char hex[] = "0123456789abcdef";
  std::string base = "events:";
  for (auto b : random) {
    base.push_back(hex[b >> 4]);
    base.push_back(hex[b & 15]);
  }
  std::string down_channel = base + ":out", up_channel = base + ":in";
  auto subscribe = array({"SUBSCRIBE", down_channel});
  st = co_await writeAll(*down, subscribe);
  if (!st.ok())
    co_return st;
  auto sub = co_await down_reader->read();
  if (!sub.ok())
    co_return sub.status();
  if (sub->type != '*' || sub->array.size() != 3 ||
      sub->array[0].text != "subscribe" || sub->array[1].text != down_channel ||
      sub->array[2].type != ':' || sub->array[2].number != 1)
    co_return absl::InvalidArgumentError(
        "redis: invalid subscription response");
  co_await up->async_connect(endpoint, asio::redirect_error(use_awaitable, ec));
  if (ec)
    co_return absl::UnavailableError(ec.message());
  auto up_reader = std::make_shared<Reader>(up);
  st = co_await authenticate(*up_reader, password);
  if (!st.ok())
    co_return st;
  auto publish = array({"PUBLISH", up_channel, ""});
  st = co_await writeAll(*up, publish);
  if (!st.ok())
    co_return st;
  auto ack = co_await up_reader->read();
  if (!ack.ok())
    co_return ack.status();
  if (ack->type != ':' || ack->number != 1)
    co_return absl::InvalidArgumentError("redis: pairing failed");
  auto stream = std::make_shared<Stream>(
      executor, up_reader, down_reader, std::move(up_channel),
      std::move(down_channel), true, compression);
  co_return Carrier{std::move(stream), std::move(executor)};
}

} // namespace shine::redis
