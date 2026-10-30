#include "redis/acceptor.hpp"
#include <algorithm>
#include <cctype>

namespace shine::redis {
struct Acceptor::Pair {
  std::string channel, secret;
  std::shared_ptr<Reader> down, up;
  std::shared_ptr<WriteGate> gate, ready_signal;
  std::shared_ptr<asio::steady_timer> timer;
  bool closed = false, paired = false;
};
namespace {
std::string upper(std::string s) {
  for (auto &c : s)
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  return s;
}
bool validChannel(const std::string &s) {
  if (s.empty() || s.size() > 128)
    return false;
  for (unsigned char c : s)
    if (c < 33 || c > 126)
      return false;
  return true;
}
struct Deadline {
  std::shared_ptr<asio::steady_timer> timer;
  std::shared_ptr<tcp::socket> socket;
  void reset(std::chrono::seconds duration) {
    timer->expires_after(duration);
    auto s = socket;
    timer->async_wait([s](boost::system::error_code ec) {
      if (!ec) {
        boost::system::error_code ignored;
        s->close(ignored);
      }
    });
  }
  ~Deadline() { timer->cancel(); }
};
} // namespace
Acceptor::Acceptor(asio::any_io_executor ex, std::string password,
                   std::vector<AuthUser> users, bool compression,
                   std::chrono::seconds timeout)
    : strand_(asio::make_strand(ex)),
      fake_(std::move(password), std::move(users)), compression_(compression),
      timeout_(timeout) {}
void Acceptor::closePair(const std::shared_ptr<Pair> &p) {
  if (p->closed)
    return;
  p->closed = true;
  boost::system::error_code ec;
  p->timer->cancel();
  p->gate->close();
  p->ready_signal->close();
  p->down->socket()->close(ec);
  sockets_.erase(p->down->socket());
  if (p->up) {
    p->up->socket()->close(ec);
    sockets_.erase(p->up->socket());
  }
  auto it = pending_.find(p->channel);
  if (it != pending_.end() && it->second == p)
    pending_.erase(it);
  pairs_.erase(p);
}
void Acceptor::stop() {
  auto self = shared_from_this();
  asio::post(strand_, [self] {
    self->stopped_ = true;
    auto pairs = self->pairs_;
    for (auto &p : pairs)
      self->closePair(p);
    boost::system::error_code ec;
    for (auto &s : self->sockets_)
      s->close(ec);
    self->sockets_.clear();
  });
}
awaitable<void> Acceptor::monitor(std::shared_ptr<Pair> p) {
  while (!p->closed) {
    auto cmd = co_await p->down->read(4096);
    if (!cmd.ok() || cmd->array.empty() ||
        !bulkArray(*cmd, cmd->array.size()) ||
        upper(cmd->array[0].text) != "PING" || cmd->array.size() > 2)
      break;
    auto response = array({"pong", cmd->array.size() == 2
                                       ? absl::string_view(cmd->array[1].text)
                                       : absl::string_view()});
    boost::system::error_code ec;
    co_await p->gate->async_receive(asio::redirect_error(use_awaitable, ec));
    if (ec)
      break;
    auto st = co_await writeAll(*p->down->socket(), response);
    p->gate->try_send(boost::system::error_code{});
    if (!st.ok())
      break;
  }
  closePair(p);
}
awaitable<StatusOr<std::shared_ptr<ISessionStream>>>
Acceptor::accept(tcp::socket socket) {
  auto sock = std::make_shared<tcp::socket>(std::move(socket));
  sockets_.insert(sock);
  auto reader = std::make_shared<Reader>(sock);
  Deadline deadline{std::make_shared<asio::steady_timer>(strand_), sock};
  deadline.reset(timeout_);
  FakeSession state;
  state.authenticated = !fake_.required();
  state.id = ++next_id_;
  bool owned = false;
  struct Cleanup {
    Acceptor *owner;
    std::shared_ptr<tcp::socket> sock;
    bool &owned;
    ~Cleanup() {
      if (!owned) {
        boost::system::error_code ec;
        sock->close(ec);
        owner->sockets_.erase(sock);
      }
    }
  } cleanup{this, sock, owned};
  while (!stopped_) {
    auto cmd = co_await reader->read();
    if (!cmd.ok())
      co_return cmd.status();
    if (cmd->type != '*' || cmd->array.empty() || cmd->array[0].type != '$')
      co_return absl::InvalidArgumentError("redis: expected command array");
    auto reply = fake_.reply(*cmd, state);
    if (reply.handled) {
      auto st = co_await writeAll(*sock, reply.wire);
      if (!st.ok())
        co_return st;
      if (reply.quit)
        co_return std::shared_ptr<ISessionStream>{};
      deadline.reset(std::chrono::minutes(2));
      continue;
    }
    auto name = upper(cmd->array[0].text);
    if (name == "SUBSCRIBE" && bulkArray(*cmd, 2)) {
      auto channel = cmd->array[1].text;
      if (!validChannel(channel))
        co_return absl::InvalidArgumentError("redis: invalid channel");
      if (pending_.size() >= 256 || pending_.count(channel))
        co_return absl::ResourceExhaustedError("redis: subscriber unavailable");
      auto p = std::make_shared<Pair>();
      p->channel = channel;
      p->secret = state.secret;
      p->down = reader;
      p->gate = std::make_shared<WriteGate>(strand_, 1);
      p->gate->try_send(boost::system::error_code{});
      p->ready_signal = std::make_shared<WriteGate>(strand_, 1);
      p->timer = std::make_shared<asio::steady_timer>(strand_);
      pending_[channel] = p;
      pairs_.insert(p);
      owned = true;
      auto wire = subscribeAck(channel);
      auto st = co_await writeAll(*sock, wire);
      if (!st.ok()) {
        closePair(p);
        co_return st;
      }
      if (p->closed)
        co_return absl::UnavailableError("redis: subscriber closed");
      p->ready_signal->try_send(boost::system::error_code{});
      p->timer->expires_after(std::chrono::seconds(10));
      auto self = shared_from_this();
      p->timer->async_wait([self, p](boost::system::error_code ec) {
        if (!ec && !p->paired)
          self->closePair(p);
      });
      asio::co_spawn(
          strand_, [self, p]() { return self->monitor(p); }, asio::detached);
      co_return std::shared_ptr<ISessionStream>{};
    }
    if (name == "PUBLISH" && bulkArray(*cmd, 3)) {
      auto channel = cmd->array[1].text;
      std::shared_ptr<Pair> p;
      if (cmd->array[2].text.empty() && channel.size() >= 3 &&
          channel.substr(channel.size() - 3) == ":in") {
        auto down = channel.substr(0, channel.size() - 3) + ":out";
        auto it = pending_.find(down);
        if (it != pending_.end() &&
            authEquals(it->second->secret, state.secret)) {
          p = it->second;
          pending_.erase(it);
        }
      }
      if (!p) {
        auto st = co_await writeAll(*sock, ":0\r\n");
        if (!st.ok())
          co_return st;
        deadline.reset(std::chrono::minutes(2));
        continue;
      }
      boost::system::error_code ready_error;
      co_await p->ready_signal->async_receive(
          asio::redirect_error(use_awaitable, ready_error));
      if (ready_error || p->closed)
        co_return absl::UnavailableError("redis: subscriber closed");
      p->up = reader;
      p->paired = true;
      p->timer->cancel();
      owned = true;
      auto st = co_await writeAll(*sock, ":1\r\n");
      if (!st.ok()) {
        closePair(p);
        co_return st;
      }
      if (p->closed)
        co_return absl::UnavailableError("redis: subscriber closed");
      auto self = shared_from_this();
      auto stream = std::make_shared<Stream>(
          strand_, reader, p->down, channel, p->channel, false, compression_,
          p->gate, [self, p]() { self->closePair(p); });
      co_return std::shared_ptr<ISessionStream>(std::move(stream));
    }
    std::string low = name;
    for (auto &c : low)
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    auto wire = "-ERR wrong number of arguments for '" + low + "' command\r\n";
    auto st = co_await writeAll(*sock, wire);
    if (!st.ok())
      co_return st;
    deadline.reset(std::chrono::minutes(2));
  }
  co_return absl::UnavailableError("redis: stopped");
}
} // namespace shine::redis
