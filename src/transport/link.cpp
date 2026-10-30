#include "transport/link.hpp"

#include "core/logging.hpp"
#include "proto/resp2_writer.hpp"
#include "transport/tcp_stream.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/write.hpp>

#include <absl/strings/str_cat.h>

#include <utility>

namespace shine {

using namespace boost::asio::experimental::awaitable_operators;

Link::Link(tcp::socket sock, LinkOptions opts)
    : opts_(std::move(opts)), strand_(asio::make_strand(sock.get_executor())),
      stream_(std::make_shared<TcpSocketStream>(std::move(sock))), parser_(opts_.max_payload) {
    write_chan_       = std::make_unique<WriteChan>(strand_, 1024);
    handshake_signal_ = std::make_unique<HandshakeSignal>(strand_, 1);
    rng_.seed(static_cast<u64>(nowNs()) ^ reinterpret_cast<std::uintptr_t>(this));
}

Link::Link(std::shared_ptr<ISessionStream> stream, asio::any_io_executor executor,
           LinkOptions opts)
    : opts_(std::move(opts)), strand_(asio::make_strand(std::move(executor))),
      stream_(std::move(stream)), parser_(opts_.max_payload) {
    write_chan_       = std::make_unique<WriteChan>(strand_, 1024);
    handshake_signal_ = std::make_unique<HandshakeSignal>(strand_, 1);
    rng_.seed(static_cast<u64>(nowNs()) ^ reinterpret_cast<std::uintptr_t>(this));
}

Link::~Link() {
    markClosed();
}

void Link::start(NewSessionHandler on_new_session) {
    on_new_session_ = std::move(on_new_session);
    auto self = shared_from_this();
    last_pong_ns_.store(nowNs(), std::memory_order_relaxed);

    // Handshake + reader on strand.
    asio::co_spawn(strand_, [self]() -> awaitable<void> {
        if (self->opts_.is_server) {
            co_await self->handshakeServer();
        } else {
            co_await self->handshakeClient();
        }
        if (self->closed_) co_return;
        self->handshake_done_.store(true, std::memory_order_release);
        self->handshake_signal_->try_send(boost::system::error_code{});
        self->handshake_signal_->close();
        co_await self->readerLoop();
    }, asio::detached);

    asio::co_spawn(strand_, [self]() { return self->writerLoop(); },
                   asio::detached);
    asio::co_spawn(strand_, [self]() { return self->heartbeatLoop(); },
                   asio::detached);
}

awaitable<void> Link::handshakeServer() {
    // Expect HELLO, optionally AUTH.
    // Set a handshake timeout via steady_timer race.
    auto timer = std::make_shared<asio::steady_timer>(strand_);
    timer->expires_after(opts_.handshake_timeout);
    auto self = shared_from_this();
    asio::co_spawn(strand_, [self, timer]() -> awaitable<void> {
        boost::system::error_code ec;
        co_await timer->async_wait(asio::redirect_error(use_awaitable, ec));
        if (!ec) co_await self->failLink("handshake timeout");
    }, asio::detached);

    auto one = [&]() -> awaitable<StatusOr<proto::ShineFrame>> {
        for (;;) {
            proto::Resp2Frame rf;
            auto sc = parser_.tryParse(read_buf_, rf);
            if (!sc.ok()) co_return sc.status();
            if (*sc > 0) {
                auto sf = proto::decodeShineFrame(rf);
                read_buf_.consume(*sc);
                if (!sf.ok()) co_return sf.status();
                co_return std::move(*sf);
            }
            read_buf_.ensure_writable(8192);
            auto n = co_await stream_->read(std::span<u8>(read_buf_.writable_ptr(), read_buf_.writable_size()));
            if (!n.ok()) co_return n.status();
            if (*n == 0) co_return absl::UnavailableError("eof");
            read_buf_.committed(*n);
        }
    };

    auto f1 = co_await one();
    if (!f1.ok()) { co_await failLink(f1.status().message()); co_return; }
    if (!std::holds_alternative<proto::HelloFrame>(*f1)) {
        co_await failLink("expected HELLO"); co_return;
    }
    // Reply HELLO.
    std::string out;
    proto::encodeHello(out, proto::HelloFrame{"1", "mux,flow"});
    co_await enqueueFrame(std::move(out));

    if (!opts_.password.empty() || !opts_.users.empty()) {
        auto f2 = co_await one();
        if (!f2.ok()) { co_await failLink(f2.status().message()); co_return; }
        if (!std::holds_alternative<proto::AuthFrame>(*f2)) {
            co_await failLink("expected AUTH"); co_return;
        }
        const auto& password = std::get<proto::AuthFrame>(*f2).password;
        bool authenticated = !opts_.password.empty() && authEquals(password, opts_.password);
        for (const auto& user : opts_.users) {
            if (authEquals(password, user.auth)) {
                authenticated = true;
                identity_ = UserIdentity{user.email, user.level};
            }
        }
        if (authenticated && opts_.users.empty()) identity_ = UserIdentity{};
        if (!authenticated) {
            std::string err;
            proto::encodeErr(err, proto::ErrFrame{"AUTH", "bad password"});
            co_await enqueueFrame(std::move(err));
            co_await failLink("bad password"); co_return;
        }
        std::string ok;
        proto::encodeOk(ok);
        co_await enqueueFrame(std::move(ok));
    }
    timer->cancel();
}

awaitable<void> Link::handshakeClient() {
    std::string out;
    proto::encodeHello(out, proto::HelloFrame{"1", "mux,flow"});
    co_await enqueueFrame(std::move(out));

    auto timer = std::make_shared<asio::steady_timer>(strand_);
    timer->expires_after(opts_.handshake_timeout);
    auto self = shared_from_this();
    asio::co_spawn(strand_, [self, timer]() -> awaitable<void> {
        boost::system::error_code ec;
        co_await timer->async_wait(asio::redirect_error(use_awaitable, ec));
        if (!ec) co_await self->failLink("handshake timeout");
    }, asio::detached);

    auto one = [&]() -> awaitable<StatusOr<proto::ShineFrame>> {
        for (;;) {
            proto::Resp2Frame rf;
            auto sc = parser_.tryParse(read_buf_, rf);
            if (!sc.ok()) co_return sc.status();
            if (*sc > 0) {
                auto sf = proto::decodeShineFrame(rf);
                read_buf_.consume(*sc);
                if (!sf.ok()) co_return sf.status();
                co_return std::move(*sf);
            }
            read_buf_.ensure_writable(8192);
            auto n = co_await stream_->read(std::span<u8>(read_buf_.writable_ptr(), read_buf_.writable_size()));
            if (!n.ok()) co_return n.status();
            if (*n == 0) co_return absl::UnavailableError("eof");
            read_buf_.committed(*n);
        }
    };

    auto f1 = co_await one();
    if (!f1.ok()) { co_await failLink(f1.status().message()); co_return; }
    if (!std::holds_alternative<proto::HelloFrame>(*f1)) {
        co_await failLink("expected HELLO reply"); co_return;
    }
    if (!opts_.password.empty()) {
        std::string auth;
        proto::encodeAuth(auth, proto::AuthFrame{opts_.password});
        co_await enqueueFrame(std::move(auth));
        auto f2 = co_await one();
        if (!f2.ok()) { co_await failLink(f2.status().message()); co_return; }
        if (std::holds_alternative<proto::ErrFrame>(*f2)) {
            co_await failLink("auth error"); co_return;
        }
        if (!std::holds_alternative<proto::OkFrame>(*f2)) {
            co_await failLink("expected OK after AUTH"); co_return;
        }
    }
    timer->cancel();
}

awaitable<void> Link::readerLoop() {
    while (!closed_.load(std::memory_order_acquire)) {
        proto::Resp2Frame rf;
        auto sc = parser_.tryParse(read_buf_, rf);
        if (!sc.ok()) {
            SHINE_WARN("resp2 parse error peer={}: {}", opts_.peer_label, sc.status().message());
            co_await failLink(sc.status().message()); co_return;
        }
        if (*sc == 0) {
            read_buf_.ensure_writable(16 * 1024);
            auto n = co_await stream_->read(std::span<u8>(read_buf_.writable_ptr(), read_buf_.writable_size()));
            if (!n.ok() || *n == 0) {
                co_await failLink(n.ok() ? "eof" : n.status().message()); co_return;
            }
            read_buf_.committed(*n);
            continue;
        }
        auto frame = proto::decodeShineFrame(rf);
        if (!frame.ok()) {
            read_buf_.consume(*sc);
            co_await failLink(frame.status().message()); co_return;
        }
        co_await dispatchFrame(std::move(*frame));
        read_buf_.consume(*sc);
    }
}

awaitable<void> Link::writerLoop() {
    while (!closed_.load(std::memory_order_acquire)) {
        boost::system::error_code ec;
        auto msg = co_await write_chan_->async_receive(asio::redirect_error(use_awaitable, ec));
        if (ec) co_return;
        if (msg.empty()) continue;
        auto st = co_await stream_->write(std::span<const u8>(reinterpret_cast<const u8*>(msg.data()), msg.size()));
        if (!st.ok()) {
            SHINE_WARN("link writer error peer={}: {}", opts_.peer_label, st.message());
            co_await failLink(st.message()); co_return;
        }
    }
}

awaitable<void> Link::heartbeatLoop() {
    asio::steady_timer tick(strand_);
    while (!closed_.load(std::memory_order_acquire)) {
        tick.expires_after(std::chrono::seconds(1));
        boost::system::error_code ec;
        co_await tick.async_wait(asio::redirect_error(use_awaitable, ec));
        if (ec) co_return;
        if (!handshake_done_.load(std::memory_order_acquire)) continue;

        u64 now = nowNs();
        u64 last_pong = last_pong_ns_.load(std::memory_order_relaxed);
        u64 last_sent = last_ping_sent_ns_.load(std::memory_order_relaxed);

        auto ping_ns    = static_cast<u64>(opts_.ping_interval.count()) * 1'000'000'000ull;
        auto pong_to_ns = static_cast<u64>(opts_.pong_timeout.count()) * 1'000'000'000ull;

        // If outstanding ping exceeded pong_timeout, fail link.
        u64 pending = pending_ping_nonce_.load(std::memory_order_relaxed);
        if (pending != 0 && now - last_sent > pong_to_ns) {
            SHINE_WARN("link ping timeout peer={}", opts_.peer_label);
            co_await failLink("ping timeout");
            co_return;
        }
        // Idle long enough → send ping.
        if (pending == 0 && now - last_pong > ping_ns) {
            u64 nonce;
            {
                absl::MutexLock lock(&rng_mu_);
                nonce = rng_();
            }
            if (nonce == 0) nonce = 1;
            pending_ping_nonce_.store(nonce, std::memory_order_relaxed);
            last_ping_sent_ns_.store(now, std::memory_order_relaxed);
            std::string wire;
            proto::encodePing(wire, proto::PingFrame{nonce});
            co_await enqueueFrame(std::move(wire));
        }
    }
}

awaitable<void> Link::dispatchFrame(proto::ShineFrame frame) {
    if (std::holds_alternative<proto::DataFrame>(frame)) {
        auto& df = std::get<proto::DataFrame>(frame);
        auto s = findSession(df.sid);
        if (!s) co_return;
        auto pay = std::make_shared<std::string>(
            reinterpret_cast<const char*>(df.payload), df.size);
        co_await s->onData(std::move(pay));
        co_return;
    }
    if (std::holds_alternative<proto::UdpDataFrame>(frame)) {
        auto& df = std::get<proto::UdpDataFrame>(frame);
        auto s = findSession(df.sid);
        if (!s) co_return;
        auto addr = Address::parseBinary(df.addr_blob);
        if (!addr.ok()) co_return;
        auto pay = std::make_shared<std::string>(std::move(df.payload));
        co_await s->onUdpData(std::move(pay), *addr);
        co_return;
    }
    if (std::holds_alternative<proto::NewFrame>(frame) || std::holds_alternative<proto::NewUdpFrame>(frame)) {
        if (!on_new_session_) co_return;
        bool is_udp = std::holds_alternative<proto::NewUdpFrame>(frame);
        u64 sid = is_udp ? std::get<proto::NewUdpFrame>(frame).sid : std::get<proto::NewFrame>(frame).sid;
        u32 win = is_udp ? 0 : std::get<proto::NewFrame>(frame).init_window;
        
        Address target;
        if (!is_udp) {
            auto addr = Address::parseBinary(std::get<proto::NewFrame>(frame).addr_blob);
            if (!addr.ok()) {
                std::string wire;
                proto::encodeReset(wire, proto::ResetFrame{sid});
                co_await enqueueFrame(std::move(wire));
                co_return;
            }
            target = *addr;
        }

        auto s = std::make_shared<Session>(
            shared_from_this(), sid, target, win, opts_.init_window,
            is_udp ? SessionRequest::Protocol::UDP : SessionRequest::Protocol::TCP);
        {
            absl::MutexLock lock(&sessions_mu_);
            sessions_.emplace(sid, s);
        }
        std::string wire;
        proto::encodeAck(wire, proto::AckFrame{sid});
        co_await enqueueFrame(std::move(wire));
        // Spawn the handler DETACHED so the reader loop is not blocked for the
        // lifetime of the session.
        auto self   = shared_from_this();
        auto handler = on_new_session_;
        asio::co_spawn(strand_,
            [self, s, target, handler]() -> awaitable<void> {
                co_await handler(s, target);
            },
            asio::detached);
        co_return;
    }
    if (std::holds_alternative<proto::AckFrame>(frame)) {
        // Client side: could flip state; sessions are usable once NEW is sent.
        co_return;
    }
    if (std::holds_alternative<proto::CloseFrame>(frame)) {
        auto& f = std::get<proto::CloseFrame>(frame);
        if (auto s = findSession(f.sid)) {
            s->onPeerClose();
            if (s->state() == Session::State::Closed) eraseSession(f.sid);
        }
        co_return;
    }
    if (std::holds_alternative<proto::ResetFrame>(frame)) {
        auto& f = std::get<proto::ResetFrame>(frame);
        if (auto s = findSession(f.sid)) { s->onReset(); eraseSession(f.sid); }
        co_return;
    }
    if (std::holds_alternative<proto::WindowFrame>(frame)) {
        auto& f = std::get<proto::WindowFrame>(frame);
        if (auto s = findSession(f.sid)) s->onWindow(f.delta);
        co_return;
    }
    if (std::holds_alternative<proto::PingFrame>(frame)) {
        auto& f = std::get<proto::PingFrame>(frame);
        std::string wire;
        proto::encodePong(wire, proto::PongFrame{f.nonce});
        co_await enqueueFrame(std::move(wire));
        co_return;
    }
    if (std::holds_alternative<proto::PongFrame>(frame)) {
        auto& f = std::get<proto::PongFrame>(frame);
        u64 expect = pending_ping_nonce_.load(std::memory_order_relaxed);
        if (expect == f.nonce) {
            pending_ping_nonce_.store(0, std::memory_order_relaxed);
            last_pong_ns_.store(nowNs(), std::memory_order_relaxed);
        }
        co_return;
    }
    // Other frame types at this stage (HELLO/AUTH/OK/ERR) are ignored post-handshake.
    co_return;
}

awaitable<void> Link::enqueueFrame(std::string frame_bytes) {
    if (closed_.load(std::memory_order_acquire)) co_return;
    boost::system::error_code ec;
    co_await write_chan_->async_send(boost::system::error_code{}, std::move(frame_bytes),
                                     asio::redirect_error(use_awaitable, ec));
}

awaitable<StatusOr<SessionPtr>> Link::openSession(Address target, bool is_udp) {
    if (closed_.load(std::memory_order_acquire)) {
        co_return absl::UnavailableError("link closed");
    }
    u64 sid;
    {
        absl::MutexLock lock(&rng_mu_);
        do { sid = rng_(); } while (sid == 0);
    }
    auto self = shared_from_this();
    auto s    = std::make_shared<Session>(
        self, sid, target, opts_.init_window, opts_.init_window,
        is_udp ? SessionRequest::Protocol::UDP : SessionRequest::Protocol::TCP);
    {
        absl::MutexLock lock(&sessions_mu_);
        sessions_.emplace(sid, s);
    }
    std::string wire;
    if (is_udp) {
        proto::encodeNewUdp(wire, proto::NewUdpFrame{sid});
    } else {
        proto::encodeNew(wire, proto::NewFrame{sid, target.toBinary(), opts_.init_window});
    }
    co_await enqueueFrame(std::move(wire));
    co_return s;
}

awaitable<void> Link::gracefulShutdown(std::chrono::seconds drain_timeout) {
    std::vector<SessionPtr> to_close;
    {
        absl::MutexLock lock(&sessions_mu_);
        to_close.reserve(sessions_.size());
        for (auto& kv : sessions_) to_close.push_back(kv.second);
    }
    for (auto& s : to_close) {
        std::string wire;
        proto::encodeClose(wire, proto::CloseFrame{s->sid(), "shutdown"});
        co_await enqueueFrame(std::move(wire));
    }
    asio::steady_timer timer(strand_);
    timer.expires_after(drain_timeout);
    boost::system::error_code ec;
    co_await timer.async_wait(asio::redirect_error(use_awaitable, ec));
    co_await failLink("graceful drain done");
}

awaitable<void> Link::failLink(absl::string_view reason) {
    if (closed_.exchange(true, std::memory_order_acq_rel)) co_return;
    SHINE_DEBUG("link closed peer={} reason={}", opts_.peer_label, reason);
    stream_->close();
    write_chan_->close();
    if (handshake_signal_) handshake_signal_->close();
    std::vector<SessionPtr> all;
    {
        absl::MutexLock lock(&sessions_mu_);
        for (auto& kv : sessions_) all.push_back(kv.second);
        sessions_.clear();
    }
    for (auto& s : all) s->onReset();
    co_return;
}

void Link::markClosed() {
    closed_.store(true, std::memory_order_release);
    if (stream_) stream_->close();
    if (write_chan_) write_chan_->close();
}

void Link::closeNow() {
    if (closed_.exchange(true, std::memory_order_acq_rel)) return;
    // Post to strand to avoid concurrent socket access from reader/writer.
    asio::post(strand_, [self = shared_from_this()]() {
        self->stream_->close();
        if (self->write_chan_) self->write_chan_->close();
        if (self->handshake_signal_) self->handshake_signal_->close();
        std::vector<SessionPtr> all;
        {
            absl::MutexLock lock(&self->sessions_mu_);
            for (auto& kv : self->sessions_) all.push_back(kv.second);
            self->sessions_.clear();
        }
        for (auto& session : all) session->onReset();
    });
}

awaitable<bool> Link::waitHandshake() {
    if (handshake_done_.load(std::memory_order_acquire)) co_return true;
    if (closed_.load(std::memory_order_acquire)) co_return false;
    boost::system::error_code ec;
    co_await handshake_signal_->async_receive(asio::redirect_error(use_awaitable, ec));
    co_return handshake_done_.load(std::memory_order_acquire);
}

SessionPtr Link::findSession(u64 sid) {
    absl::MutexLock lock(&sessions_mu_);
    auto it = sessions_.find(sid);
    return it == sessions_.end() ? nullptr : it->second;
}

void Link::eraseSession(u64 sid) {
    absl::MutexLock lock(&sessions_mu_);
    sessions_.erase(sid);
}

} // namespace shine
